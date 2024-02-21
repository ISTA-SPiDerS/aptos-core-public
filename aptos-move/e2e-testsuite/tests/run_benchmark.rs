// Copyright © Aptos Foundation

// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

extern crate core;

use aptos_types::{
    account_address::AccountAddress,
    transaction::{
        ExecutionStatus, Module, SignedTransaction, Transaction, TransactionStatus, TransactionRegister, ExecutionMode
    },
};
use aptos_mempool::core_mempool::{BlockFiller, DependencyFiller, SimpleFiller, TxnPointer};
use aptos_vm_validator::vm_validator::TransactionValidation;

use rand::prelude::*;
use regex::Regex;
use std::{collections::hash_map::HashMap, fmt, format, fs, str::FromStr, time::Instant};
use std::{thread, time};
use std::borrow::{Borrow, BorrowMut};
use std::char::MAX;
use std::cmp::max;
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::iter::Enumerate;
use std::ops::Deref;
use std::ptr::null;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::atomic::Ordering::Relaxed;
use std::sync::mpsc;
use std::sync::mpsc::SyncSender;
use std::thread::sleep;
use std::time::Duration;
use itertools::Itertools;
use move_core_types::{ident_str, identifier};
use move_core_types::language_storage::{ModuleId, StructTag, TypeTag};
use proptest::char::range;
use rand::distributions::WeightedIndex;
use rand::seq::index::IndexVec::USize;
use regex::internal::Exec;
use aptos_cached_packages::aptos_stdlib::coin_transfer;
use aptos_language_e2e_tests::account::{Account, AccountData};
use aptos_language_e2e_tests::account_activity_distribution::{TX_FROM, TX_NFT_FROM, TX_NFT_TO, TX_TO};
use aptos_language_e2e_tests::solana_distribution::{RES_DISTR, COST_DISTR, LEN_DISTR};

use aptos_language_e2e_tests::uniswap_distribution::{AVG, BURSTY};

use aptos_language_e2e_tests::compile::compile_source_module;
use aptos_language_e2e_tests::current_function_name;
use aptos_language_e2e_tests::executor::{FakeExecutor, FakeValidation};
use aptos_transaction_generator_lib::LoadType;
use aptos_transaction_generator_lib::LoadType::{DEXAVG, DEXBURSTY, NFT, P2PTX, MIXED};
use aptos_types::transaction::ExecutionMode::{Pythia, Pythia_Sig, Standard};
use aptos_types::transaction::{EntryFunction, Profiler, RAYON_EXEC_POOL, TransactionOutput};
use dashmap::{DashMap, DashSet};
use move_core_types::vm_status::VMStatus;
use rayon::iter::ParallelIterator;
use rayon::iter::IntoParallelIterator;
use rayon::prelude::*;
use aptos_mempool::shared_mempool::types::{SYNC_CACHE};
use aptos_types::state_store::state_key::StateKey;
use aptos_types::write_set::WriteSet;
use aptos_vm::data_cache::StorageAdapter;

const INITIAL_BALANCE: u64 = 9_000_000_000;
const SEQ_NUM: u64 = 10;

const MAX_COIN_NUM: usize = 1000;
const CORES: u64 = 10;

fn main() {
    let module_path = "test_module_new.move";
    let num_accounts = 100000;
    let block_size = 10000;

    let mut executor = FakeExecutor::from_head_genesis();
    executor.set_golden_file(current_function_name!());

    let accounts = executor.create_accounts(289023, INITIAL_BALANCE, SEQ_NUM);

    let (module_owner, module_id) = create_module(&mut executor, module_path.to_string());
    let mut seq_num = HashMap::new();

    for idx in 0..289023 {
        seq_num.insert(idx, SEQ_NUM);
    }
    seq_num.insert(usize::MAX, SEQ_NUM + 1); //module owner SEQ_NUM stored in key value usize::MAX

    println!("STARTING WARMUP");
    /*for _ in [1, 2, 3] {
        let txn = create_block(block_size, module_owner.clone(), accounts.clone(), &mut seq_num, &module_id, LoadType::P2PTX);
        println!("block created");
        let block = get_transaction_register(txn.clone(), &executor, 4, 4500000 * 10).0
            .map_par_txns(Transaction::UserTransaction);

        let mut prex_block_result = executor.execute_transaction_block_parallel(
            block.clone(),
            4 as usize,
            Pythia, &mut Profiler::new(),
        )
            .unwrap();

        for result in prex_block_result {
            match result.status() {
                TransactionStatus::Keep(status) => {
                    executor.apply_write_set(result.write_set());
                    assert_eq!(
                        status,
                        &ExecutionStatus::Success,
                        "transaction failed with {:?}",
                        status
                    );
                }
                TransactionStatus::Discard(status) => panic!("transaction discarded with {:?}", status),
                TransactionStatus::Retry => panic!("transaction status is retry"),
            };
        }
    }*/
    println!("END WARMUP");


    println!("EXECUTE BLOCKS");

    // 750000 for NFT & DEX
    // 4500000 for solana
    // 1500000 for p2p

    // Evaluate 5 things:
    // a) Good blocks Pythia vs Bad blocks Pythia (hint based/pessimistic) = 2
    // b) Good blocks BlockSTM vs Good blocks BlockSTM (optimistic) = 2
    // c) Varying workload and how we adjust to it.

    let core_set = [4, 8, 16, 32, 64];
    //let core_set = [4,6,8];

    let trial_count = 3;
    let modes = [Pythia_Sig];
    let additional_modes = ["Good"];

    // Give each transaction and index of which batch they are. Record the batch numbers to get calculate latency. Stop once 10k of batch 1 finished.
    //
    // Compare more and less strict models also in terms of execution time.
    //
    // Optimize execution time more of the filler calc. We can now more easily evaluate it. As its single threaded even on my pc alone.
    //
    // Can we do something like "identify popular resources, if tx accesses multiple popular ones, it's okay if it appears in the first 100, or in the last 100, else move up to next block.

    /*for mode in modes {
        for mode_two in additional_modes {
            for c in core_set {
                runExperimentWithSetting(mode, c, trial_count, num_accounts, block_size, &mut executor, &module_id, &accounts, &module_owner, &mut seq_num, MIXED, 10000000, mode_two);
            }
            println!("#################################################################################");
        }
    }*/

    for mode in modes {
        for mode_two in additional_modes {
            for c in core_set {
                let time = runExperimentWithSetting(mode, c, trial_count, num_accounts, block_size, &mut executor, &module_id, &accounts, &module_owner, &mut seq_num, P2PTX, 2255000, mode_two);
            }
            println!("#################################################################################");
        }
    }

    /*for mode in modes {
       for mode_two in additional_modes {
           for c in core_set {

               let mut max_gas = 40_000_000;
               let mut min_gas = 2_000_000;

               while true
               {
                   let time = runExperimentWithSetting(mode, c, trial_count, num_accounts, block_size, &mut executor, &module_id, &accounts, &module_owner, &mut seq_num, MIXED, min_gas, mode_two);

                   if time == u128::MAX {
                       min_gas = min_gas + (max_gas-min_gas) / 2;
                   }
                   else {
                       max_gas = min_gas;
                       min_gas = min_gas / 2;
                   }
                   println!("new min: {} max: {}", min_gas, max_gas);

                   if max_gas <= min_gas + 500 {
                       println!("------------------- ^ FOUND BEST for setting ^ -------------------");
                       break;
                   }
               }
           }
           println!("#################################################################################");
       }
   }

   for mode in modes {
       for mode_two in additional_modes {
           for c in core_set {
               let mut max_gas = 2_100_000;
               let mut min_gas = 100_000;

               while true
               {
                   let time = runExperimentWithSetting(mode, c, trial_count, num_accounts, block_size, &mut executor, &module_id, &accounts, &module_owner, &mut seq_num, P2PTX, min_gas, mode_two);

                   if time == u128::MAX {
                       min_gas = min_gas + (max_gas - min_gas) / 2;
                   } else {
                       max_gas = min_gas;
                       min_gas = min_gas / 2;
                   }
                   println!("new min: {} max: {}", min_gas, max_gas);

                   if max_gas <= min_gas + 500 {
                       println!("------------------- ^ FOUND BEST for setting ^ -------------------");
                       break;
                   }
               }
           }
           println!("#################################################################################");
       }
   }


   for mode in modes {
       for mode_two in additional_modes {
           for c in core_set {
               let mut max_gas = 1_100_000;
               let mut min_gas = 100_000;

               while true
               {
                   let time = runExperimentWithSetting(mode, c, trial_count, num_accounts, block_size, &mut executor, &module_id, &accounts, &module_owner, &mut seq_num, DEXBURSTY, min_gas, mode_two);

                   if time == u128::MAX {
                       min_gas = min_gas + (max_gas - min_gas) / 2;
                   } else {
                       max_gas = min_gas;
                       min_gas = min_gas / 2;
                   }
                   println!("new min: {} max: {}", min_gas, max_gas);

                   if max_gas <= min_gas + 500 {
                       println!("------------------- ^ FOUND BEST for setting ^ -------------------");
                       break;
                   }
               }
           }
           println!("#################################################################################");
       }
   }

   for mode in modes {
       for mode_two in additional_modes {
           for c in core_set {
               let mut max_gas = 1_100_000;
               let mut min_gas = 100_000;

               while true
               {
                   let time = runExperimentWithSetting(mode, c, trial_count, num_accounts, block_size, &mut executor, &module_id, &accounts, &module_owner, &mut seq_num, DEXAVG, min_gas, mode_two);

                   if time == u128::MAX {
                       min_gas = min_gas + (max_gas - min_gas) / 2;
                   } else {
                       max_gas = min_gas;
                       min_gas = min_gas / 2;
                   }
                   println!("new min: {} max: {}", min_gas, max_gas);

                   if max_gas <= min_gas + 500 {
                       println!("------------------- ^ FOUND BEST for setting ^ -------------------");
                       break;
                   }
               }
           }
           println!("#################################################################################");
       }
   }

   for mode in modes {
       for mode_two in additional_modes {
           for c in core_set {
               let mut max_gas = 1_100_000;
               let mut min_gas = 100_000;

               while true
               {
                   let time = runExperimentWithSetting(mode, c, trial_count, num_accounts, block_size, &mut executor, &module_id, &accounts, &module_owner, &mut seq_num, NFT, min_gas, mode_two);

                   if time == u128::MAX {
                       min_gas = min_gas + (max_gas - min_gas) / 2;
                   } else {
                       max_gas = min_gas;
                       min_gas = min_gas / 2;
                   }
                   println!("new min: {} max: {}", min_gas, max_gas);

                   if max_gas <= min_gas + 500 {
                       println!("------------------- ^ FOUND BEST for setting ^ -------------------");
                       break;
                   }
               }
           }
           println!("#################################################################################");
       }
   }*/

    println!("EXECUTION SUCCESS");
}

fn runExperimentWithSetting(mode: ExecutionMode, c: usize, trial_count: usize, num_accounts: usize, block_size: u64, executor: &mut FakeExecutor, module_id: &ModuleId, accounts: &Vec<Account>, module_owner: &AccountData, seq_num: &mut HashMap<usize, u64>, load_type: LoadType, max_gas: usize, mode_two: &str) -> u128 {
    // This is for the total time
    let mut times = vec![];
    let mut filler_times = vec![];

    let mut all_stats:BTreeMap<String, Vec<u128>> = BTreeMap::new();
    let mut block_result;

    let mut print_mode = mode.to_string();
    if !mode_two.is_empty()
    {
        print_mode = mode.to_string() + "_" + mode_two;
    }

    for trial in 0..trial_count {
        let mut profiler = Profiler::new();

        let mut ac_block_size = block_size;
        if !mode_two.is_empty()
        {
            ac_block_size = block_size * 20;
        }

        let mut ac_max_gas = max_gas;
        if mode_two.is_empty()
        {
            ac_max_gas = max_gas * 64;
        }

        // I have a list of transactions. I will go through them and pick a bunch of them (mostly the beginning of the queue, but also a bunch in the middle). I want those removed.

        // Generate the workload.
        let mut main_block = create_block( ac_block_size, module_owner.clone(), accounts.clone(), seq_num, &module_id, load_type.clone());


        while true {
            if let first = Some(main_block.first_key_value()) {
                if *first.unwrap().unwrap().0 >= 10000 {
                    println!("Reached End {}", first.unwrap().unwrap().0);
                    break;
                }

                //todo continue until the smallest number in map is 10000
                // Run it through the block creation & hint extraction
                let (return_block, filler_time) = get_transaction_register(&mut main_block, &executor, c, ac_max_gas);
                filler_times.push(filler_time);

                // Map to user transactions.
                let block = return_block.map_par_txns(Transaction::UserTransaction);


                if block.len() < 10000 {
                    println!("Only {} in block: {}", block.len(), filler_time);
                    return u128::MAX;
                }

                println!("block size: {}, accounts: {}, cores: {}, mode: {}, load: {:?}", ac_block_size, num_accounts, c, print_mode, load_type);

                let start = Instant::now();

                // The actual execution.
                block_result = executor
                    .execute_transaction_block_parallel(
                        block.clone(),
                        c as usize,
                        mode, profiler.borrow_mut()
                    )
                    .unwrap();

                times.push(start.elapsed().as_millis());

                let collected_times = profiler.collective_times.borrow();
                for (key,value) in collected_times {
                    if all_stats.contains_key(key) {
                        all_stats.get_mut(key).unwrap().push(value.as_millis());
                    }
                    else {
                        all_stats.insert(key.to_string(), vec![value.as_millis()]);
                    }
                }

                let counters = profiler.counters.borrow();
                for (key,value) in counters {
                    if all_stats.contains_key(key) {
                        all_stats.get_mut(key).unwrap().push(*value);
                    }
                    else {
                        all_stats.insert(key.to_string(), vec![*value]);
                    }
                }

                println!("Total time: {:?}", start.elapsed());

                for result in block_result {
                    match result.status() {
                        TransactionStatus::Keep(status) => {
                            executor.apply_write_set(result.write_set());
                            assert_eq!(
                                status,
                                &ExecutionStatus::Success,
                                "transaction failed with {:?}",
                                status
                            );
                        }
                        TransactionStatus::Discard(status) => panic!("transaction discarded with {:?}", status),
                        TransactionStatus::Retry => panic!("transaction status is retry"),
                    };
                }

            }
            else {
                println!("What happened???");
                break;
            }
        }


        //profiler.print()
    }

    all_stats.insert("final_time".to_string(), times);
    all_stats.insert("filler_time".to_string(), filler_times);


    println!("###,{},{},{:?},{}", print_mode, c, load_type, max_gas);
    let mut final_time = 0;
    for (key, value) in all_stats
    {
        let mean = (value.iter().sum::<u128>() as f64 / value.len() as f64) as f64;
        if key.eq("final_time") {
            final_time = mean as u128;
        }
        let variance = value.iter().map(|x| (*x as f64 - mean).powi(2)).sum::<f64>() / value.len() as f64;
        let standard_deviation = variance.sqrt();
        let min = value.iter().min().unwrap();
        let max = value.iter().max().unwrap();
        println!("#,{},avg:,{},deviation:,{},min:,{},max:,{}", key, mean, standard_deviation, min, max);
    }
    println!("#-------------------------------------------------------------------------");

    return final_time;
}

fn get_transaction_register(txns: &mut BTreeMap<u16, SignedTransaction>, executor: &FakeExecutor, cores: usize, max_gas: usize) -> (TransactionRegister<SignedTransaction>, u128) {
    let mut transaction_validation = executor.get_transaction_validation();

    let mut filler: DependencyFiller = DependencyFiller::new(
        (max_gas / cores) as u64,
        1_000_000_000,
        10_000,
        cores as u32
    );

    {
        let mut locked_cache = SYNC_CACHE.lock().unwrap();
        for (ind, tx) in txns.clone() {
            let result = transaction_validation.speculate_transaction(&tx);
            let (a, b) = result.unwrap();
            match b {
                VMStatus::Executed => {
                    locked_cache.insert((tx.sender(), tx.sequence_number()), (a.output.txn_output().write_set().clone(), a.input, a.output.txn_output().gas_used() as u32, tx.clone()));
                }
                _ => {
                    // Do nothing.
                }
            }
        }
    }

    let start = Instant::now();

    filler.add_all( txns);

    let elapsed = start.elapsed().as_millis();
    println!("elapsed: {}", elapsed);

    let gas_estimates = filler.get_gas_estimates();
    let dependencies = filler.get_dependency_graph();
    let txns = filler.get_block();

    // Resetting for next block!
    SYNC_CACHE.lock().unwrap().clear();

    (TransactionRegister::new(txns, gas_estimates, dependencies), elapsed)
}

//Create block with coin exchange transactions
fn create_block(
    size: u64,
    owner: AccountData,
    accounts: Vec<Account>,
    seq_num: &mut HashMap<usize, u64>,
    module_id: &ModuleId,
    load_type: LoadType,
) -> BTreeMap<u16, SignedTransaction> {

    let mut result = BTreeMap::new();
    let mut rng: ThreadRng = thread_rng();

    let mut resource_distribution_vec: &[f64] = &AVG;
    if matches!(load_type, LoadType::DEXBURSTY)
    {
        resource_distribution_vec = &BURSTY;
    }
    else if matches!(load_type, LoadType::NFT)
    {
        resource_distribution_vec = &TX_NFT_TO;
    }
    else if matches!(load_type, LoadType::MIXED)
    {
        resource_distribution_vec = &RES_DISTR;
    }

    let general_resource_distribution: WeightedIndex<f64> = WeightedIndex::new(resource_distribution_vec).unwrap();
    let p2p_receiver_distribution: WeightedIndex<f64> = WeightedIndex::new(&TX_FROM).unwrap();
    let p2p_sender_distribution: WeightedIndex<f64> = WeightedIndex::new(&TX_TO).unwrap();
    let nft_sender_distribution: WeightedIndex<f64> = WeightedIndex::new(&TX_NFT_FROM).unwrap();

    for i in 0..size {

        let mut sender_id: usize = (i as usize) % accounts.len();
        let tx_entry_function;

        if matches!(load_type, MIXED)
        {
            let cost_sample = COST_DISTR[rand::thread_rng().gen_range(0..COST_DISTR.len())];
            let write_len_sample = LEN_DISTR[rand::thread_rng().gen_range(0..LEN_DISTR.len())] as usize;

            let mut writes: Vec<u64> = Vec::new();
            let mut i = 0;
            while i < write_len_sample {
                i+=1;
                writes.push(general_resource_distribution.sample(&mut rng) as u64);
            }

            let length = max(1, cost_sample.round() as usize);

            tx_entry_function = EntryFunction::new(
                module_id.clone(),
                ident_str!("loop_exchange").to_owned(),
                vec![],
                vec![bcs::to_bytes(owner.address()).unwrap(), bcs::to_bytes(&length).unwrap(), bcs::to_bytes(&writes).unwrap()],
            );
        }
        else if matches!(load_type, P2PTX)
        {
            let receiver_id = p2p_receiver_distribution.sample(&mut rng) % accounts.len();
            let sender_id = p2p_sender_distribution.sample(&mut rng) % accounts.len();

            tx_entry_function = EntryFunction::new(
                module_id.clone(),
                ident_str!("exchangetwo").to_owned(),
                vec![],
                vec![bcs::to_bytes(owner.address()).unwrap(), bcs::to_bytes(&receiver_id).unwrap(), bcs::to_bytes(&sender_id).unwrap()],
            );
        }
        else
        {

            let resource_id = general_resource_distribution.sample(&mut rng);
            if matches!(load_type, NFT)
            {
                sender_id = nft_sender_distribution.sample(&mut rng) % accounts.len();
            }

            tx_entry_function = EntryFunction::new(
                module_id.clone(),
                ident_str!("exchange").to_owned(),
                vec![],
                vec![bcs::to_bytes(owner.address()).unwrap(), bcs::to_bytes(&resource_id).unwrap()],
            );
        }

        let txn = accounts[sender_id]
            .transaction()
            .entry_function(tx_entry_function.clone())
            .sequence_number(seq_num[&sender_id])
            .sign();
        seq_num.insert(sender_id, seq_num[&sender_id] + 1);
        result.insert(i as u16, txn);
    }

    result
}

fn create_module(executor: &mut FakeExecutor, module_path: String) -> (AccountData, ModuleId) {
    let owner_account = executor.create_raw_account_data(INITIAL_BALANCE, SEQ_NUM);
    executor.add_account_data(&owner_account);

    let module_macros = HashMap::from([(r"Owner".to_string(), owner_account.address())]);
    let (mid, module) = compile_source_module(module_path, &module_macros);
    let publish_txn = owner_account
        .account()
        .transaction()
        .module(module)
        .sequence_number(SEQ_NUM)
        .sign();
    let _result = executor.execute_and_apply(publish_txn);
    println!("PASSED!");

    (owner_account, mid)
}
