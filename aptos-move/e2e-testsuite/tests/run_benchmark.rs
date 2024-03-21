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
use std::{fmt, format, fs, str::FromStr, time::Instant, hash::BuildHasherDefault};
use std::{thread, time};
use std::borrow::{Borrow, BorrowMut};
use std::char::MAX;
use std::cmp::max;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
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
use rustc_hash::{FxHashMap, FxHashSet};
use aptos_mempool::shared_mempool::types::{SYNC_CACHE};
use aptos_types::state_store::state_key::StateKey;
use aptos_types::write_set::WriteSet;
use aptos_vm::data_cache::StorageAdapter;
use statrs::statistics::OrderStatistics;
use statrs::statistics::Data;
use aptos_aggregator::transaction::TransactionOutputExt;

const INITIAL_BALANCE: u64 = 9_000_000_000;
const SEQ_NUM: u64 = 10;

const MAX_COIN_NUM: usize = 1000;
const CORES: u64 = 10;

fn main() {
    // 750000 for NFT & DEX
    // 4500000 for solana
    // 1500000 for p2p

    // Evaluate 5 things:
    // a) Good blocks Pythia vs Bad blocks Pythia (hint based/pessimistic) = 2
    // b) Good blocks BlockSTM vs Good blocks BlockSTM (optimistic) = 2
    // c) Varying workload and how we adjust to it.

    let num_accounts = 100000;
    let block_size = 10000;
    //let core_set = [4,8,12,16,20,24,28,32];
    let core_set = [20];

    let trial_count = 5;
    let modes = [Pythia_Sig];
    let additional_modes = ["Good", ""];

    //50 for 300k skipped

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
                let mut time = runExperimentWithSetting(mode, c, trial_count, num_accounts, block_size, P2PTX, 2300000, mode_two, false);
            }
            println!("#################################################################################");
        }
    }

    for mode in modes {
        for mode_two in additional_modes {
            for c in core_set {
                let mut time = runExperimentWithSetting(mode, c, trial_count, num_accounts, block_size, DEXAVG, 1300000, mode_two, false);
            }
            println!("#################################################################################");
        }
    }

    for mode in modes {
        for mode_two in additional_modes {
            for c in core_set {
                let mut time = runExperimentWithSetting(mode, c, trial_count, num_accounts, block_size, NFT, 1700000, mode_two, false);
            }
            println!("#################################################################################");
        }
    }*/

    for mode in modes {
        for mode_two in additional_modes {
            for c in core_set {
                let mut time = runExperimentWithSetting(mode, c, trial_count, num_accounts, block_size, DEXBURSTY, 1500000, mode_two, false);
            }
            println!("#################################################################################");
        }
    }

    for mode in modes {
        for mode_two in additional_modes {
            for c in core_set {
                let mut time = runExperimentWithSetting(mode, c, trial_count, num_accounts, block_size, MIXED, 14500000, mode_two, false);
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

fn runExperimentWithSetting(mode: ExecutionMode, c: usize, trial_count: usize, num_accounts: usize, block_size: u64, load_type: LoadType, max_gas: usize, mode_two: &str, abort_if_lower: bool) -> u128 {
    let module_path = "test_module_new.move";

    // This is for the total time
    let mut all_stats:BTreeMap<String, Vec<u128>> = BTreeMap::new();
    let mut block_result;

    let mut print_mode = mode.to_string();
    if !mode_two.is_empty()
    {
        print_mode = mode.to_string() + "_" + mode_two;
    }

    for trial in 0..trial_count {
        let mut executor = FakeExecutor::from_head_genesis();
        //executor.set_golden_file(name);

        let accounts = executor.create_accounts(289023, INITIAL_BALANCE, SEQ_NUM);

        let (module_owner, module_id) = create_module(&mut executor, module_path.to_string());
        let mut seq_num = HashMap::new();

        for idx in 0..289023 {
            seq_num.insert(idx, SEQ_NUM);
        }
        seq_num.insert(usize::MAX, SEQ_NUM + 1);

        run_warmup(mode, c, 10000, load_type, max_gas, &mut seq_num, module_owner.clone(), &module_id, accounts.clone(), &mut executor);

        let mut multiplier = 1;
        if !mode_two.is_empty()
        {
            multiplier = c as u64;
        }

        //todo: Now we want to measure the latency per tx for the initial block.
        //todo: The other transactions, are they of the same workload? Do we want to have just generic "cheap" transactions added in to allow filling up?
        let mut local_stats:BTreeMap<String, Vec<u128>> = BTreeMap::new();

        // I have a list of transactions. I will go through them and pick a bunch of them (mostly the beginning of the queue, but also a bunch in the middle). I want those removed.

        // Generate the workload.
        let mut main_block = create_block( block_size, multiplier, module_owner.clone(), accounts.clone(), &mut seq_num, &module_id, load_type.clone(), &executor);

        let mut latvec = vec![];
        let mut total_tx = 0;

        let mut run = true;
        let mut iter = 0;

        while run {
            iter+=1;

            let (return_block, filler_time, first_iter_tx) = get_transaction_register(&mut main_block, &executor, c, max_gas, !mode_two.is_empty(), true);

            let dif = first_iter_tx - total_tx;
            total_tx = first_iter_tx;
            if total_tx >= 10000 {
                run = false;
            }

            // Map to user transactions.
            let block = return_block.map_par_txns(Transaction::UserTransaction);
            if block.len() < 10000 && abort_if_lower {
                println!("Only {} in block: {}", block.len(), filler_time);
                return u128::MAX;
            }

            println!("block size: {}, accounts: {}, cores: {}, mode: {}, load: {:?}", block_size*multiplier, num_accounts, c, print_mode, load_type);

            let start = Instant::now();

            let mut profiler = Profiler::new();

            // The actual execution.
            block_result = executor
                .execute_transaction_block_parallel(
                    block.clone(),
                    c as usize,
                    mode, profiler.borrow_mut(),
                )
                .unwrap();

            let final_time = start.elapsed();
            latvec.push((dif, final_time.as_millis()+filler_time));


            let mut collected_times = profiler.collective_times.borrow_mut();
            collected_times.insert("final_time".to_string(), final_time);

            if mode_two.is_empty() {
                collected_times.insert("filler_time".to_string(), Duration::from_millis(0 as u64));
            } else {
                collected_times.insert("filler_time".to_string(), Duration::from_millis(filler_time as u64));
            }

            for (key, value) in collected_times {
                if local_stats.contains_key(key) {
                    local_stats.get_mut(key).unwrap().push(value.as_millis());
                } else {
                    local_stats.insert(key.to_string(), vec![value.as_millis()]);
                }
            }

            let counters = profiler.counters.borrow();
            for (key, value) in counters {
                if local_stats.contains_key(key) {
                    local_stats.get_mut(key).unwrap().push(*value);
                } else {
                    local_stats.insert(key.to_string(), vec![*value]);
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

        let mut data = vec![];
        let mut lat = 0;
        let mut total = 0;
        for (key, value) in latvec
        {
            lat = lat + value;
            total+= key as u128 * lat;
            for i in 0..key {
                data.push(lat as f64);
            }
        }

        let mut dataFrame = Data::new(data);
        local_stats.insert("p50".to_string(), vec![dataFrame.percentile(50) as u128]);
        local_stats.insert("p90".to_string(), vec![dataFrame.percentile(90) as u128]);
        local_stats.insert("p95".to_string(), vec![dataFrame.percentile(95) as u128]);

        let avg = total/10000;
        local_stats.insert("mean_latency".to_string(), vec![avg.into()]);

        for (key, value) in local_stats
        {
            let sum = value.iter().sum::<u128>();
            if all_stats.contains_key(&key) {
                all_stats.get_mut(&key).unwrap().push(sum);
            }
            else {
                all_stats.insert(key.to_string(), vec![sum]);
            }
        }
    }

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

fn run_warmup(mode: ExecutionMode, c: usize, block_size: u64, load_type: LoadType, max_gas: usize, seq_num: &mut HashMap<usize, u64>, module_owner: AccountData, module_id: &ModuleId, accounts: Vec<Account>, executor: &mut FakeExecutor) {
    println!("start warump");

    // Generate the workload.
    let mut main_block = create_block(block_size, 1 as u64, module_owner.clone(), accounts.clone(), seq_num, &module_id, load_type.clone(), &executor);
    println!("{} {}", main_block.len(), main_block.get(0).unwrap().len());
    let (return_block, filler_time, first_iter_tx) = get_transaction_register(&mut main_block, &executor, c, max_gas, false, false);
    // Map to user transactions.
    let block = return_block.map_par_txns(Transaction::UserTransaction);

    let mut profiler = Profiler::new();
    // The actual execution.
    let block_result = executor
        .execute_transaction_block_parallel(
            block.clone(),
            c as usize,
            mode, profiler.borrow_mut(),
        )
        .unwrap();

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

    println!("end warump");
}

fn get_transaction_register(txns: &mut Vec<Vec<(WriteSet, BTreeSet<StateKey>, u32, SignedTransaction)>>, executor: &FakeExecutor, cores: usize, max_gas: usize, good_block: bool, prod: bool) -> (TransactionRegister<SignedTransaction>, u128, u16) {
    let goal_qty = 10_000;
    let mut filler: DependencyFiller = DependencyFiller::new(
        (max_gas / cores) as u64,
        1_000_000_000,
        goal_qty,
        cores as u32
    );
    let min_modifier = usize::min(txns.len(),cores);


    let mut first_iter_tx:u16 = 0;
    let mut total_filler_time = 0;
    //let mut last_touched: FxHashMap<Vec<u8>, (u32, u16)> = FxHashMap::with_capacity_and_hasher(txns.len(), Default::default());
    //let mut skipped_users: FxHashSet<Vec<u8>> = FxHashSet::with_capacity_and_hasher(txns.len(), Default::default());

    let mut last_touched: FxHashMap<Vec<u8>, (u32, u16)> = FxHashMap::default();
    let mut skipped_users: FxHashSet<Vec<u8>> = FxHashSet::default();
    let mut index = 0;
    let num_blocks = txns.len();
    let mut prev_filler_state = 0;

    while true
    {
        let start = Instant::now();
        let mut vec_at_index = txns.get_mut(index).unwrap();
        let startlen = vec_at_index.len();

        let result = filler.add_all( vec_at_index, &mut last_touched, &mut skipped_users, good_block);
        let result_len = result.len();

        let len = filler.get_blockx().len();
        let dif = len - prev_filler_state;

        let per_batch_target = startlen / min_modifier;

        if dif < per_batch_target
        {
            filler.set_gas_per_core(((max_gas/cores) * dif)as u64);
            println!("Relax filler! {} {} {}", dif, per_batch_target, len)
        }

        // okay, I do know the total number of transactions. it's vec_len * vec_at_index_len.  Say it's 12 batches for 12 cores
        // Then, for block1, out of 10.000 transactions, we only added 1000. I know I got another 12 batches. 10.000/12 = 833, 1000 > 833 => okay. If 1000 < 833. What I want / What I got = 1.2 = multiplier for gas cost.


        prev_filler_state = len;
        std::mem::replace(vec_at_index, result);

        if index == 0 {
            first_iter_tx = (10000 - result_len) as u16;
        }

        let elapsed = start.elapsed().as_millis();
        total_filler_time += elapsed;
        if prod {
            println!("elapsed: {}", total_filler_time);

            if len >= 9990 || index + 1 >= num_blocks || (startlen == 10000 && dif == 0) {
                println!("done {} {} {} {}", len, index, num_blocks, first_iter_tx);
                break;
            }
        }
        else {
            break;
        }

        index+=1;
    }

    let gas_estimates = filler.get_gas_estimates();
    let dependencies = filler.get_dependency_graph();
    let txns = filler.get_block();

    // Resetting for next block!
    SYNC_CACHE.lock().unwrap().clear();

    (TransactionRegister::new(txns, gas_estimates, dependencies), total_filler_time, first_iter_tx)
}

//Create block with coin exchange transactions
fn create_block(
    size: u64,
    c: u64,
    owner: AccountData,
    accounts: Vec<Account>,
    seq_num: &mut HashMap<usize, u64>,
    module_id: &ModuleId,
    load_type: LoadType,
    executor: &FakeExecutor
) -> Vec<Vec<(WriteSet, BTreeSet<StateKey>, u32, SignedTransaction)>> {

    let mut transaction_validation = executor.get_transaction_validation();
    let mut end_result = Vec::new();

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

    let mut client_index: usize = 0;
    for j in 0..c
    {
        let mut result = Vec::new();
        for i in 0..size {
            client_index+=1;
            let mut sender_id: usize = client_index % accounts.len();
            let tx_entry_function;

            if matches!(load_type, MIXED)
            {
                let cost_sample = COST_DISTR[rand::thread_rng().gen_range(0..COST_DISTR.len())];
                let write_len_sample = LEN_DISTR[rand::thread_rng().gen_range(0..LEN_DISTR.len())] as usize;

                let mut writes: Vec<u64> = Vec::new();
                let mut size_param = 0;
                while size_param < write_len_sample {
                    size_param += 1;
                    writes.push(general_resource_distribution.sample(&mut rng) as u64);
                }

                let length = max(1, cost_sample.round() as usize);

                tx_entry_function = EntryFunction::new(
                    module_id.clone(),
                    ident_str!("loop_exchange").to_owned(),
                    vec![],
                    vec![bcs::to_bytes(owner.address()).unwrap(), bcs::to_bytes(&length).unwrap(), bcs::to_bytes(&writes).unwrap()],
                );
            } else if matches!(load_type, P2PTX)
            {
                let receiver_id = p2p_receiver_distribution.sample(&mut rng) % accounts.len();
                sender_id = p2p_sender_distribution.sample(&mut rng) % accounts.len();

                tx_entry_function = EntryFunction::new(
                    module_id.clone(),
                    ident_str!("exchangetwo").to_owned(),
                    vec![],
                    vec![bcs::to_bytes(owner.address()).unwrap(), bcs::to_bytes(&receiver_id).unwrap(), bcs::to_bytes(&sender_id).unwrap()],
                );
            } else {
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


            let ex_result = transaction_validation.speculate_transaction(&txn);
            let (a, b) = ex_result.unwrap();
            match b {
                VMStatus::Executed => {
                    result.push((a.output.txn_output().write_set().clone(), a.input, a.output.txn_output().gas_used() as u32, txn.clone()));
                }
                _ => {
                    // Do nothing.
                }
            }
        }
        end_result.push(result);
    }

    end_result
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
