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
use aptos_mempool::core_mempool::{BlockFiller, DependencyFiller, SimpleFiller};
use aptos_vm_validator::vm_validator::TransactionValidation;

use rand::prelude::*;
use regex::Regex;
use std::{collections::hash_map::HashMap, fmt, format, fs, str::FromStr, time::Instant};
use std::{thread, time};
use std::borrow::{Borrow, BorrowMut};
use std::char::MAX;
use std::cmp::max;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::iter::Enumerate;
use std::ops::Deref;
use std::ptr::null;
use std::sync::atomic::{AtomicU32, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::SyncSender;
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
use aptos_transaction_generator_lib::LoadType::{DEXAVG, DEXBURSTY, NFT, P2PTX, SOLANA};
use aptos_types::transaction::ExecutionMode::{Pythia, Pythia_Sig, Standard};
use aptos_types::transaction::{EntryFunction, Profiler, RAYON_EXEC_POOL, TransactionOutput};
use dashmap::{DashMap, DashSet};
use move_core_types::vm_status::VMStatus;
use rayon::iter::ParallelIterator;
use rayon::iter::IntoParallelIterator;
use rayon::prelude::*;
use aptos_mempool::shared_mempool::types::{CACHE};
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
    for _ in [1, 2, 3] {
        let txn = create_block(block_size, module_owner.clone(), accounts.clone(), &mut seq_num, &module_id, LoadType::P2PTX);
        let block = get_transaction_register(txn.clone(), &executor, 4)
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
    }
    println!("END WARMUP");


    println!("EXECUTE BLOCKS");

    let core_set = [4,8,12,16,20,24,28,32];
    let trial_count = 10;
    let modes = [Pythia, Pythia_Sig];

    for mode in modes {
        for c in core_set {
            runExperimentWithSetting(mode, c, trial_count, num_accounts, block_size, &mut executor, &module_id, &accounts, &module_owner, &mut seq_num, DEXBURSTY);
        }
        println!("#################################################################################");
    }

    for mode in modes {
        for c in core_set {
            runExperimentWithSetting(mode, c, trial_count, num_accounts, block_size, &mut executor, &module_id, &accounts, &module_owner, &mut seq_num, DEXAVG);
        }
        println!("#################################################################################");
    }

    for mode in modes {
        for c in core_set {
            runExperimentWithSetting(mode, c, trial_count, num_accounts, block_size, &mut executor, &module_id, &accounts, &module_owner, &mut seq_num, NFT);
        }
        println!("#################################################################################");
    }

    for mode in modes {
        for c in core_set {
            runExperimentWithSetting(mode, c, trial_count, num_accounts, block_size, &mut executor, &module_id, &accounts, &module_owner, &mut seq_num, SOLANA);
        }
        println!("#################################################################################");
    }

    for mode in modes {
        for c in core_set {
            runExperimentWithSetting(mode, c, trial_count, num_accounts, block_size, &mut executor, &module_id, &accounts, &module_owner, &mut seq_num, P2PTX);
        }
        println!("#################################################################################");
    }

    println!("EXECUTION SUCCESS");
}

fn runExperimentWithSetting(mode: ExecutionMode, c: usize, trial_count: usize, num_accounts: usize, block_size: u64, executor: &mut FakeExecutor, module_id: &ModuleId, accounts: &Vec<Account>, module_owner: &AccountData, seq_num: &mut HashMap<usize, u64>, load_type: LoadType) {
    // This is for the total time
    let mut times = vec![];
    let mut all_stats:BTreeMap<String, Vec<u128>> = BTreeMap::new();
    let mut block_result;

    for trial in 0..trial_count {
        let mut profiler = Profiler::new();

        let block = create_block(block_size, module_owner.clone(), accounts.clone(), seq_num, &module_id, load_type.clone());
        let block = get_transaction_register(block.clone(), &executor, c)
            .map_par_txns(Transaction::UserTransaction);

        println!("block size: {}, accounts: {}, cores: {}, mode: {}, load: {:?}", block_size, num_accounts, c, mode, load_type);
        let start = Instant::now();
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
        //profiler.print()
    }

    all_stats.insert("final_time".to_string(), times);


    println!("###,{},{},{:?}", mode, c, load_type);
    for (key, value) in all_stats
    {
        let mean = (value.iter().sum::<u128>() as f64 / value.len() as f64) as f64;
        let variance = value.iter().map(|x| (*x as f64 - mean).powi(2)).sum::<f64>() / value.len() as f64;
        let standard_deviation = variance.sqrt();
        let min = value.iter().min().unwrap();
        let max = value.iter().max().unwrap();
        println!("#,{},avg:,{},deviation:,{},min:,{},max:,{}", key, mean, standard_deviation, min, max);
    }
    println!("#-------------------------------------------------------------------------");
}

fn get_transaction_register(txns: VecDeque<SignedTransaction>, executor: &FakeExecutor, cores: usize) -> TransactionRegister<SignedTransaction> {
    let mut transaction_validation = executor.get_transaction_validation();
    let (tx, rx):(mpsc::SyncSender<(u64, SignedTransaction)>, mpsc::Receiver<(u64, SignedTransaction)>) = mpsc::sync_channel(100000);

    let mut filler: DependencyFiller = DependencyFiller::new(
        1000000000,
        1_000_000_000,
        10_000_000,
        16, tx
    );

    let len = txns.len();

    let th = rayon::spawn(move || {

        let mut input = vec![];
        loop
        {
            let start = Instant::now();
            let mut count = 0;
            loop
            {
                if let Ok((index, tx)) = rx.try_recv() {
                    input.push((index, tx));
                    count += 1;
                }
                else if count >= len {
                    //println!("xxx thread end xxx");
                    break;
                }
            }

            if input.is_empty()
            {
                continue;
            }

            let exec_counter = AtomicUsize::new(0);

            let failures = DashMap::new();
            {
                RAYON_EXEC_POOL.lock().unwrap().scope(|s| {
                    for _ in 0..cores {
                        s.spawn(|_| {

                            let mut current_index = exec_counter.fetch_add(1, Ordering::SeqCst);
                            while current_index < count {
                                let (index, tx) = &input[current_index];
                                let result = transaction_validation.speculate_transaction(&tx);
                                let (a, b) = result.unwrap();
                                match b {
                                    VMStatus::Executed => {
                                        CACHE.insert(index.clone(), (a, b, tx.clone()));
                                    }
                                    _ => {
                                        failures.insert(index.clone(), tx.clone());
                                    }
                                }
                                current_index = exec_counter.fetch_add(1, Ordering::SeqCst);
                            }
                        });
                    }
                });
            }

            input.clear();

            if count >= len {
                println!("xxx thread end xxx {}", failures.len());
                println!("Total time: {:?}", start.elapsed().as_millis());

                return;
            }

            for value in failures {
                input.push(value);
            }
        }
    });


    filler.add_all(txns, &mut VecDeque::with_capacity(10000), &mut u64::MAX, &mut 0, &mut HashSet::new());

    let gas_estimates = filler.get_gas_estimates();
    let dependencies = filler.get_dependency_graph();
    let txns = filler.get_block();

    drop(th);

    println!("---Start---");

    TransactionRegister::new(txns, gas_estimates, dependencies)
}

//Create block with coin exchange transactions
fn create_block(
    size: u64,
    owner: AccountData,
    accounts: Vec<Account>,
    seq_num: &mut HashMap<usize, u64>,
    module_id: &ModuleId,
    load_type: LoadType,
) -> VecDeque<SignedTransaction> {

    let mut result = VecDeque::new();
    let mut rng: ThreadRng = thread_rng();
    let extended_size = size * 10;

    let mut resource_distribution_vec:Vec<f64> = vec![1.0,1.0,1.0,1.0];
    let mut count = 0.0;
    if matches!(load_type, LoadType::DEXAVG)
    {
        for value in AVG {
            resource_distribution_vec.push(value);
        }
    }
    else if matches!(load_type, LoadType::DEXBURSTY)
    {
        for value in BURSTY {
            resource_distribution_vec.push(value);
        }
    }
    else if matches!(load_type, LoadType::NFT)
    {
        for value in TX_NFT_TO {
            resource_distribution_vec.push(value);
        }
    }
    else if matches!(load_type, LoadType::SOLANA)
    {
        for value in RES_DISTR {
            for _ in 0..20 {
                resource_distribution_vec.push(value);
            }
        }
    }

    let mut solana_len_options:Vec<usize> = vec![];
    let mut solana_cost_options:Vec<f64> = vec![];

    for value in LEN_DISTR {
        solana_len_options.push(value.round() as usize);
    }

    for value in COST_DISTR {
        solana_cost_options.push(value);
    }

    let general_resource_distribution: WeightedIndex<f64> = WeightedIndex::new(&resource_distribution_vec).unwrap();

    let mut nft_sender_distr_vec: Vec<f64> = vec![];
    for value in TX_NFT_FROM {
        nft_sender_distr_vec.push(value);
    }

    let nft_sender_distribution: WeightedIndex<f64> = WeightedIndex::new(&nft_sender_distr_vec).unwrap();

    let mut p2p_sender_distr_vec:Vec<f64> = vec![];
    let mut p2p_receiver_distr_vec:Vec<f64> = vec![];

    for value in TX_TO {
        p2p_receiver_distr_vec.push(value);
    }

    for value in TX_FROM {
        p2p_sender_distr_vec.push(value);
    }

    let p2p_receiver_distribution: WeightedIndex<f64> = WeightedIndex::new(&p2p_receiver_distr_vec).unwrap();
    let p2p_sender_distribution: WeightedIndex<f64> = WeightedIndex::new(&p2p_sender_distr_vec).unwrap();

    for i in 0..size {
        let mut sender_id: usize = (i as usize) % accounts.len();
        let tx_entry_function;

        if matches!(load_type, SOLANA)
        {
            let cost_sample = solana_cost_options[rand::thread_rng().gen_range(0..solana_cost_options.len())];
            let write_len_sample = solana_len_options[rand::thread_rng().gen_range(0..solana_len_options.len())];

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
        result.push_back(txn);
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
