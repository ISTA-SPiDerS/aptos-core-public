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
use aptos_language_e2e_tests::account_activity_distribution::{COIN_DISTR, TX_FROM, TX_NFT_FROM, TX_NFT_TO, TX_TO};
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

const INITIAL_BALANCE: u64 = 9_000_000_000;
const SEQ_NUM: u64 = 10;

const MAX_COIN_NUM: usize = 1000;
const CORES: u64 = 10;

fn main() {
    let module_path = "test_module_new.move";
    let num_accounts = 100000;
    let block_size = 100;

    let mut executor = FakeExecutor::from_head_genesis();
    executor.set_golden_file(current_function_name!());

    let accounts = executor.create_accounts(289023, INITIAL_BALANCE, SEQ_NUM);

    let (module_owner, module_id) = create_module(&mut executor, module_path.to_string());
    let mut seq_num = HashMap::new();

    for idx in 0..289023 {
        seq_num.insert(idx, SEQ_NUM);
    }
    seq_num.insert(usize::MAX, SEQ_NUM + 1); //module owner SEQ_NUM stored in key value usize::MAX

    /*println!("STARTING WARMUP");
    for warmup in [1, 2, 3] {
        let txn = create_block(block_size, module_owner.clone(), accounts.clone(), &mut seq_num, &module_id, 2, LoadType::P2PTX);
        let block = get_transaction_register(txn.clone(), &executor)
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
    println!("END WARMUP");*/


    println!("EXECUTE BLOCKS");

    let core_set = [8];
    let coin_set = [2,4,8,16,32,64,128];
    let trial_count = 10;
    let modes = [Pythia, Pythia_Sig];
    //let distributions = [WeightedIndex::new(&COIN_DISTR).unwrap(), WeightedIndex::new([])];

    // for mode in modes {
    //    for coins in coin_set {
    //        for c in core_set {
    //            runExperimentWithSetting(mode, coins, c, trial_count, num_accounts, block_size, &mut executor, &module_id, &accounts, &module_owner, &mut seq_num, COINS);
    //        }
    //    }
    //    println!("#################################################################################");
    // }

    for mode in modes {
        for c in core_set {
            runExperimentWithSetting(mode, COIN_DISTR.len(), c, trial_count, num_accounts, block_size, &mut executor, &module_id, &accounts, &module_owner, &mut seq_num, DEXBURSTY);
        }
        println!("#################################################################################");
    }

    println!("EXECUTION SUCCESS");
}

fn runExperimentWithSetting(mode: ExecutionMode, coins: usize, c: usize, trial_count: usize, num_accounts: usize, block_size: u64, executor: &mut FakeExecutor, module_id: &ModuleId, accounts: &Vec<Account>, module_owner: &AccountData, seq_num: &mut HashMap<usize, u64>, load_type: LoadType) {
    // This is for the total time
    let mut times = vec![];
    let mut all_stats:BTreeMap<String, Vec<u128>> = BTreeMap::new();
    let mut block_result;

    for trial in 0..trial_count {
        let mut profiler = Profiler::new();

        let block = create_block(block_size, module_owner.clone(), accounts.clone(), seq_num, &module_id, coins, load_type.clone());
        let block = get_transaction_register(block.clone(), &executor)
            .map_par_txns(Transaction::UserTransaction);

        println!("block size: {}, accounts: {}, cores: {}, coins: {}, mode: {}, load: {:?}", block_size, num_accounts, c, coins, mode, load_type);
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


    println!("###,{},{},{},{:?}", mode, coins, c, load_type);
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

fn get_transaction_register(txns: VecDeque<SignedTransaction>, executor: &FakeExecutor) -> TransactionRegister<SignedTransaction> {
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
        let val = transaction_validation.clone();
        let mut count = 0;

        let mut input = vec![];
        loop
        {
            loop
            {
                if let Ok((index, tx)) = rx.try_recv() {
                    input.push((index, tx));
                    count += 1;
                }
                else {
                    break;
                }
            }

            let failures = DashMap::new();
            if !input.is_empty()
            {
                RAYON_EXEC_POOL.lock().unwrap().install(|| {
                    input.par_drain(..)
                        .for_each(|(index, tx)| {
                            let result = val.speculate_transaction(&tx);
                            let (a, b) = result.unwrap();

                            match b {
                                VMStatus::Executed => {
                                    CACHE.insert(index, (a, b, tx));
                                }
                                _ => {
                                    failures.insert(index, tx);
                                }
                            }
                        });
                });
            }

            if input.is_empty() && count >= len {
                println!("xxx thread end xxx");
                return;
            }

            for value in failures {
                input.push(value);
            }
        }
    });


    let mut _simple_filler = SimpleFiller::new(100_000_000, 100_000);
    filler.add_all(txns, &mut VecDeque::new(), &mut u64::MAX, &mut 0, &mut HashSet::new());

    let gas_estimates = filler.get_gas_estimates();
    let dependencies = filler.get_dependency_graph();
    let txns = filler.get_block();

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
    coins: usize,
    load_type: LoadType,
) -> VecDeque<SignedTransaction> {

    let mut result = VecDeque::new();
    let mut rng: ThreadRng = thread_rng();

    let mut distr:Vec<f64> = vec![];
    if matches!(load_type, LoadType::DEXAVG)
    {
        for (key, value) in AVG {
            for i in 0..value {
                distr.push(key)
            }
        }
        println!("{}", distr.len())
    }
    else if matches!(load_type, LoadType::DEXBURSTY)
    {
        for (key, value) in BURSTY {
            for i in 0..value {
                distr.push(key)
            }
        }
    }
    else if matches!(load_type, LoadType::NFT)
    {
        for (key, value) in TX_NFT_TO {
            for i in 0..value {
                distr.push(key)
            }
        }
    }
    else if matches!(load_type, LoadType::SOLANA)
    {
        for (key, value) in RES_DISTR {
            for i in 0..value*20 {
                distr.push(key)
            }
        }
    }
    else
    {
        distr = Vec::from(COIN_DISTR);
    }

    let mut len_options:Vec<usize> = vec![];
    let mut len_distr:Vec<usize> = vec![];

    for (key, value) in LEN_DISTR {
        len_options.push(key.round() as usize);
        len_distr.push(value as usize);
    }

    let mut cost_options:Vec<f64> = vec![];
    let mut cost_distr:Vec<usize> = vec![];

    for (key, value) in COST_DISTR {
        cost_options.push(key);
        cost_distr.push(value as usize);
    }

    let tx_num_writes_distr : WeightedIndex<usize> = WeightedIndex::new(&len_distr).unwrap();
    let tx_cost_distr : WeightedIndex<usize> = WeightedIndex::new(&cost_distr).unwrap();

    let dist : WeightedIndex<f64> = WeightedIndex::new(&distr).unwrap();

    let mut fromVec: Vec<usize> = vec![];
    for (key, value) in TX_NFT_FROM {
        for i in 0..(value as usize) {
            fromVec.push(key as usize)
        }
    }
    let from_dist: WeightedIndex<usize> = WeightedIndex::new(&fromVec).unwrap();

    let mut max_count:usize = 1;
    let max_value_opt = len_options.iter().max();
    match max_value_opt {
        Some(max) => { max_count = *max; },
        None      => println!("vec empty, wat!")
    }


    let mut fromVecP2P:Vec<f64> = vec![];
    let mut toVecP2P:Vec<f64> = vec![];

    for (key, value) in TX_TO {
        for i in 0..value {
            toVecP2P.push(key);
        }
    }

    for (key, value) in TX_FROM {
        for i in 0..value {
            fromVecP2P.push(key);
        }
    }

    let to_dist_p2p:WeightedIndex<f64> = WeightedIndex::new(&toVecP2P).unwrap();
    let from_dist_p2p:WeightedIndex<f64> = WeightedIndex::new(&fromVecP2P).unwrap();

    for i in 0..size {
        let mut idx: usize = (i as usize) % accounts.len();
        let entry_function;

        if matches!(load_type, SOLANA)
        {
            let cost = cost_options[tx_cost_distr.sample(&mut rng)];
            let num_writes = len_options[tx_num_writes_distr.sample(&mut rng)];

            let mut writes: Vec<u64> = Vec::new();
            let mut i = 0;
            while i < num_writes {
                i+=1;
                writes.push(dist.sample(&mut rng) as u64);
            }

            let length = max(1, cost.round() as usize);

            entry_function = EntryFunction::new(
                module_id.clone(),
                ident_str!("loop_exchange").to_owned(),
                vec![],
                vec![bcs::to_bytes(owner.address()).unwrap(), bcs::to_bytes(&length).unwrap(), bcs::to_bytes(&writes).unwrap()],
            );
        }
        else if matches!(load_type, P2PTX)
        {
            let idx_to = to_dist_p2p.sample(&mut rng) % accounts.len();
            let idx_from = from_dist_p2p.sample(&mut rng) % accounts.len();

            entry_function = EntryFunction::new(
                module_id.clone(),
                ident_str!("exchangetwo").to_owned(),
                vec![],
                vec![bcs::to_bytes(owner.address()).unwrap(), bcs::to_bytes(&idx_to).unwrap(), bcs::to_bytes(&idx_from).unwrap()],
            );
        }
        else
        {
            let coin_1_num;
            if matches!(load_type, DEXAVG) || matches!(load_type, DEXBURSTY)
            {
                coin_1_num = dist.sample(&mut rng) % coins;
            }
            else if matches!(load_type, NFT)
            {
                idx = from_dist.sample(&mut rng) % accounts.len();
                coin_1_num = dist.sample(&mut rng) % coins;
            }
            else {
                coin_1_num = rng.gen::<usize>() % coins;
            }

            entry_function = EntryFunction::new(
                module_id.clone(),
                ident_str!("exchange").to_owned(),
                vec![],
                vec![bcs::to_bytes(owner.address()).unwrap(), bcs::to_bytes(&coin_1_num).unwrap()],
            );
        }


        let txn = accounts[idx]
            .transaction()
            .entry_function(entry_function.clone())
            .sequence_number(seq_num[&idx])
            .sign();
        seq_num.insert(idx, seq_num[&idx] + 1);
        result.push_back(txn);
    }
    // println!("{:?}", result);

    result
}

//todo! resource allocation is odd!

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
