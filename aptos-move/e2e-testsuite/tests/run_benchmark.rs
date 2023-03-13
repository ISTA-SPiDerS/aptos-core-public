// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use aptos_types::{
    account_address::AccountAddress,
    transaction::{
        EntryFunction, ExecutionStatus, Module, SignedTransaction, Transaction, TransactionStatus, TransactionRegister, ExecutionMode
    },
};
use aptos_mempool::core_mempool::{BlockFiller, DependencyFiller, SimpleFiller};


use rand::prelude::*;
use regex::Regex;
use std::{collections::hash_map::HashMap, fmt, format, fs, str::FromStr, time::Instant};
use std::{thread, time};
use std::borrow::{Borrow, BorrowMut};
use std::char::MAX;
use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::iter::Enumerate;
use std::ops::Deref;
use itertools::Itertools;
use move_core_types::{ident_str, identifier};
use move_core_types::language_storage::{ModuleId, StructTag, TypeTag};
use proptest::char::range;
use rand::distributions::WeightedIndex;
use rand::seq::index::IndexVec::USize;
use regex::internal::Exec;
use aptos_cached_packages::aptos_stdlib::coin_transfer;
use aptos_language_e2e_tests::account::{Account, AccountData};
use aptos_language_e2e_tests::account_activity_distribution::{COIN_DISTR, RES_DISTR, TX_FROM, TX_LENGTH_WEIGHT, TX_LENGTHS, TX_TO, TX_WRITES, TX_WRITES_WEIGHT};
use aptos_language_e2e_tests::compile::compile_source_module;
use aptos_language_e2e_tests::current_function_name;
use aptos_language_e2e_tests::executor::{FakeExecutor, FakeValidation};
use aptos_types::transaction::ExecutionMode::{Hints, Standard};
use aptos_types::transaction::{Profiler, TransactionOutput};
use crate::LoadType::{COINS, DEX, P2PTX, SOLANA};

const INITIAL_BALANCE: u64 = 9_000_000_000;
const SEQ_NUM: u64 = 10;

const MAX_COIN_NUM: usize = 1000;
const CORES: u64 = 10;

#[derive(Clone, Copy, Debug)]
enum LoadType
{
    COINS,
    DEX,
    P2PTX,
    SOLANA
}

impl Display for LoadType {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}


fn main() {
    let module_path = "test_module_new.move";
    let num_accounts = 10000;
    let block_size = 10000;

    let mut executor = FakeExecutor::from_head_genesis();
    executor.set_golden_file(current_function_name!());

    let accounts = executor.create_accounts(289023, INITIAL_BALANCE, SEQ_NUM);

    let (module_owner, module_id) = create_module(&mut executor, module_path.to_string());
    let mut seq_num = HashMap::new();

    for idx in 0..289023 {
        seq_num.insert(idx, SEQ_NUM);
    }
    seq_num.insert(usize::MAX, SEQ_NUM + 2); //module owner SEQ_NUM stored in key value usize::MAX
    let register_block = register_coins(&mut executor, &module_id, &module_owner, &mut seq_num);
    println!("CALLING REGISTER");

    let block = get_transaction_register(register_block, &executor).map_par_txns(Transaction::UserTransaction);
    let register_block_result = executor.execute_transaction_block_parallel(
        block, CORES as usize, ExecutionMode::Hints, &mut Profiler::new()
    ).unwrap();
    for result in register_block_result.clone() {
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

    println!("REGISTER SUCCESS");

    println!("STARTING WARMUP");
    for warmup in [1, 2, 3] {
        let block = create_block(block_size, module_owner.clone(), accounts.clone(), &mut seq_num, &module_id, 2, COINS);
        let block = get_transaction_register(block.clone(), &executor)
            .map_par_txns(Transaction::UserTransaction);

        let mut prex_block_result = register_block_result.clone();

        prex_block_result = executor.execute_transaction_block_parallel(
            block.clone(),
            CORES as usize,
            Standard, &mut Profiler::new(),
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
    let coin_set = [2,4,8,16,32,64,128];
    let trial_count = 10;
    let modes = [Hints];
    //let distributions = [WeightedIndex::new(&COIN_DISTR).unwrap(), WeightedIndex::new([])];

    //for mode in modes {
    //    for coins in coin_set {
    //        for c in core_set {
    //            runExperimentWithSetting(mode, coins, c, trial_count, num_accounts, block_size, &mut executor, &module_id, &accounts, &module_owner, &mut seq_num, COINS);
    //        }
    //    }
    //    println!("#################################################################################");
    //g}

    for mode in modes {
        for c in core_set {
            runExperimentWithSetting(mode, COIN_DISTR.len(), c, trial_count, num_accounts, block_size, &mut executor, &module_id, &accounts, &module_owner, &mut seq_num, SOLANA);
        }
        println!("#################################################################################");
    }

    for mode in modes {
        for c in core_set {
            runExperimentWithSetting(mode, COIN_DISTR.len(), c, trial_count, num_accounts, block_size, &mut executor, &module_id, &accounts, &module_owner, &mut seq_num, P2PTX);
        }
        println!("#################################################################################");
    }

    for mode in modes {
        for c in core_set {
            runExperimentWithSetting(mode, COIN_DISTR.len(), c, trial_count, num_accounts, block_size, &mut executor, &module_id, &accounts, &module_owner, &mut seq_num, DEX);
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

        println!("block size: {}, accounts: {}, cores: {}, coins: {}, mode: {}, load: {}", block_size, num_accounts, c, coins, mode, load_type);
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


    println!("###,{},{},{},{}", mode, coins, c, load_type);
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

fn get_transaction_register(txns: Vec<SignedTransaction>, executor: &FakeExecutor) -> TransactionRegister<SignedTransaction> {
    let mut transaction_validation = executor.get_transaction_validation();
    let mut filler: DependencyFiller<FakeValidation, CORES> = DependencyFiller::new(
        &mut transaction_validation,
        100_000_000,
        100_000_000,
        100_000
    );
    let mut _simple_filler = SimpleFiller::new(100_000_000, 100_000);
    for tx in txns {
        filler.add(tx);
    }
    let gas_estimates = filler.get_gas_estimates();
    let dependencies = filler.get_dependency_graph();
    let txns = filler.get_block();

    TransactionRegister::new(txns, gas_estimates, dependencies)
}

fn register_coins(
    executor: &mut FakeExecutor,
    module_id: &ModuleId,
    module_owner: &AccountData,
    seq_num: &mut HashMap<usize, u64>,
) -> Vec<SignedTransaction> {
    let mut result = vec![];
    for coin_number in 0_usize..MAX_COIN_NUM {
        let mut coin: String = "CoinC".to_string();
        let coin_number_string = coin_number.to_string();
        coin.push_str(&coin_number_string);
        // println!("{}", coin);

        let coin_clone = coin.clone();
        //let coin_clone: &str = &coin.clone();
        let coinId: identifier::Identifier = identifier::Identifier::new(coin).unwrap();
        // coinId.from_str(&coin);
        let coin_c = TypeTag::Struct(Box::new(StructTag {
            address: module_owner.address().clone(),
            module: ident_str!("Exchange").to_owned(),
            name: coinId,
            type_params: vec![],
        }));

        let register_coin_function = EntryFunction::new(
            module_id.clone(),
            ident_str!("register_coin").to_owned(),
            vec![coin_c.clone()],
            vec![bcs::to_bytes(&coin_clone).unwrap()],
        );
        let register_txn = module_owner
            .account()
            .transaction()
            .entry_function(register_coin_function)
            .sequence_number(seq_num[&usize::MAX])
            .sign();

        result.push(register_txn);
        // let _result = executor.execute_and_apply(register_txn);
        seq_num.insert(usize::MAX, seq_num[&usize::MAX] + 1);
    }
    result
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
) -> Vec<SignedTransaction> {

    let mut result = vec![];
    let mut rng = thread_rng();

    if matches!(load_type, P2PTX)
    {
        let to_dist:WeightedIndex<usize> = WeightedIndex::new(&TX_TO).unwrap();
        let from_dist:WeightedIndex<usize> = WeightedIndex::new(&TX_FROM).unwrap();

        for i in 0..size {
            // get account with likelyhood of similar distribution
            let mut idx_from: usize = from_dist.sample(&mut rng) % accounts.len();
            // get account with likelyhood of similar distribution
            let mut idx_to: usize = to_dist.sample(&mut rng) % accounts.len();

            while idx_from == idx_to {
                idx_to = to_dist.sample(&mut rng) % accounts.len();
            }

            let txn = accounts[idx_from]
                .transaction()
                .payload(
                    coin_transfer(
                        aptos_types::utility_coin::APTOS_COIN_TYPE.clone(),
                        *accounts[idx_to].address(),
                        1,
                    ))
                .sequence_number(seq_num[&idx_from])
                .sign();
            seq_num.insert(idx_from, seq_num[&idx_from] + 1);
            result.push(txn);
        }
        return result;
    }

    let dist : WeightedIndex<usize> = WeightedIndex::new(&COIN_DISTR).unwrap();
    let tx_weight_distr : WeightedIndex<usize> = WeightedIndex::new(&TX_LENGTH_WEIGHT).unwrap();
    let tx_num_writes_distr : WeightedIndex<usize> = WeightedIndex::new(&TX_WRITES_WEIGHT).unwrap();
    let tx_res_distr : WeightedIndex<usize> = WeightedIndex::new(&RES_DISTR).unwrap();

    let mut max_count:usize = 1;
    let max_value_opt = TX_WRITES.iter().max();
    match max_value_opt {
        Some(max) => { max_count = *max; },
        None      => println!("vec empty, wat!")
    }

    for i in 0..size {
        // let idx: usize = rng.gen::<usize>() % accounts.len();
        let idx: usize = (i as usize) % accounts.len();
        //let coin_1_num = (i as usize) % coins;

        let coin_1_num;
        if matches!(load_type, DEX)
        {
            coin_1_num = dist.sample(&mut rng) % coins;
        }
        else
        {
            coin_1_num = rng.gen::<usize>() % coins;
        }

        let coin_2_num = coin_1_num;

        // let coin_2_num = rng.gen::<usize>() % MAX_COIN_NUM;
        // let coin_1_num = idx;
        // let coin_2_num = idx;

        let mut coin: String = "CoinC".to_string();
        let mut coin_clone = coin.clone();
        let coin_number1_string = coin_1_num.to_string();
        let coin_number2_string = coin_2_num.to_string();
        coin.push_str(&coin_number1_string);
        coin_clone.push_str(&coin_number2_string);
        // println!("Coin1:{}", coin);
        // println!("Coin2:{}", coin_clone);

        //let coin_clone: &str = &coin.clone();
        let coin_id1: identifier::Identifier = identifier::Identifier::new(coin).unwrap();
        let coin_id2: identifier::Identifier = identifier::Identifier::new(coin_clone).unwrap();

        let coin_1 = TypeTag::Struct(Box::new(StructTag {
            address: owner.address().clone(),
            module: ident_str!("Exchange").to_owned(),
            name: coin_id1,
            type_params: vec![],
        }));

        let coin_2 = TypeTag::Struct(Box::new(StructTag {
            address: owner.address().clone(),
            module: ident_str!("Exchange").to_owned(),
            name: coin_id2,
            type_params: vec![],
        }));

        let entry_function;

        if matches!(load_type, SOLANA)
        {
            let length = TX_LENGTHS[tx_weight_distr.sample(&mut rng)] * max_count as u64;
            let num_writes = TX_WRITES[tx_num_writes_distr.sample(&mut rng)];
            let mut writes: Vec<u64> = Vec::new();
            let mut i = 0;
            while i < num_writes {
                i+=1;
                writes.push(tx_res_distr.sample(&mut rng) as u64);
            }

            entry_function = EntryFunction::new(
                module_id.clone(),
                ident_str!("loop_exchange").to_owned(),
                vec![],
                vec![bcs::to_bytes(&length).unwrap(), bcs::to_bytes(&writes).unwrap()],
            );
        }
        else
        {
            entry_function = EntryFunction::new(
                module_id.clone(),
                ident_str!("exchange").to_owned(),
                vec![coin_1.clone(), coin_2.clone()],
                vec![],
            );
        }

        let txn = accounts[idx]
            .transaction()
            .entry_function(entry_function.clone())
            .sequence_number(seq_num[&idx])
            .sign();
        seq_num.insert(idx, seq_num[&idx] + 1);
        result.push(txn);
    }
    // println!("{:?}", result);

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

    let seed: Vec<u8> = vec![0];
    let entry_function = EntryFunction::new(
        mid.clone(),
        ident_str!("init").to_owned(),
        vec![],
        vec![bcs::to_bytes(&seed).unwrap()],
    );
    let init_txn = owner_account
        .account()
        .transaction()
        .entry_function(entry_function)
        .sequence_number(SEQ_NUM + 1)
        .sign();
    println!("CALLING INIT!");
    let result = executor.execute_and_apply(init_txn);

    println!("PASSED!");

    (owner_account, mid)
}
