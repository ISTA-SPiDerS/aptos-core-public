// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

use aptos_types::{
    account_address::AccountAddress,
    transaction::{
        EntryFunction, ExecutionStatus, Module, SignedTransaction, Transaction, TransactionStatus,
    },
};
use aptos_language_e2e_tests::{
    account::Account, account::AccountData, compile::compile_source_module, current_function_name,
    executor::FakeExecutor,
};

use rand::Rng;
use regex::Regex;
use std::{collections::hash_map::HashMap, format, fs, str::FromStr};
use move_core_types::ident_str;
use move_core_types::language_storage::{ModuleId, StructTag, TypeTag};
use aptos_types::transaction::{ExecutionMode, Profiler};

const INITIAL_BALANCE: u64 = 1_000_000;
const SEQ_NUM: u64 = 10;

fn run_benchmarkasdsada() {
    let module_path = "test_module.move";
    let num_accounts = 5;
    let block_size = 100;

    let mut executor = FakeExecutor::from_head_genesis();
    executor.set_golden_file(current_function_name!());

    let accounts = executor.create_accounts(num_accounts, INITIAL_BALANCE, SEQ_NUM);

    let (module_owner, module_id) = create_module(&mut executor, module_path.to_string());
    let mut seq_num = HashMap::new();

    for idx in 0..num_accounts {
        seq_num.insert(idx, SEQ_NUM);
    }

    let block = create_block(block_size, module_owner, accounts, &mut seq_num, &module_id);
    let block_result = executor
        .execute_transaction_block_parallel(
            block
                .into_iter()
                .map(Transaction::UserTransaction)
                .collect::<Vec<Transaction>>()
                .into(), 4,
            ExecutionMode::Standard,
            &mut Profiler::new()
        )
        .unwrap();
    for result in block_result {
        match result.status() {
            TransactionStatus::Keep(status) => {
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

fn create_block(
    size: u64,
    owner: AccountData,
    accounts: Vec<Account>,
    seq_num: &mut HashMap<usize, u64>,
    module_id: &ModuleId,
) -> Vec<SignedTransaction> {
    let mut rng = rand::thread_rng();
    let mut result = vec![];
    for i in 0..size {
        let idx: usize = rng.gen::<usize>() % accounts.len();

        let coin_c = TypeTag::Struct(Box::new(StructTag {
            address: owner.address().clone(),
            module: ident_str!("Exchange").to_owned(),
            name: ident_str!("CoinC1").to_owned(),
            type_params: vec![],
        }));

        let seed: Vec<u8> = vec![0];
        let entry_function = EntryFunction::new(
            module_id.clone(),
            ident_str!("exchange").to_owned(),
            vec![coin_c.clone(), coin_c.clone()],
            vec![],
        );
        let txn = accounts[idx]
            .transaction()
            .entry_function(entry_function)
            .sequence_number(seq_num[&idx])
            .sign();
        seq_num.insert(idx, seq_num[&idx] + 1);
        result.push(txn);
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
