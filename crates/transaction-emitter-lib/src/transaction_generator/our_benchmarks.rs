// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use std::borrow::Borrow;
use super::{
    publishing::{module_simple::EntryPoints, publish_util::Package},
    TransactionExecutor,
};
use crate::{
    emitter::account_minter::create_and_fund_account_request,
    transaction_generator::{TransactionGenerator, TransactionGeneratorCreator},
    emitter::{account_activity_distribution::{COIN_DISTR, TX_FROM, TX_NFT_FROM, TX_NFT_TO, TX_TO}},
    emitter::{solana_distribution::{RES_DISTR, COST_DISTR, LEN_DISTR}},
    emitter::uniswap_distribution::{AVG, BURSTY},
};
use aptos_logger::info;
use aptos_sdk::{bcs, transaction_builder::{aptos_stdlib::aptos_token_stdlib, TransactionFactory}, types::{account_address::AccountAddress, transaction::SignedTransaction, LocalAccount}};

use async_trait::async_trait;
use rand::prelude::*;
use std::collections::{BTreeMap, HashMap};
use anyhow::anyhow;
use rand::distributions::WeightedIndex;
use aptos_framework::named_addresses;
use aptos_rest_client::aptos_api_types::AccountData;
use aptos_sdk::move_types::{ident_str, identifier};
use aptos_sdk::move_types::identifier::Identifier;
use aptos_sdk::move_types::language_storage::{ModuleId, StructTag, TypeTag};
use aptos_sdk::types::account_config;
use aptos_sdk::types::transaction::{EntryFunction, Module};

pub struct OurBenchmark {
    txn_factory: TransactionFactory,
    entry_point: EntryPoints,
}

impl OurBenchmark {
    pub async fn new(
        txn_factory: TransactionFactory,
        entry_point: EntryPoints,

    ) -> Self {
        Self {
            txn_factory,
            entry_point
        }
    }
}

impl TransactionGenerator for OurBenchmark {
    fn generate_transactions(
        &mut self,
        mut accounts: Vec<&mut LocalAccount>,
        transactions_per_account: usize,
    ) -> Vec<SignedTransaction> {
        let needed = accounts.len() * transactions_per_account * 10;
        let mut requests = Vec::with_capacity(needed);
        let loadtype = self.entry_point;
        let coins = COIN_DISTR.len();
        let mut rng: ThreadRng = thread_rng();
        let mut from_dist: Box<WeightedIndex<usize>> = Box::new(WeightedIndex::new(&vec![17,42]).unwrap());
        let mut dist: Box<WeightedIndex<f64>> = Box::new(WeightedIndex::new(&vec![17.0,42.0]).unwrap());
        let mut tx_num_writes_distr : Box<WeightedIndex<usize>> = Box::new(WeightedIndex::new(&vec![17,42]).unwrap());
        let mut tx_cost_distr : Box<WeightedIndex<usize>> = Box::new(WeightedIndex::new(&vec![17,42]).unwrap());
        let mut cost_options:Vec<f64> = vec![];
        let mut len_options:Vec<usize> = vec![];

        if matches!(loadtype, DEXAVG) {
            let mut distr:Vec<f64> = vec![];

            for (key, value) in AVG {
                for i in 0..value {
                    distr.push(key)
                }
            }
            // println!("{}", distr.len());

            let mut len_distr:Vec<usize> = vec![];

            for (key, value) in LEN_DISTR {
                len_options.push(key.round() as usize);
                len_distr.push(value as usize);
            }

            // let mut cost_options:Vec<f64> = vec![];
            // let mut cost_distr:Vec<usize> = vec![];

            // for (key, value) in COST_DISTR {
            //     cost_options.push(key);
            //     cost_distr.push(value as usize);
            // }

            // let tx_num_writes_distr : WeightedIndex<usize> = WeightedIndex::new(&len_distr).unwrap();
            // let tx_cost_distr : WeightedIndex<usize> = WeightedIndex::new(&cost_distr).unwrap();

            *dist = WeightedIndex::new(&distr).unwrap();

            let mut fromVec: Vec<usize> = vec![];
            for (key, value) in TX_NFT_FROM {
                for i in 0..(value as usize) {
                    fromVec.push(key as usize)
                }
            }
            *from_dist = WeightedIndex::new(&fromVec).unwrap();

            let mut max_count:usize = 1;
            let max_value_opt = len_options.iter().max();
            match max_value_opt {
                Some(max) => { max_count = *max; },
                None      => println!("vec empty, wat!")
            }
            // todo: change the 100 to number of coins
        }
        if matches!(loadtype, DEXBURSTY) {
            let mut distr:Vec<f64> = vec![];

            for (key, value) in BURSTY {
                for i in 0..value {
                    distr.push(key)
                }
            }
            // println!("{}", distr.len());

            // let mut len_options:Vec<usize> = vec![];
            let mut len_distr:Vec<usize> = vec![];

            for (key, value) in LEN_DISTR {
                len_options.push(key.round() as usize);
                len_distr.push(value as usize);
            }

            // let mut cost_options:Vec<f64> = vec![];
            // let mut cost_distr:Vec<usize> = vec![];

            // for (key, value) in COST_DISTR {
            //     cost_options.push(key);
            //     cost_distr.push(value as usize);
            // }

            // let tx_num_writes_distr : WeightedIndex<usize> = WeightedIndex::new(&len_distr).unwrap();
            // let tx_cost_distr : WeightedIndex<usize> = WeightedIndex::new(&cost_distr).unwrap();

            *dist = WeightedIndex::new(&distr).unwrap();

            let mut fromVec: Vec<usize> = vec![];
            for (key, value) in TX_NFT_FROM {
                for i in 0..(value as usize) {
                    fromVec.push(key as usize)
                }
            }
            *from_dist  = WeightedIndex::new(&fromVec).unwrap();

            let mut max_count:usize = 1;
            let max_value_opt = len_options.iter().max();
            match max_value_opt {
                Some(max) => { max_count = *max; },
                None      => println!("vec empty, wat!")
            }
            // todo: change the 100 to number of coins
        }
        if matches!(loadtype, NFT) {
            let mut distr:Vec<f64> = vec![];
            for (key, value) in TX_NFT_TO {
                for i in 0..value {
                    distr.push(key)
                }
            }
            // let mut len_options:Vec<usize> = vec![];
            let mut len_distr:Vec<usize> = vec![];

            for (key, value) in LEN_DISTR {
                len_options.push(key.round() as usize);
                len_distr.push(value as usize);
            }

            // let mut cost_options:Vec<f64> = vec![];
            // let mut cost_distr:Vec<usize> = vec![];

            // for (key, value) in COST_DISTR {
            //     cost_options.push(key);
            //     cost_distr.push(value as usize);
            // }

            // let tx_num_writes_distr : WeightedIndex<usize> = WeightedIndex::new(&len_distr).unwrap();
            // let tx_cost_distr : WeightedIndex<usize> = WeightedIndex::new(&cost_distr).unwrap();

            *dist = WeightedIndex::new(&distr).unwrap();

            let mut fromVec: Vec<usize> = vec![];
            for (key, value) in TX_NFT_FROM {
                for i in 0..(value as usize) {
                    fromVec.push(key as usize)
                }
            }
            *from_dist = WeightedIndex::new(&fromVec).unwrap();

            let mut max_count:usize = 1;
            let max_value_opt = len_options.iter().max();
            match max_value_opt {
                Some(max) => { max_count = *max; },
                None      => println!("vec empty, wat!")
            }
        }
        if matches!(loadtype, SOLANA) {
            let mut distr:Vec<f64> = vec![];
            for (key, value) in RES_DISTR {
                for i in 0..value {
                    distr.push(key)
                }
            }
            // let mut len_options:Vec<usize> = vec![];
            let mut len_distr:Vec<usize> = vec![];

            for (key, value) in LEN_DISTR {
                len_options.push(key.round() as usize);
                len_distr.push(value as usize);
            }

            let mut cost_distr:Vec<usize> = vec![];

            for (key, value) in COST_DISTR {
                cost_options.push(key);
                cost_distr.push(value as usize);
            }

            *tx_num_writes_distr = WeightedIndex::new(&len_distr).unwrap();
            *tx_cost_distr = WeightedIndex::new(&cost_distr).unwrap();

            *dist = WeightedIndex::new(&distr).unwrap();

            let mut fromVec: Vec<usize> = vec![];
            for (key, value) in TX_NFT_FROM {
                for i in 0..(value as usize) {
                    fromVec.push(key as usize)
                }
            }
            *from_dist = WeightedIndex::new(&fromVec).unwrap();

            let mut max_count:usize = 1;
            let max_value_opt = len_options.iter().max();
            match max_value_opt {
                Some(max) => { max_count = *max; },
                None      => println!("vec empty, wat!")
            }

        }

        for i in 0..needed {
            let mut idx = (i as usize) % accounts.len();
            let coin_1_num:u64;
            if matches!(loadtype, DEXAVG) || matches!(loadtype, DEXBURSTY)
            {
                coin_1_num = (dist.sample(&mut rng) % coins) as u64;
            }
            else if matches!(loadtype, NFT)
            {
                idx = from_dist.sample(&mut rng) % accounts.len();
                coin_1_num = (dist.sample(&mut rng) % coins) as u64;
            }
            else {
                coin_1_num = (rng.gen::<usize>() % coins) as u64;
            }
            let mut length: usize = 0;
            let mut writes: Vec<u64> = vec![];
            if matches!(loadtype, SOLANA) {
                let cost = cost_options[tx_cost_distr.sample(&mut rng)];
                let num_writes = len_options[tx_num_writes_distr.sample(&mut rng)];
                // let mut writes: Vec<u64> = Vec::new();
                let mut i = 0;
                while i < num_writes {
                    i+=1;
                    writes.push(dist.sample(&mut rng) as u64);
                }
                length = cost.round() as usize;
            }

            let entry_function = EntryFunction::new(
                ModuleId::new(
                    account_config::CORE_CODE_ADDRESS,
                    ident_str!("benchmark").to_owned(),
                ),
                ident_str!("exchange").to_owned(),
                vec![],
                vec![bcs::to_bytes(&coin_1_num).unwrap()],
            );

            requests.push(accounts[idx].sign_with_transaction_builder(self.txn_factory.entry_function(entry_function)));
        }
        requests
    }
}

pub struct OurBenchmarkGeneratorCreator {
    txn_factory: TransactionFactory,
    entry_point: EntryPoints,
}

impl OurBenchmarkGeneratorCreator {
    pub async fn new(
        txn_factory: TransactionFactory,
        init_txn_factory: TransactionFactory,
        root_account: &mut LocalAccount,
        txn_executor: &dyn TransactionExecutor,
        entry_point: EntryPoints,
    ) -> Self {
        Self {
            txn_factory,
            entry_point
        }
    }
}

#[async_trait]
impl TransactionGeneratorCreator for OurBenchmarkGeneratorCreator {
    async fn create_transaction_generator(&mut self) -> Box<dyn TransactionGenerator> {
        Box::new(
            OurBenchmark::new(
                self.txn_factory.clone(),
                self.entry_point
            )
            .await,
        )
    }
}
