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
    load_type: LoadType,
}

#[derive(Clone, Copy, Debug)]
pub enum LoadType
{
    DEXAVG,
    DEXBURSTY,
    P2PTX,
    SOLANA,
    NFT
}


impl OurBenchmark {
    pub async fn new(
        txn_factory: TransactionFactory,
        load_type: LoadType,

    ) -> Self {
        Self {
            txn_factory,
            load_type
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
        let load_type = self.load_type;
        let coins = COIN_DISTR.len();
        let mut rng: ThreadRng = thread_rng();
        println!("Generating {} transactions", needed);

        if matches!(load_type, LoadType::P2PTX)
        {
            let mut fromVec:Vec<f64> = vec![];
            let mut toVec:Vec<f64> = vec![];

            for (key, value) in TX_TO {
                for i in 0..value {
                    toVec.push(key);
                }
            }

            for (key, value) in TX_FROM {
                for i in 0..value {
                    fromVec.push(key);
                }
            }

            let to_dist:WeightedIndex<f64> = WeightedIndex::new(&toVec).unwrap();
            let from_dist:WeightedIndex<f64> = WeightedIndex::new(&fromVec).unwrap();

            for i in 0..needed {
                // get account with likelyhood of similar distribution
                let mut idx_from: usize = from_dist.sample(&mut rng) % accounts.len();
                // get account with likelyhood of similar distribution
                let mut idx_to: usize = to_dist.sample(&mut rng) % accounts.len();

                while idx_from == idx_to {
                    idx_to = to_dist.sample(&mut rng) % accounts.len();
                }

                let receiver = accounts[idx_to].address().clone();
                requests.push(accounts[idx_from].sign_with_transaction_builder(self.txn_factory.transfer(receiver, 1)));
            }
            return requests;
        }

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
                for i in 0..value {
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

        for i in 0..needed {
            let mut idx: usize = (i as usize) % accounts.len();

            let coin_1_num:u64;
            if matches!(load_type, LoadType::DEXAVG) || matches!(load_type, LoadType::DEXBURSTY)
            {
                coin_1_num = (dist.sample(&mut rng) % coins) as u64;
            } else if matches!(load_type, LoadType::NFT)
            {
                idx = from_dist.sample(&mut rng) % accounts.len();
                coin_1_num = (dist.sample(&mut rng) % coins) as u64;
            } else {
                coin_1_num = (rng.gen::<usize>() % coins) as u64;
            }

            let entry_function;

            if matches!(load_type, LoadType::SOLANA)
            {
                let cost = cost_options[tx_cost_distr.sample(&mut rng)];
                let num_writes = len_options[tx_num_writes_distr.sample(&mut rng)];

                let mut writes: Vec<u64> = Vec::new();
                let mut i = 0;
                while i < num_writes {
                    i += 1;
                    writes.push(dist.sample(&mut rng) as u64);
                }

                let length = cost.round() as usize;

                entry_function = EntryFunction::new(
                    ModuleId::new(
                        account_config::CORE_CODE_ADDRESS,
                        ident_str!("benchmark").to_owned(),
                    ),
                    ident_str!("loop_exchange").to_owned(),
                    vec![],
                    vec![bcs::to_bytes(&length).unwrap(), bcs::to_bytes(&writes).unwrap()],
                );
            } else {
                entry_function = EntryFunction::new(
                    ModuleId::new(
                        account_config::CORE_CODE_ADDRESS,
                        ident_str!("benchmark").to_owned(),
                    ),
                    ident_str!("exchange").to_owned(),
                    vec![],
                    vec![bcs::to_bytes(&coin_1_num).unwrap()],
                );
            }

            requests.push(accounts[idx].sign_with_transaction_builder(self.txn_factory.entry_function(entry_function)));
        }

        requests
    }
}

pub struct OurBenchmarkGeneratorCreator {
    txn_factory: TransactionFactory,
    load_type: LoadType,
}

impl OurBenchmarkGeneratorCreator {
    pub async fn new(
        txn_factory: TransactionFactory,
        load_type: LoadType,
    ) -> Self {
        Self {
            txn_factory,
            load_type
        }
    }
}

#[async_trait]
impl TransactionGeneratorCreator for OurBenchmarkGeneratorCreator {
    async fn create_transaction_generator(&mut self) -> Box<dyn TransactionGenerator> {
        println!("Starting our Benchmark creator!");
        Box::new(
            OurBenchmark::new(
                self.txn_factory.clone(),
                self.load_type
            )
            .await,
        )
    }
}
