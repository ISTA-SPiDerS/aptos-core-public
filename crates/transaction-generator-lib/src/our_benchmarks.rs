// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use std::any::Any;
use std::borrow::Borrow;
use std::cmp::max;
use super::{
    publishing::{module_simple::EntryPoints, publish_util::Package},
    TransactionExecutor,
};
use aptos_logger::info;
use async_trait::async_trait;
use rand::prelude::*;
use std::collections::{BTreeMap, HashMap};
use anyhow::anyhow;
use rand::distributions::WeightedIndex;
use aptos_framework::named_addresses;
use aptos_rest_client::aptos_api_types::AccountData;
use aptos_sdk::bcs;
use aptos_sdk::move_types::{ident_str, identifier};
use aptos_sdk::move_types::account_address::AccountAddress;
use aptos_sdk::move_types::identifier::Identifier;
use aptos_sdk::move_types::language_storage::{ModuleId, StructTag, TypeTag};
use aptos_sdk::transaction_builder::TransactionFactory;
use aptos_sdk::types::{account_config, LocalAccount};
use aptos_sdk::types::transaction::{EntryFunction, Module, SignedTransaction};
use crate::{TransactionGenerator, TransactionGeneratorCreator};
use crate::account_activity_distribution::{COIN_DISTR, TX_FROM, TX_NFT_FROM, TX_NFT_TO, TX_TO};
use crate::publishing::publish_util::PackageHandler;
use crate::solana_distribution::{COST_DISTR, LEN_DISTR, RES_DISTR};
use crate::uniswap_distribution::{AVG, BURSTY};

pub struct OurBenchmark {
    txn_factory: TransactionFactory,
    load_type: LoadType,
    package: Package,
    owner: AccountAddress
}

#[derive(Debug, Copy, Clone)]
pub enum LoadType {
    NFT,
    SOLANA,
    DEXAVG,
    DEXBURSTY,
    P2PTX,
}

impl OurBenchmark {
    pub async fn new(
        txn_factory: TransactionFactory,
        load_type: LoadType,
        package: Package,
        owner: AccountAddress
    ) -> Self {
        Self {
            txn_factory,
            load_type,
            package,
            owner
        }
    }
}

impl TransactionGenerator for OurBenchmark {
    fn generate_transactions(
        &mut self,
        mut accounts: Vec<&mut LocalAccount>,
        transactions_per_account: usize,
    ) -> Vec<SignedTransaction> {
        let needed = accounts.len();
        let mut requests = Vec::with_capacity(needed);
        let load_type = self.load_type;
        let coins = COIN_DISTR.len();
        let mut rng: ThreadRng = thread_rng();
        println!("Generating {} transactions", needed);

        let mut from_vec_p2p:Vec<f64> = vec![];
        let mut to_vec_p2p:Vec<f64> = vec![];

        for (key, value) in TX_TO {
            for i in 0..value {
                to_vec_p2p.push(key);
            }
        }

        for (key, value) in TX_FROM {
            for i in 0..value {
                from_vec_p2p.push(key);
            }
        }

        let to_dist_p2p:WeightedIndex<f64> = WeightedIndex::new(&to_vec_p2p).unwrap();
        let from_dist_p2p:WeightedIndex<f64> = WeightedIndex::new(&from_vec_p2p).unwrap();
        
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

        let mut from_vec: Vec<usize> = vec![];
        for (key, value) in TX_NFT_FROM {
            for i in 0..(value as usize) {
                from_vec.push(key as usize)
            }
        }
        let from_dist: WeightedIndex<usize> = WeightedIndex::new(&from_vec).unwrap();

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
            }
            else if matches!(load_type, LoadType::NFT)
            {
                idx = from_dist.sample(&mut rng) % accounts.len();
                coin_1_num = (dist.sample(&mut rng) % coins) as u64;
            }
            else {
                coin_1_num = (rng.gen::<usize>() % coins) as u64;
            }

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

                let length = max(1, cost.round() as usize);

                requests.push(self.package.our_spec_transaction(accounts[idx],
                                                                &self.txn_factory,
                                                                ident_str!("loop_exchange").to_owned(),
                                                                vec![],
                                                                vec![bcs::to_bytes(&self.owner).unwrap(), bcs::to_bytes(&length).unwrap(), bcs::to_bytes(&writes).unwrap()]));

            }
            else if matches!(load_type, LoadType::P2PTX) {
                let mut idx_from: usize = from_dist_p2p.sample(&mut rng) % accounts.len();
                // get account with likelyhood of similar distribution
                let mut idx_to: usize = to_dist_p2p.sample(&mut rng) % accounts.len();

                while idx_from == idx_to {
                    idx_to = to_dist_p2p.sample(&mut rng) % accounts.len();
                }

                requests.push(self.package.our_spec_transaction(accounts[idx],
                                                                &self.txn_factory,
                                                                ident_str!("exchangetwo").to_owned(),
                                                                vec![],
                                                                vec![bcs::to_bytes(&self.owner).unwrap(), bcs::to_bytes(&idx_to).unwrap(), bcs::to_bytes(&idx_from).unwrap()]));
            }
            else {
                requests.push(self.package.our_spec_transaction(accounts[idx],
                                                  &self.txn_factory,
                                                  ident_str!("exchange").to_owned(),
                                                  vec![],
                                                  vec![bcs::to_bytes(&self.owner).unwrap(), bcs::to_bytes(&coin_1_num).unwrap()]));
            }

        }

        requests
    }
}

pub struct OurBenchmarkGeneratorCreator {
    txn_factory: TransactionFactory,
    load_type: LoadType,
    package: Package,
    owner: AccountAddress
}

impl OurBenchmarkGeneratorCreator {
    pub async fn new(
        txn_factory: TransactionFactory,
        load_type: LoadType,
        account: &mut LocalAccount,
        txn_executor: &dyn TransactionExecutor,
    ) -> Self {


        let mut requests = Vec::with_capacity(1);
        let mut package_handler = PackageHandler::new();
        let package = package_handler.pick_benchmark_package(account);
        let txn = package.publish_transaction(account, &txn_factory);
        info!("Publishing {} packages {}", requests.len(), txn.authenticator());

        requests.push(txn);

        txn_executor.execute_transactions(&requests).await.unwrap();
        info!("Done publishing {} packages", requests.len());

        Self {
            txn_factory,
            load_type,
            package,
            owner: account.address()
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
                self.load_type,
                self.package.clone(),
                self.owner.clone()
            )
            .await,
        )
    }
}
