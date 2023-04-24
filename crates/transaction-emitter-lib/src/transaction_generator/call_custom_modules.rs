// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use super::{
    publishing::{module_simple::EntryPoints, publish_util::Package},
    TransactionExecutor,
};
use crate::transaction_generator::{
    publishing::publish_util::PackageHandler, TransactionGenerator, TransactionGeneratorCreator,
};
use crate::{
    emitter::{account_activity_distribution::{COIN_DISTR, TX_FROM, TX_NFT_FROM, TX_NFT_TO, TX_TO}},
    emitter::{solana_distribution::{RES_DISTR, COST_DISTR, LEN_DISTR}},
    emitter::uniswap_distribution::{AVG, BURSTY},

};
use aptos_logger::info;
use aptos_sdk::{
    transaction_builder::TransactionFactory,
    types::{transaction::SignedTransaction, LocalAccount},
};
use async_trait::async_trait;
use rand::{rngs::StdRng, seq::SliceRandom, SeedableRng};
use std::sync::Arc;
use rand::distributions::WeightedIndex;
use rand::prelude::*;

pub struct CallCustomModulesGenerator {
    rng: StdRng,
    txn_factory: TransactionFactory,
    packages: Arc<Vec<Package>>,
    entry_point: EntryPoints,
}

impl CallCustomModulesGenerator {
    pub fn new(
        rng: StdRng,
        txn_factory: TransactionFactory,
        packages: Arc<Vec<Package>>,
        entry_point: EntryPoints,
    ) -> Self {
        Self {
            rng,
            txn_factory,
            packages,
            entry_point,
        }
    }
}

#[async_trait]
impl TransactionGenerator for CallCustomModulesGenerator {
    fn generate_transactions(
        &mut self,
        mut accounts: Vec<&mut LocalAccount>,
        transactions_per_account: usize,
    ) -> Vec<SignedTransaction> {
        let needed = accounts.len() * transactions_per_account * 10;
        let mut requests = Vec::with_capacity(needed);
        let loadtype = self.entry_point;
        let mut coin_1_num: usize = 0;
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

        for i in 0..transactions_per_account * accounts.len() {
            let mut idx = (i as usize) % accounts.len();
            let coin_1_num;
            if matches!(loadtype, DEXAVG) || matches!(loadtype, DEXBURSTY)
            {
                coin_1_num = dist.sample(&mut rng) % coins;
            }
            else if matches!(loadtype, NFT)
            {
                idx = from_dist.sample(&mut rng) % accounts.len();
                coin_1_num = dist.sample(&mut rng) % coins;
            }
            else {
                coin_1_num = rng.gen::<usize>() % coins;
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

            let request = self
                .packages
                .choose(&mut self.rng)
                .unwrap()
                .use_specific_transaction(
                    self.entry_point,
                    accounts[idx],
                    &self.txn_factory,
                    Some(&mut self.rng),
                    None,
                    coin_1_num,
                    length,
                    writes,
                );
            requests.push(request);
        }
        requests
    }
}

pub struct CallCustomModulesCreator {
    txn_factory: TransactionFactory,
    packages: Arc<Vec<Package>>,
    entry_point: EntryPoints,
}

impl CallCustomModulesCreator {
    pub async fn new(
        txn_factory: TransactionFactory,
        init_txn_factory: TransactionFactory,
        accounts: &mut [LocalAccount],
        txn_executor: &dyn TransactionExecutor,
        entry_point: EntryPoints,
        num_modules: usize,
    ) -> Self {
        let mut rng = StdRng::from_entropy();
        assert!(accounts.len() >= num_modules);
        let mut requests = Vec::with_capacity(accounts.len());
        let mut package_handler = PackageHandler::new();
        let mut packages = Vec::new();
        for account in accounts.iter_mut().take(num_modules) {
            let package = package_handler.pick_package(&mut rng, account);
            let txn = package.publish_transaction(account, &init_txn_factory);
            requests.push(txn);
            packages.push(package);
        }
        info!("Publishing {} packages", requests.len());
        txn_executor.execute_transactions(&requests).await.unwrap();
        info!("Done publishing {} packages", requests.len());

        Self {
            txn_factory,
            packages: Arc::new(packages),
            entry_point,
        }
    }
}

#[async_trait]
impl TransactionGeneratorCreator for CallCustomModulesCreator {
    async fn create_transaction_generator(&mut self) -> Box<dyn TransactionGenerator> {
        Box::new(CallCustomModulesGenerator::new(
            StdRng::from_entropy(),
            self.txn_factory.clone(),
            self.packages.clone(),
            self.entry_point,
        ))
    }
}
