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
use rand::seq::index::IndexVec::USize;
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
        let mut rng: ThreadRng = thread_rng();
        println!("Generating {:?} {} transactions", self.load_type, needed);

        let mut resource_distribution_vec:Vec<f64> = vec![1.0,1.0,1.0,1.0];
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
        //normalize_distribution_vectors(sum, extended_size, &mut p2p_sender_distr_vec);

        let p2p_receiver_distribution: WeightedIndex<f64> = WeightedIndex::new(&p2p_receiver_distr_vec).unwrap();
        let p2p_sender_distribution: WeightedIndex<f64> = WeightedIndex::new(&p2p_sender_distr_vec).unwrap();

        for i in 0..needed {
            let mut sender_id: usize = (i as usize) % accounts.len();

            if matches!(load_type, SOLANA)
            {
                let cost_sample = solana_cost_options[rand::thread_rng().gen_range(0..solana_cost_options.len()) as usize];
                let write_len_sample = solana_len_options[rand::thread_rng().gen_range(0..solana_len_options.len()) as usize];

                let mut writes: Vec<u64> = Vec::new();
                let mut i = 0;
                while i < write_len_sample {
                    i+=1;
                    writes.push(general_resource_distribution.sample(&mut rng) as u64);
                }

                let length = max(1, cost_sample.round() as usize);

                requests.push(self.package.our_spec_transaction(accounts[sender_id],
                                                                &self.txn_factory,
                                                                ident_str!("loop_exchange").to_owned(),
                                                                vec![],
                                                                vec![bcs::to_bytes(&self.owner).unwrap(), bcs::to_bytes(&length).unwrap(), bcs::to_bytes(&writes).unwrap()]));

            }
            else if matches!(load_type, P2PTX)
            {
                let receiver_id = p2p_receiver_distribution.sample(&mut rng) % accounts.len();
                let sender_id = p2p_sender_distribution.sample(&mut rng) % accounts.len();

                requests.push(self.package.our_spec_transaction(accounts[sender_id],
                                                                &self.txn_factory,
                                                                ident_str!("exchangetwo").to_owned(),
                                                                vec![],
                                                                vec![bcs::to_bytes(&self.owner).unwrap(), bcs::to_bytes(&receiver_id).unwrap(), bcs::to_bytes(&sender_id).unwrap()]));
            }
            else
            {
                let resource_id = general_resource_distribution.sample(&mut rng);
                if matches!(load_type, NFT)
                {
                    sender_id = nft_sender_distribution.sample(&mut rng) % accounts.len();
                }

                requests.push(self.package.our_spec_transaction(accounts[sender_id],
                                                                &self.txn_factory,
                                                                ident_str!("exchange").to_owned(),
                                                                vec![],
                                                                vec![bcs::to_bytes(&self.owner).unwrap(), bcs::to_bytes(&resource_id).unwrap()]));
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
