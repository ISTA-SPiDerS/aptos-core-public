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
use crate::account_activity_distribution::{TX_FROM, TX_NFT_FROM, TX_NFT_TO, TX_TO};
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
        println!("Enter generate");
        let needed = accounts.len();
        let mut requests = Vec::with_capacity(needed);

        let load_type = self.load_type;
        let mut rng: ThreadRng = thread_rng();
        println!("Generating {:?} {} transactions", self.load_type, needed);

       
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
