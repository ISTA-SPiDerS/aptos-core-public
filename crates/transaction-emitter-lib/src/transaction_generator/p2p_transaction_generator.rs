// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use crate::{
    emitter::{account_activity_distribution::{COIN_DISTR, TX_FROM, TX_NFT_FROM, TX_NFT_TO, TX_TO}},
    transaction_generator::{TransactionGenerator, TransactionGeneratorCreator}
};

use aptos_infallible::RwLock;
use aptos_sdk::{
    move_types::account_address::AccountAddress,
    transaction_builder::{aptos_stdlib, TransactionFactory},
    types::{chain_id::ChainId, transaction::SignedTransaction, LocalAccount},
};
use async_trait::async_trait;
use rand::{
    distributions::{Distribution, Standard},
    prelude::SliceRandom,
    rngs::StdRng,
    Rng, RngCore, SeedableRng,
};
use std::{cmp::max, sync::Arc};
use rand::distributions::WeightedIndex;

pub struct P2PTransactionGenerator {
    rng: StdRng,
    send_amount: u64,
    txn_factory: TransactionFactory,
    all_addresses: Arc<RwLock<Vec<AccountAddress>>>,
    invalid_transaction_ratio: usize,
    fromVec: Vec<f64>,
    toVec: Vec<f64>,
}

impl P2PTransactionGenerator {
    pub fn new(
        rng: StdRng,
        send_amount: u64,
        txn_factory: TransactionFactory,
        all_addresses: Arc<RwLock<Vec<AccountAddress>>>,
        invalid_transaction_ratio: usize,
    ) -> Self {

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

        Self {
            rng,
            send_amount,
            txn_factory,
            all_addresses,
            invalid_transaction_ratio,
            fromVec,
            toVec
        }
    }

    fn gen_single_txn(
        &self,
        from: &mut LocalAccount,
        to: &AccountAddress,
        num_coins: u64,
        txn_factory: &TransactionFactory,
    ) -> SignedTransaction {
        from.sign_with_transaction_builder(
            txn_factory.payload(aptos_stdlib::aptos_coin_transfer(*to, num_coins)),
        )
    }
}

#[derive(Debug)]
enum InvalidTransactionType {
    /// invalid tx with wrong chain id
    ChainId,
    /// invalid tx with sender not on chain
    Sender,
    /// invalid tx with receiver not on chain
    Receiver,
    /// duplicate an exist tx
    Duplication,
}

impl Distribution<InvalidTransactionType> for Standard {
    fn sample<R: RngCore + ?Sized>(&self, rng: &mut R) -> InvalidTransactionType {
        match rng.gen_range(0, 4) {
            0 => InvalidTransactionType::ChainId,
            1 => InvalidTransactionType::Sender,
            2 => InvalidTransactionType::Receiver,
            _ => InvalidTransactionType::Duplication,
        }
    }
}

impl TransactionGenerator for P2PTransactionGenerator {
    fn generate_transactions(
        &mut self,
        mut accounts: Vec<&mut LocalAccount>,
        transactions_per_account: usize,
    ) -> Vec<SignedTransaction> {
        println!("wat {}", accounts.len());

        let mut requests = Vec::with_capacity(accounts.len()*10);
        let mut num_valid_tx = accounts.len()*10;

        let to_dist:WeightedIndex<f64> = WeightedIndex::new(&self.toVec).unwrap();
        let from_dist:WeightedIndex<f64> = WeightedIndex::new(&self.fromVec).unwrap();

        for i in 0..num_valid_tx {
            // get account with likelyhood of similar distribution
            let mut idx_from: usize = from_dist.sample(&mut self.rng) % accounts.len();
            // get account with likelyhood of similar distribution
            let mut idx_to: usize = to_dist.sample(&mut self.rng) % accounts.len();

            while idx_from == idx_to {
                idx_to = to_dist.sample(&mut self.rng) % accounts.len();
            }

            let receiver = &(accounts[idx_to].address()).clone();
            let request = self.gen_single_txn(accounts[idx_from], receiver, self.send_amount, &self.txn_factory);
            requests.push(request);
        }

        println!("generated {}", requests.len());
        requests
    }
}

pub struct P2PTransactionGeneratorCreator {
    txn_factory: TransactionFactory,
    amount: u64,
    all_addresses: Arc<RwLock<Vec<AccountAddress>>>,
    invalid_transaction_ratio: usize,
}

impl P2PTransactionGeneratorCreator {
    pub fn new(
        txn_factory: TransactionFactory,
        amount: u64,
        all_addresses: Arc<RwLock<Vec<AccountAddress>>>,
        invalid_transaction_ratio: usize,
    ) -> Self {
        Self {
            txn_factory,
            amount,
            all_addresses,
            invalid_transaction_ratio,
        }
    }
}

#[async_trait]
impl TransactionGeneratorCreator for P2PTransactionGeneratorCreator {
    async fn create_transaction_generator(&mut self) -> Box<dyn TransactionGenerator> {
        Box::new(P2PTransactionGenerator::new(
            StdRng::from_entropy(),
            self.amount,
            self.txn_factory.clone(),
            self.all_addresses.clone(),
            self.invalid_transaction_ratio,
        ))
    }
}
