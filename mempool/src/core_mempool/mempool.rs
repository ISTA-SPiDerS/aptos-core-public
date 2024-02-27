// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Mempool is used to track transactions which have been submitted but not yet
//! agreed upon.
use crate::{
    core_mempool::{
        index::TxnPointer,
        transaction::{MempoolTransaction, TimelineState},
        transaction_store::TransactionStore,
        filler::BlockFiller,
    },
    counters,
    counters::{CONSENSUS_PULLED_LABEL, E2E_LABEL, INSERT_LABEL, LOCAL_LABEL, REMOVE_LABEL},
    logging::{LogEntry, LogSchema, TxnsLog},
    shared_mempool::types::MultiBucketTimelineIndexIds,
};
use aptos_config::config::NodeConfig;
use aptos_crypto::HashValue;
use aptos_logger::prelude::*;
use aptos_types::{
    account_address::AccountAddress,
    account_config::AccountSequenceInfo,
    mempool_status::{MempoolStatus, MempoolStatusCode},
    transaction::{SignedTransaction, authenticator::TransactionAuthenticator},
};
use aptos_types::vm_status::VMStatus;
use aptos_vm_validator::vm_validator::{VMSpeculationResult};
use std::{
    collections::HashSet,
    time::{Duration, SystemTime},
};
use std::cmp::{max, min};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::future::pending;
use std::hash::Hash;
use std::num::Wrapping;
use std::sync::LockResult;
use std::sync::mpsc::SyncSender;
use std::time::Instant;
use anyhow;
use dashmap::{DashMap, DashSet};
use futures::pending;
use kanal::{Receiver, Sender};
use aptos_crypto::hash::TestOnlyHash;
use aptos_types::transaction::RAYON_EXEC_POOL;
use crate::core_mempool::DependencyFiller;
use crate::core_mempool::index::OrderedQueueKey;
use crate::shared_mempool::types::SYNC_CACHE;

pub struct Mempool {
    // Stores the metadata of all transactions in mempool (of all states).
    transactions: TransactionStore,

    pub system_transaction_timeout: Duration,
    last_max_gas: u64,
    cached_ex: VecDeque<(VMSpeculationResult, VMStatus, SignedTransaction)>,
    total: u64,
    current: u64
}

impl Mempool {
    pub fn new(config: &NodeConfig) -> Self {
        Mempool {
            transactions: TransactionStore::new(&config.mempool),
            system_transaction_timeout: Duration::from_secs(
                config.mempool.system_transaction_timeout_secs,
            ),
            last_max_gas: 2_000_000_000,
            cached_ex: VecDeque::new(),
            total: 0,
            current: 0,
        }
    }

    /// This function will be called once the transaction has been stored.
    pub(crate) fn commit_transaction(&mut self, sender: &AccountAddress, sequence_number: u64) {
        trace!(
            LogSchema::new(LogEntry::RemoveTxn).txns(TxnsLog::new_txn(*sender, sequence_number)),
            is_rejected = false
        );
        self.log_latency(*sender, sequence_number, counters::COMMIT_ACCEPTED_LABEL);
        if let Some(ranking_score) = self.transactions.get_ranking_score(sender, sequence_number) {
            counters::core_mempool_txn_ranking_score(
                REMOVE_LABEL,
                counters::COMMIT_ACCEPTED_LABEL,
                self.transactions.get_bucket(ranking_score),
                ranking_score,
            );
        }

        self.transactions
            .commit_transaction(sender, sequence_number);
    }

    pub(crate) fn reject_transaction(
        &mut self,
        sender: &AccountAddress,
        sequence_number: u64,
        hash: &HashValue,
    ) {
        trace!(
            LogSchema::new(LogEntry::RemoveTxn).txns(TxnsLog::new_txn(*sender, sequence_number)),
            is_rejected = true
        );
        self.log_latency(*sender, sequence_number, counters::COMMIT_REJECTED_LABEL);
        if let Some(ranking_score) = self.transactions.get_ranking_score(sender, sequence_number) {
            counters::core_mempool_txn_ranking_score(
                REMOVE_LABEL,
                counters::COMMIT_REJECTED_LABEL,
                self.transactions.get_bucket(ranking_score),
                ranking_score,
            );
        }

        self.transactions
            .reject_transaction(sender, sequence_number, hash);
    }

    fn log_latency(&self, account: AccountAddress, sequence_number: u64, stage: &'static str) {
        if let Some((&insertion_time, is_end_to_end, bucket)) = self
            .transactions
            .get_insertion_time_and_bucket(&account, sequence_number)
        {
            if let Ok(time_delta) = SystemTime::now().duration_since(insertion_time) {
                let scope = if is_end_to_end {
                    E2E_LABEL
                } else {
                    LOCAL_LABEL
                };
                counters::core_mempool_txn_commit_latency(stage, scope, bucket, time_delta);
            }
        }
    }

    pub(crate) fn get_by_hash(&self, hash: HashValue) -> Option<SignedTransaction> {
        self.transactions.get_by_hash(hash)
    }

    /// Used to add a transaction to the Mempool.
    /// Performs basic validation: checks account's sequence number.
    pub(crate) fn add_sharded_txn(
        &mut self,
        txn: SignedTransaction,
        ranking_score: u64,
        sequence_info: AccountSequenceInfo,
        timeline_state: TimelineState,
        peer_count: u8,
        peer_id: u8,
        sender: &std::sync::mpsc::Sender<SignedTransaction>
    ) -> MempoolStatus {
        let db_sequence_number = sequence_info.min_seq();
        trace!(
            LogSchema::new(LogEntry::AddTxn)
                .txns(TxnsLog::new_txn(txn.sender(), txn.sequence_number())),
            committed_seq_number = db_sequence_number
        );

        // don't accept old transactions (e.g. seq is less than account's current seq_number)
        if txn.sequence_number() < db_sequence_number {
            return MempoolStatus::new(MempoolStatusCode::InvalidSeqNumber).with_message(format!(
                "transaction sequence number is {}, current sequence number is  {}",
                txn.sequence_number(),
                db_sequence_number,
            ));
        }

        let shards = ((peer_count as f32/ 3.0).floor() as u32 + 1);

        let dif:u32 = (256.0 / peer_count as f32).ceil() as u32;
        let mut my_space_start= 0 as u32;
        let mut my_space_end = u8::MAX as u32;

        if peer_count > 1
        {
            my_space_start = peer_id as u32 * dif;
            my_space_end = my_space_start + dif * shards;
        }

        let mut shard = Wrapping(0 as u8);
        for el in txn.sender().iter() {
            shard = shard + Wrapping(*el);
        }

        let val = shard.0 as u32;
        if my_space_end > u8::MAX as u32 {

            let dif_end = my_space_end - u8::MAX as u32;
            if val < my_space_start && val >= dif_end {
                //println!("bla shard deny {} {} {} {} {}", shard, my_space_start, my_space_end, peer_id, peer_count);
                //return MempoolStatus::new(MempoolStatusCode::Accepted);
                return MempoolStatus::new(MempoolStatusCode::UnknownStatus).with_message(
                    "sharded out this tx".to_string());
            }
        }
        else if val < my_space_start || val >= my_space_end {
            //println!("bla shard deny {} {} {} {} {}", shard, my_space_start, my_space_end, peer_id, peer_count);
            //return MempoolStatus::new(MempoolStatusCode::Accepted);
            return MempoolStatus::new(MempoolStatusCode::UnknownStatus).with_message(
                "sharded out this tx".to_string());
        }

        sender.send(txn.clone()).unwrap();

        let now = SystemTime::now();
        let expiration_time =
            aptos_infallible::duration_since_epoch_at(&now) + self.system_transaction_timeout;

        let txn_info = MempoolTransaction::new(
            txn,
            expiration_time,
            ranking_score,
            timeline_state,
            AccountSequenceInfo::Sequential(db_sequence_number),
            now,
        );

        let status = self.transactions.insert(txn_info);
        counters::core_mempool_txn_ranking_score(
            INSERT_LABEL,
            status.code.to_string().as_str(),
            self.transactions.get_bucket(ranking_score),
            ranking_score,
        );
        status
    }

    /// Used to add a transaction to the Mempool.
    /// Performs basic validation: checks account's sequence number.
    pub(crate) fn add_txn(
        &mut self,
        txn: SignedTransaction,
        ranking_score: u64,
        sequence_info: AccountSequenceInfo,
        timeline_state: TimelineState
    ) -> MempoolStatus {
       let (tx_sender, tx_receiver): (std::sync::mpsc::Sender<SignedTransaction>, std::sync::mpsc::Receiver<SignedTransaction>) = std::sync::mpsc::channel();
       self.add_sharded_txn(txn, ranking_score, sequence_info, timeline_state, 1, 0, &tx_sender)
    }

    /// Fetches next block of transactions for consensus.
    /// `batch_size` - size of requested block.
    /// `seen_txns` - transactions that were sent to Consensus but were not committed yet,
    ///  mempool should filter out such transactions.
    #[allow(clippy::explicit_counter_loop)]
    //pub(crate) fn get_batch<F: BlockFiller>(
    //    &mut self,
    //    mut seen: HashSet<TxnPointer>,
    //    block_filler: &mut F
    //) -> &mut F {
    //    block_filler
    //}

    #[allow(clippy::explicit_counter_loop)]
    pub(crate) fn get_batch<'a, F: BlockFiller>(
       &'a mut self,
        mut seen: HashSet<TxnPointer>,
        block_filler: &'a mut F ) -> &mut F {
        block_filler
    }

    /// Fetches next block of transactions for consensus.
    /// `batch_size` - size of requested block.
    /// `seen_txns` - transactions that were sent to Consensus but were not committed yet,
    ///  mempool should filter out such transactions.
    #[allow(clippy::explicit_counter_loop)]
    pub(crate) fn get_full_batch(
        &mut self,
        mut seen: HashSet<TxnPointer>,
        receiver: &kanal::Receiver<DependencyFiller>
    ) -> DependencyFiller {

        let mut time = Instant::now();
        let mut result = BTreeMap::new();
        // Helper DS. Helps to mitigate scenarios where account submits several transactions
        // with increasing gas price (e.g. user submits transactions with sequence number 1, 2
        // and gas_price 1, 10 respectively)
        // Later txn has higher gas price and will be observed first in priority index iterator,
        // but can't be executed before first txn. Once observed, such txn will be saved in
        // `skipped` DS and rechecked once it's ancestor becomes available
        let mut skipped = HashSet::new();
        let mut total_bytes = 0;
        let seen_size = seen.len();

        let mut txn_walked = 0usize;
        let mut index = 0;

        let mut block_filler: DependencyFiller = DependencyFiller::new(
            2_000_000_000,
            1_000_000,
            10000, RAYON_EXEC_POOL.current_num_threads() as u32);

        // iterate over the queue of transactions based on gas price
        'main: for txn in self.transactions.iter_queue() {
            txn_walked += 1;
            if seen.contains(&TxnPointer::from(txn)) {
                continue;
            }

            let tx_seq = txn.sequence_number.transaction_sequence_number;
            let account_sequence_number = self.transactions.get_sequence_number(&txn.address);
            let seen_previous = tx_seq > 0 && (seen.contains(&(txn.address, tx_seq - 1)));
            // include transaction if it's "next" for given account or
            // we've already sent its ancestor to Consensus.
            if seen_previous || account_sequence_number == Some(&tx_seq) {
                let ptr = TxnPointer::from(txn);

                if index as u64 >= block_filler.get_max_txn() {
                   break;
                }

                let full_tx = self.transactions.get(&ptr.0, ptr.1).unwrap();
                if total_bytes + full_tx.raw_txn_bytes_len() as u64 > block_filler.get_max_bytes() {
                    break;
                }
                seen.insert(ptr);

                total_bytes += full_tx.raw_txn_bytes_len() as u64;

                result.insert(index, full_tx);
                index += 1;

                // check if we can now include some transactions
                // that were skipped before for given account
                let mut skipped_txn = (txn.address, tx_seq + 1);
                while skipped.contains(&skipped_txn) {
                    if result.len() as u64 >= block_filler.get_max_txn() {
                        break 'main;
                    }
                    seen.insert(skipped_txn);
                    result.insert(index, self.transactions.get(&skipped_txn.0, skipped_txn.1).unwrap());
                    index += 1;


                    skipped_txn = (txn.address, skipped_txn.1 + 1);
                }
            } else {
                skipped.insert(TxnPointer::from(txn));
            }
        }

        if !result.is_empty() {
            let elapsed1 = time.elapsed().as_millis();
            let result_size = index;
            let mut map = BTreeMap::new();
            block_filler.set_gas_per_core(self.last_max_gas);
            block_filler.add_all(&mut map, true);

            let len =  block_filler.get_blockx().len();
            if (len > 0) {
                println!("bla blocklen: {}", len);
            }


            if len >= 500 {
                let dif = max(block_filler.get_max_txn() as usize / len, 1);
                self.last_max_gas = block_filler.get_current_gas() * dif as u64;
            }

            debug!(
            LogSchema::new(LogEntry::GetBlock),
            seen_consensus = seen_size,
            walked = txn_walked,
            seen_after = seen.len(),
            result_size = result_size,
            block_size = len,
            byte_size = total_bytes,);

            counters::mempool_service_transactions(counters::GET_BLOCK_LABEL, len);
            counters::MEMPOOL_SERVICE_BYTES_GET_BLOCK.observe(total_bytes as f64);

            let elapsed = time.elapsed().as_millis();
            if elapsed > 0 {
                println!("bla total: {} {} {} {}", elapsed1, elapsed, len, result_size);
            }
        }
        else {
            debug!(
            LogSchema::new(LogEntry::GetBlock),
            seen_consensus = seen_size,
            walked = txn_walked,
            seen_after = seen.len(),
            result_size = 0,
            block_size = 0,
            byte_size = 0);
        }

        block_filler
    }

    /// Periodic core mempool garbage collection.
    /// Removes all expired transactions and clears expired entries in metrics
    /// cache and sequence number cache.
    pub(crate) fn gc(&mut self) {
        let now = aptos_infallible::duration_since_epoch();
        self.transactions.gc_by_system_ttl(now);
    }

    /// Garbage collection based on client-specified expiration time.
    pub(crate) fn gc_by_expiration_time(&mut self, block_time: Duration) {
        self.transactions.gc_by_expiration_time(block_time);
    }

    /// Returns block of transactions and new last_timeline_id.
    pub(crate) fn read_timeline(
        &self,
        timeline_id: &MultiBucketTimelineIndexIds,
        count: usize,
    ) -> (Vec<SignedTransaction>, MultiBucketTimelineIndexIds) {
        self.transactions.read_timeline(timeline_id, count)
    }

    /// Read transactions from timeline from `start_id` (exclusive) to `end_id` (inclusive).
    pub(crate) fn timeline_range(
        &self,
        start_end_pairs: &Vec<(u64, u64)>,
    ) -> Vec<SignedTransaction> {
        self.transactions.timeline_range(start_end_pairs)
    }

    pub fn gen_snapshot(&self) -> TxnsLog {
        self.transactions.gen_snapshot()
    }

    #[cfg(test)]
    pub fn get_parking_lot_size(&self) -> usize {
        self.transactions.get_parking_lot_size()
    }

    #[cfg(test)]
    pub fn get_transaction_store(&self) -> &TransactionStore {
        &self.transactions
    }
}
