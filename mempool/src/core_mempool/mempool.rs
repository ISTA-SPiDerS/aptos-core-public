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
use std::collections::VecDeque;
use std::num::Wrapping;
use dashmap::DashMap;

pub struct Mempool {
    // Stores the metadata of all transactions in mempool (of all states).
    transactions: TransactionStore,

    pub system_transaction_timeout: Duration,
}

impl Mempool {
    pub fn new(config: &NodeConfig) -> Self {
        Mempool {
            transactions: TransactionStore::new(&config.mempool),
            system_transaction_timeout: Duration::from_secs(
                config.mempool.system_transaction_timeout_secs,
            ),
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
        peer_id: u8
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

        let dif:u32 = (256.0 / peer_count as f32).ceil() as u32;
        let mut my_space_start= 0 as u32;
        let mut my_space_end = u8::MAX as u32;

        if peer_count > 1
        {
            my_space_start = peer_id as u32 * dif;
            my_space_end = my_space_start + dif;
        }

        let mut shard = Wrapping(1 as u8);
        for el in txn.sender().iter() {
            shard = shard + Wrapping(*el);
        }

        if (shard.0 as u32) < my_space_start || (shard.0 as u32) >= my_space_end {
            //println!("bla shard deny {} {} {} {} {}", shard, my_space_start, my_space_end, peer_id, peer_count);
            //return MempoolStatus::new(MempoolStatusCode::Accepted);
            return MempoolStatus::new(MempoolStatusCode::UnknownStatus).with_message(
                "sharded out this tx".to_string());
        }

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
        self.add_sharded_txn(txn, ranking_score, sequence_info, timeline_state, 1, 0)
    }

    /// Fetches next block of transactions for consensus.
    /// `batch_size` - size of requested block.
    /// `seen_txns` - transactions that were sent to Consensus but were not committed yet,
    ///  mempool should filter out such transactions.
    #[allow(clippy::explicit_counter_loop)]
    pub(crate) fn get_batch<F: BlockFiller>( &mut self,
                                             mut seen: HashSet<TxnPointer>,
                                             block_filler: &mut F)
    {
        self.get_full_batch(seen, block_filler, 1, 0)
    }

    /// Fetches next block of transactions for consensus.
    /// `batch_size` - size of requested block.
    /// `seen_txns` - transactions that were sent to Consensus but were not committed yet,
    ///  mempool should filter out such transactions.
    #[allow(clippy::explicit_counter_loop)]
    pub(crate) fn get_full_batch<F: BlockFiller>(
        &mut self,
        mut seen: HashSet<TxnPointer>,
        block_filler: &mut F,
        peer_count: u8,
        peer_id: u8
    ) {
        let mut result = VecDeque::new();
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

        let dif:u32 = 256 as u32 / peer_count as u32;
        let mut my_space_start= 0 as u32;
        let mut my_space_end = u8::MAX as u32;

        //println!("bla peers: {} {}", peer_id, peer_count);
        if peer_count > 1
        {
            my_space_start = peer_id as u32 * dif;
            my_space_end = my_space_start + dif;
        }

        // iterate over the queue of transactions based on gas price
        'main: for txn in self.transactions.iter_queue() {
            txn_walked += 1;
            if seen.contains(&TxnPointer::from(txn)) {
                continue;
            }

            let shard = txn.address[txn.address.len()-1] as u32;
            if shard < my_space_start || shard >= my_space_end {
                //println!("bla sharded: {} {} {} {}", txn.address, my_space_start, my_space_end, shard);
                //continue
            }

            let tx_seq = txn.sequence_number.transaction_sequence_number;
            let account_sequence_number = self.transactions.get_sequence_number(&txn.address);
            let seen_previous = tx_seq > 0 && seen.contains(&(txn.address, tx_seq - 1));
            // include transaction if it's "next" for given account or
            // we've already sent its ancestor to Consensus.
            if seen_previous || account_sequence_number == Some(&tx_seq) {
                let ptr = TxnPointer::from(txn);

                if (result.len() as u64) >= block_filler.get_max_txn() {
                    break;
                }

                let full_tx = self.transactions.get(&ptr.0, ptr.1).unwrap();
                if total_bytes + full_tx.raw_txn_bytes_len() as u64 > block_filler.get_max_bytes() {
                    break;
                }
                seen.insert(ptr);
                total_bytes+= full_tx.raw_txn_bytes_len() as u64;
                result.push_back(full_tx);


                // check if we can now include some transactions
                // that were skipped before for given account
                let mut skipped_txn = (txn.address, tx_seq + 1);
                while skipped.contains(&skipped_txn) {
                    if (result.len() as u64) >= block_filler.get_max_txn() {
                        break 'main;
                    }
                    seen.insert(skipped_txn);
                    result.push_back(self.transactions.get(&skipped_txn.0, skipped_txn.1).unwrap());
                    skipped_txn = (txn.address, skipped_txn.1 + 1);
                }
            } else {
                skipped.insert(TxnPointer::from(txn));
            }
        }

        let result_size = result.len();
        if result_size > 0
        {
            //println!("bla result: {}", result_size);
            //println!("bla seen: {}", seen.len());

            let off = block_filler.add_all(result, &DashMap::new());
            //println!("bla unsee: {}", off.len());

            for tx in off
            {
                seen.remove(&(tx.sender(), tx.sequence_number()));
            }

            //println!("bla blocklen: {}", block_filler.get_blockx().len());
            //println!("bla seen now: {}", seen.len());

            debug!(
            LogSchema::new(LogEntry::GetBlock),
            seen_consensus = seen_size,
            walked = txn_walked,
            seen_after = seen.len(),
            result_size = result_size,
            block_size = block_filler.get_blockx().len(),
            byte_size = total_bytes,
        );

            println!("{}", block_filler.get_blockx().len());

            counters::mempool_service_transactions(counters::GET_BLOCK_LABEL, block_filler.get_blockx().len());
            counters::MEMPOOL_SERVICE_BYTES_GET_BLOCK.observe(total_bytes as f64);
        }
        else {
            debug!(
            LogSchema::new(LogEntry::GetBlock),
            seen_consensus = seen_size,
            walked = txn_walked,
            seen_after = seen.len(),
            result_size = 0,
            block_size = 0,
            byte_size = 0,
        );
        }
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
