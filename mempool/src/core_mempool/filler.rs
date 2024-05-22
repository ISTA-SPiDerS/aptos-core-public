use std::collections::{BTreeMap};

use std::borrow::Borrow;
use std::cmp::max;
use std::collections::{BTreeSet, HashSet, VecDeque};
use std::{mem, thread};
use std::mem::size_of;
use dashmap::{DashMap, DashSet};
use rayon::iter::IntoParallelIterator;
use aptos_types::state_store::state_key::StateKey;
use aptos_types::transaction::{RAYON_EXEC_POOL, SignedTransaction, authenticator::TransactionAuthenticator, TransactionOutput};
use aptos_types::vm_status::VMStatus;
use aptos_vm_validator::vm_validator::{TransactionValidation, VMSpeculationResult};
use std::sync::atomic::Ordering;
use std::sync::LockResult;
use std::sync::mpsc::SyncSender;
use std::time::{Duration, Instant};
use rayon::iter::IndexedParallelIterator;
use rayon::iter::ParallelIterator;
use anyhow::{anyhow, Result, Error};
use itertools::Itertools;
use crate::shared_mempool::types::{SYNC_CACHE};
use once_cell::sync::{Lazy, OnceCell};
use rustc_hash::{FxHashMap, FxHashSet};
use aptos_crypto::hash::TestOnlyHash;
use aptos_types::account_address::AccountAddress;
use aptos_types::write_set::WriteSet;
use crate::core_mempool::transaction_store::TransactionStore;
use crate::core_mempool::TxnPointer;
use aptos_aggregator::transaction::TransactionOutputExt;
use aptos_types::test_helpers::transaction_test_helpers::block;

pub trait BlockFiller {
    fn add(&mut self, txn: SignedTransaction) -> bool;
    fn add_all(
        &mut self,
        previous: &mut Vec<(WriteSet, BTreeSet<StateKey>, u32, SignedTransaction)>,
        last_touched: &mut FxHashMap<Vec<u8>, (u32, u16)>,
        skipped_users: &mut FxHashSet<Vec<u8>>,
        good_block: bool
    ) -> (Vec<(WriteSet, BTreeSet<StateKey>, u32, SignedTransaction)>, bool);

    fn get_block(self) -> Vec<SignedTransaction>;
    fn get_blockx(&mut self) -> &Vec<SignedTransaction>;

    fn is_full(&self) -> bool;

    fn get_max_bytes(&self) -> u64;

    fn get_max_txn(&self) -> u64;

    fn get_current_gas(&self) -> u64;

    fn set_gas_per_core(&mut self, last_max: u64);
}

pub struct SimpleFiller {
    max_bytes: u64,
    max_txns: u64,

    current_bytes: u64,
    full: bool,
    block: Vec<SignedTransaction>,
}

impl SimpleFiller {
    pub fn new(max_bytes: u64, max_txns: u64) -> Self {
        Self {
            max_bytes,
            max_txns,

            current_bytes: 0,
            full: false,
            block: vec![]
        }
    }
}

impl BlockFiller for SimpleFiller {
    fn add(&mut self, txn: SignedTransaction) -> bool {
        if self.full {
            return false;
        }

        if self.current_bytes + txn.raw_txn_bytes_len() as u64 > self.max_bytes {
            self.full = true;
            return false;
        }

        self.block.push(txn);

        if self.block.len() as u64 == self.max_txns {
            self.full = true;
        }

        true
    }

    fn add_all(
        &mut self,
        previous: &mut Vec<(WriteSet, BTreeSet<StateKey>, u32, SignedTransaction)>,
        last_touched: &mut FxHashMap<Vec<u8>, (u32, u16)>,
        skipped_users: &mut FxHashSet<Vec<u8>>,
        good_block: bool
    ) -> (Vec<(WriteSet, BTreeSet<StateKey>, u32, SignedTransaction)>, bool) {

        (Vec::new(), true)
    }

    fn get_block(self) -> Vec<SignedTransaction> {
        self.block
    }
    fn get_blockx(&mut self) -> &Vec<SignedTransaction> {
        &self.block
    }

    fn is_full(&self) -> bool {
        self.full
    }

    fn get_max_bytes(&self) -> u64 {
        self.max_bytes
    }

    fn get_max_txn(&self) -> u64 {
        self.max_txns
    }

    fn get_current_gas(&self) -> u64 {
        0
    }

    fn set_gas_per_core(&mut self, last_max: u64) {
        // Do nothing
    }
}

type TransactionIdx = u64;

pub struct DependencyFiller {
    gas_per_core: u64,
    gas_per_core_init: u64,
    total_max_gas: u64,
    max_bytes: u64,
    max_txns: u64,
    cores: u32,

    total_bytes: u64,
    total_estimated_gas: u64,
    full: bool,

    dependency_graph: Vec<FxHashSet<u16>>,
    block: Vec<SignedTransaction>,
    estimated_gas: Vec<u16>,
}

impl DependencyFiller {
    pub fn new(
        gas_per_core: u64,
        max_bytes: u64,
        max_txns: u64,
        cores: u32)
        -> DependencyFiller {
        Self {
            gas_per_core,
            gas_per_core_init: gas_per_core,
            total_max_gas: gas_per_core * cores as u64,
            max_bytes,
            max_txns,
            cores,
            total_bytes: 0,
            total_estimated_gas: 0,
            full: false,
            dependency_graph: Vec::with_capacity(max_txns as usize),
            block: Vec::with_capacity(max_txns as usize),
            estimated_gas: Vec::with_capacity(max_txns as usize)
        }
    }

    pub fn get_gas_estimates(&mut self) -> Vec<u16> {
        mem::take(&mut self.estimated_gas)
    }

    pub fn get_dependency_graph(&self) -> Vec<Vec<u16>> {
        let mut result = vec![];
        for parents in &self.dependency_graph {
            let mut parents_vec = vec![];
            for parent in parents {
                parents_vec.push(*parent);
            }
            result.push(parents_vec);
        }

        result
    }
}

impl BlockFiller for DependencyFiller {
    fn add(&mut self, txn: SignedTransaction) -> bool {
        if self.full {
            return false;
        }

        let txn_len = txn.raw_txn_bytes_len() as u64;
        if self.total_bytes + txn_len > self.max_bytes {
            self.full = true;
            return false;
        }

        return false;
    }

    fn add_all(
        &mut self,
        result: &mut Vec<(WriteSet, BTreeSet<StateKey>, u32, SignedTransaction)>,
        last_touched: &mut FxHashMap<Vec<u8>, (u32, u16)>,
        skipped_users: &mut FxHashSet<Vec<u8>>,
        good_block: bool
    ) -> (Vec<(WriteSet, BTreeSet<StateKey>, u32, SignedTransaction)>, bool) {

        let mut skipped = 0;
        let mut non_user_skip = 0;
        let input_len = result.len();
        let mut len = self.block.len() as u64;
        //let hot_read_percentage = 10;

        //println!("Got x transactions: {}", result.len());

        let mut return_vec  = Vec::with_capacity(result.len()/2);
        for (writeset, read_set, gas, tx) in result.drain(0..result.len()) {
            //let (speculation, status, tx) = previous.get(ind).unwrap();
            if self.full {
                return_vec.push((writeset, read_set, gas, tx));
                continue;
            }

            let gas_used = (gas / 10) as u16;
            let raw_user_state_key = tx.sender().to_vec();
            if good_block && skipped_users.contains(&raw_user_state_key)
            {
                skipped += 1;
                return_vec.push((writeset, read_set, gas, tx));
                continue;
            }

            // When transaction can start assuming unlimited resources.
            let mut arrival_time = 0;
            let mut dependencies = FxHashSet::default();
            let limit_hot_reads = good_block && len >= 1000 && len <= 9000;

            let mut bail = false;
            let mut hot_read_access = 0;
            let hot_read_limit = (self.total_estimated_gas as u32) / self.cores;
            for read in read_set.iter() {
                if let Some((time, key)) = last_touched.get(read.get_raw_ref()) {
                    arrival_time = max(arrival_time, *time);

                    if limit_hot_reads && *time > hot_read_limit {
                        hot_read_access += 1;
                    }
                    dependencies.insert(*key);
                }
                if hot_read_access >= 4 {
                    bail = true;
                    break;
                }
            }

            if bail {
                skipped_users.insert(raw_user_state_key);
                return_vec.push((writeset, read_set, gas, tx));
                skipped += 1;
                non_user_skip += 1;
                continue;
            }

            if let Some((time, key)) = last_touched.get(&raw_user_state_key) {
                arrival_time = max(arrival_time, *time);
                dependencies.insert(*key);
            }

            // Check if there is room for the new block.
            let finish_time = arrival_time + gas_used as u32;
            if good_block && finish_time > self.gas_per_core as u32 {
                //self.full = true;
                //println!("bla skip {} {}", self.total_estimated_gas, finish_time);
                skipped += 1;
                non_user_skip += 1;
                skipped_users.insert(raw_user_state_key);
                return_vec.push((writeset, read_set, gas, tx));
                continue;
            }

            if self.total_estimated_gas + gas_used as u64 > (self.total_max_gas) as u64 {
                self.full = true;
                return_vec.push((writeset, read_set, gas, tx));
                continue;
            }

            self.total_estimated_gas += gas_used as u64;

            let current_idx = self.block.len() as u16;
            self.estimated_gas.push(gas_used);
            // println!("len {}", dependencies.len());
            self.dependency_graph.push(dependencies);

            self.block.push(tx);

            for write in writeset.iter() {
                let raw = write.0.get_raw_ref();
                let curr_max = last_touched.get(raw).unwrap_or(&(0u32, 0)).0;
                if finish_time > curr_max {
                    last_touched.insert(raw.to_vec(), (finish_time, current_idx));
                }
            }

            let curr_max = last_touched.get(&raw_user_state_key).unwrap_or(&(0u32, 0)).0;
            if finish_time > curr_max {
                last_touched.insert(raw_user_state_key, (finish_time, current_idx));
            }

            len += 1;

            if len >= self.max_txns {
               self.full = true;
            }
        }

        //todo we might try to make this the normal skipped as well/
        println!("skipped: {} {}", skipped, len);
        return (return_vec, non_user_skip + 25 >= input_len);
    }

    fn get_block(self) -> Vec<SignedTransaction> {
        self.block
    }

    fn get_blockx(&mut self) -> &Vec<SignedTransaction> {
        &self.block
    }

    fn is_full(&self) -> bool {
        self.full
    }

    fn get_max_bytes(&self) -> u64 {
        self.max_bytes
    }

    fn get_max_txn(&self) -> u64 {
        self.max_txns
    }

    fn get_current_gas(&self) -> u64 {
        self.total_estimated_gas
    }

    fn set_gas_per_core(&mut self, last_max: u64) {
        self.gas_per_core = last_max / self.cores as u64;
    }
}
