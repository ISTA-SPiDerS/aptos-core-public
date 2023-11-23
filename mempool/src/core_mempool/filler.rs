use std::borrow::Borrow;
use std::cmp::max;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::{mem, thread};
use std::mem::size_of;
use dashmap::{DashMap, DashSet};
use rayon::iter::IntoParallelIterator;
use aptos_types::state_store::state_key::StateKey;
use aptos_types::transaction::{RAYON_EXEC_POOL, SignedTransaction, authenticator::TransactionAuthenticator};
use aptos_types::vm_status::VMStatus;
use aptos_vm_validator::vm_validator::{TransactionValidation, VMSpeculationResult};
use std::collections::HashMap;
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
use aptos_crypto::hash::TestOnlyHash;
use aptos_types::account_address::AccountAddress;
use crate::core_mempool::transaction_store::TransactionStore;
use crate::core_mempool::TxnPointer;

pub trait BlockFiller {
    fn add(&mut self, txn: SignedTransaction) -> bool;
    fn add_all(
        &mut self,
        previous: &mut Vec<SignedTransaction>,
    ) -> Vec<SignedTransaction>;

    fn get_block(self) -> Vec<SignedTransaction>;
    fn get_blockx(&mut self) -> Vec<SignedTransaction>;

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
        previous: &mut Vec<SignedTransaction>,
    ) -> Vec<SignedTransaction> {

        vec![]
    }

    fn get_block(self) -> Vec<SignedTransaction> {
        self.block
    }
    fn get_blockx(&mut self) -> Vec<SignedTransaction> {
        self.block.clone()
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

    max_bytes: u64,
    max_txns: u64,
    cores: u32,

    total_bytes: u64,
    total_estimated_gas: u64,
    full: bool,

    last_touched: BTreeMap<StateKey, u32>,
    writes: BTreeMap<StateKey, Vec<u16>>,
    dependency_graph: Vec<HashSet<u16>>,
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
            max_bytes,
            max_txns,
            cores,
            total_bytes: 0,
            total_estimated_gas: 0,
            full: false,
            last_touched: BTreeMap::new(),
            writes: BTreeMap::new(),
            dependency_graph: vec![],
            block: vec![],
            estimated_gas: vec![]
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
        result: &mut Vec<SignedTransaction>,
    ) -> Vec<SignedTransaction> {

        let start = Instant::now();

        if let LockResult::Ok(mut map) = SYNC_CACHE.lock() {
            let lock_time = start.elapsed().as_millis();

            let mut longest_chain = 0;
            for txinput in result
            {
                //let (speculation, status, tx) = previous.get(ind).unwrap();

                if self.full {
                    break;
                }

                let txn_len = txinput.raw_txn_bytes_len() as u64;
                if self.total_bytes + txn_len > self.max_bytes {
                    self.full = true;
                    break;
                }

                if let Some((write_set, read_set, gas , tx)) = map.remove(&(txinput.sender(), txinput.sequence_number())) {
                    let gas_used = (gas / 10) as u16;

                    println!("bla gas used {}", gas_used);

                    if write_set.is_empty()
                    {
                        println!("bla Empty????");
                        continue;
                    }

                    // When transaction can start assuming unlimited resources.
                    let mut arrival_time = 0;
                    for read in &read_set {
                        if let Some(val) = self.last_touched.get(read) {
                            arrival_time = max(arrival_time, *val);
                        }
                    }

                    // Check if there is room for the new block.
                    let finish_time = arrival_time as u32 + gas_used as u32;
                    if finish_time > 1000000
                    {
                        //println!("bla Wat a long chain: {}", finish_time);
                    }
                    if finish_time > longest_chain {
                        longest_chain = finish_time;
                    }

                    if finish_time > (self.gas_per_core * 2) as u32 {
                        //self.full = true;
                        //println!("bla skip {} {}", self.total_estimated_gas, finish_time);
                        map.insert((txinput.sender(), txinput.sequence_number()), (write_set, read_set, gas, tx));
                        continue;
                    }

                    if self.total_estimated_gas + gas_used as u64 > self.gas_per_core_init as u64 * self.cores as u64 {
                        self.full = true;
                        map.insert((txinput.sender(), txinput.sequence_number()), (write_set, read_set, gas, tx));
                        break;
                    }

                    let mut dependencies = HashSet::new();
                    for read in &read_set {
                        if let Some(val) = self.writes.get(read) {
                            for parent_txn in val {
                                dependencies.insert(*parent_txn);
                            }
                        }
                    }

                    let user_state_key = StateKey::raw(tx.sender().to_vec());
                    if self.writes.contains_key(&user_state_key) {
                        for parent_txn in self.writes.get(&user_state_key).unwrap() {
                            dependencies.insert(*parent_txn);
                        }
                    }

                    if self.total_bytes + txn_len + (dependencies.len() as u64) * (size_of::<TransactionIdx>() as u64) + (size_of::<u64>() as u64) > self.max_bytes {
                        self.full = true;
                        map.insert((txinput.sender(), txinput.sequence_number()), (write_set, read_set, gas, tx));
                        break;
                    }

                    self.total_bytes += txn_len + dependencies.len() as u64 * size_of::<TransactionIdx>() as u64 + size_of::<u64>() as u64;
                    self.total_estimated_gas += gas_used as u64;

                    let current_idx = self.block.len() as u16;

                    self.estimated_gas.push(gas_used);
                    // println!("len {}", dependencies.len());
                    self.dependency_graph.push(dependencies);

                    //self.transaction_validation.add_write_set(write_set);

                    // Update last touched time for used resources.
                    /*for (delta, _op) in delta_set {
                    let mx = max(finish_time, *self.last_touched.get(delta).unwrap_or(&0u32));
                    self.last_touched.insert(delta.clone(), mx.into());

                    if !self.writes.contains_key(delta) {
                        self.writes.insert(delta.clone(), vec![]);
                    }

                    if read_set.contains(&delta) {
                        self.writes.insert(delta.clone(), vec![]);
                    }
                    self.writes.get_mut(delta).unwrap().push(current_idx);
                    }*/

                    for write in write_set {
                        let mx = max(finish_time, *self.last_touched.get(&write.0).unwrap_or(&0u32));
                        self.last_touched.insert(write.0.clone(), mx.into());

                        if !self.writes.contains_key(&write.0) {
                            self.writes.insert(write.0.clone(), vec![]);
                        }

                        self.writes.insert(write.0.clone(), vec![current_idx]);
                    }

                    let mx = max(finish_time, *self.last_touched.get(&user_state_key).unwrap_or(&0u32));
                    self.last_touched.insert(user_state_key.clone(), mx.into());

                    self.writes.insert(user_state_key, vec![current_idx]);


                    self.block.push(tx);

                    if self.block.len() as u64 == self.max_txns {
                        self.full = true;
                        //println!("bla final gas6: {}", self.total_estimated_gas);
                    }
                } else {
                    return vec![];
                }
            }
            println!("bla endx {} {} {}", lock_time, start.elapsed().as_millis(), self.block.len());
        }


        //println!("bla final gas7: {} {}", self.total_estimated_gas, longestChain);
        return vec![];

    }

    fn get_block(self) -> Vec<SignedTransaction> {
        self.block
    }

    fn get_blockx(&mut self) -> Vec<SignedTransaction> {
        self.block.clone()
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
