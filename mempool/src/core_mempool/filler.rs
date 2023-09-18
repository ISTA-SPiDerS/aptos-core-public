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
use std::sync::mpsc::SyncSender;
use std::time::{Duration, Instant};
use rayon::iter::IndexedParallelIterator;
use rayon::iter::ParallelIterator;
use anyhow::{anyhow, Result, Error};
use itertools::Itertools;
use crate::shared_mempool::types::{CACHE};
use once_cell::sync::{Lazy, OnceCell};
use aptos_crypto::hash::TestOnlyHash;
use aptos_types::account_address::AccountAddress;
use crate::core_mempool::transaction_store::TransactionStore;
use crate::core_mempool::TxnPointer;

pub trait BlockFiller {
    fn add(&mut self, txn: SignedTransaction) -> bool;
    fn add_all(
        &mut self,
        txn: VecDeque<SignedTransaction>,
        previous: &mut VecDeque<(VMSpeculationResult, VMStatus, SignedTransaction)>,
        total: &mut u64,
        current: &mut u64,
        pending: &mut HashSet<TxnPointer>
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
        txn: VecDeque<SignedTransaction>,
        previous: &mut VecDeque<(VMSpeculationResult, VMStatus, SignedTransaction)>,
        mut total: &mut u64,
        mut current: &mut u64,
        pending: &mut HashSet<TxnPointer>
    ) -> Vec<SignedTransaction> {
        for tx in txn
        {
            /*if self.full {
                rejected.push(tx);
                continue;
            }

            if self.current_bytes + tx.raw_txn_bytes_len() as u64 > self.max_bytes {
                self.full = true;
                rejected.push(tx);
                continue;
            }*/

            self.block.push(tx);

            if self.block.len() as u64 == self.max_txns {
                self.full = true;
            }
        }
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
    cores: u64,

    total_bytes: u64,
    total_estimated_gas: u64,
    full: bool,

    last_touched: HashMap<Vec<u8>, u64>,
    writes: HashMap<Vec<u8>, Vec<TransactionIdx>>,
    dependency_graph: Vec<HashSet<TransactionIdx>>,
    block: Vec<SignedTransaction>,
    estimated_gas: Vec<u64>,
    pub sender: SyncSender<(u64, SignedTransaction)>,
}

impl DependencyFiller {
    pub fn new(
        gas_per_core: u64,
        max_bytes: u64,
        max_txns: u64,
        cores: u64,
        sender: SyncSender<(u64, SignedTransaction)>)
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
            last_touched: HashMap::with_capacity(10000),
            writes: HashMap::with_capacity(10000),
            dependency_graph: vec![],
            block: vec![],
            estimated_gas: vec![],
            sender
        }
    }

    pub fn get_gas_estimates(&mut self) -> Vec<u64> {
        mem::take(&mut self.estimated_gas)
    }

    pub fn get_dependency_graph(&self) -> Vec<Vec<TransactionIdx>> {
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
        mut txn: VecDeque<SignedTransaction>,
        previous: &mut VecDeque<(VMSpeculationResult, VMStatus, SignedTransaction)>,
        total: &mut u64,
        current: &mut u64,
        pending: &mut HashSet<TxnPointer>
    ) -> Vec<SignedTransaction> {

        let mut force = false;
        if *total == u64::MAX {
            force = true;
            *total = 0
        }

        {
            while let Some(tx) = txn.pop_front() {
                pending.insert((tx.sender(), tx.sequence_number()));
                self.sender.send((total.clone(), tx));
                *total += 1;
            }

            if force {
                while *current < *total
                {
                    while CACHE.contains_key(current)
                    {
                        let out = CACHE.remove(current).unwrap().1;
                        previous.push_back(out);

                        *current += 1;
                    }
                    if *current < *total
                    {
                        thread::sleep(Duration::from_millis(10));
                        //println!("waiting.... {} {}", *current, *total);
                    }
                }
            } else {
                while CACHE.contains_key(current)
                {
                    let out = CACHE.remove(current).unwrap().1;
                    previous.push_back(out);

                    *current += 1;
                }
            }
            //println!("bla cache len other side {}", CACHE.len());

        }

        //println!("bla prev len: {}", previous.len());
        let mut cache : VecDeque<(VMSpeculationResult, VMStatus, SignedTransaction)> = VecDeque::new();

        let mut longestChain = 0;
        while let Some((speculation, status, tx)) = previous.pop_front()
        {
            //let (speculation, status, tx) = previous.get(ind).unwrap();

            if self.full {
                cache.push_back((speculation, status, tx));
                break;
            }

            let txn_len = tx.raw_txn_bytes_len() as u64;
            if self.total_bytes + txn_len > self.max_bytes {
                self.full = true;
                cache.push_back((speculation, status, tx));
                break;
            }

            let mut read_set = &speculation.input;
            let write_set = speculation.output.txn_output().write_set();
            let delta_set = speculation.output.delta_change_set();
            let gas_used = speculation.output.txn_output().gas_used();
            if gas_used > 100000
            {
                //println!("bla Wat a big tx: {}", gas_used);
            }

            if write_set.is_empty()
            {
                println!("Empty????");
                cache.push_back((speculation, status, tx));
                continue;
            }

            // When transaction can start assuming unlimited resources.
            let mut arrival_time = 0;
            for read in read_set {
                if let Some(val) = self.last_touched.get(read.get_val()) {
                    arrival_time = max(arrival_time, *val);
                }
            }

            // Check if there is room for the new block.
            let finish_time = arrival_time + gas_used;
            if finish_time > 1000000
            {
                //println!("bla Wat a long chain: {}", finish_time);
            }
            if finish_time > longestChain {
                longestChain = finish_time;
            }

            if finish_time > (self.gas_per_core * 2) as u64 {
                //self.full = true;
                //println!("bla skip {} {}", self.total_estimated_gas, finish_time);
                cache.push_back((speculation, status, tx));
                continue;
            }

            if self.total_estimated_gas + gas_used > self.gas_per_core_init * self.cores {
                self.full = true;
                cache.push_back((speculation, status, tx));
                break;
            }

            let mut dependencies = HashSet::new();
            for read in read_set {
                if let Some(val) = self.writes.get(read.get_val()) {
                    for parent_txn in val {
                        dependencies.insert(*parent_txn);
                    }
                }
            }

            let user_state_key = tx.sender().to_vec();
            if let Some(val) = self.writes.get(&user_state_key) {
                for parent_txn in val {
                    dependencies.insert(*parent_txn);
                }
            }

            if self.total_bytes + txn_len + (dependencies.len() as u64) * (size_of::<TransactionIdx>() as u64) + (size_of::<u64>() as u64) > self.max_bytes {
                self.full = true;
                cache.push_back((speculation, status, tx));
                break;
            }

            self.total_bytes += txn_len + dependencies.len() as u64 * size_of::<TransactionIdx>() as u64 + size_of::<u64>() as u64;
            self.total_estimated_gas += gas_used;

            let current_idx = self.block.len() as TransactionIdx;

            self.estimated_gas.push(gas_used);
            // println!("len {}", dependencies.len());
            self.dependency_graph.push(dependencies);

            //self.transaction_validation.add_write_set(write_set);

            // Update last touched time for used resources.
            for (delta, _op) in delta_set {
                let mx = max(finish_time, *self.last_touched.get(delta.get_val()).unwrap_or(&0u64));
                self.last_touched.insert(delta.get_val().clone(), mx.into());

                if !self.writes.contains_key(delta.get_val()) {
                    self.writes.insert(delta.get_val().clone(), vec![]);
                }

                if read_set.contains(delta) {
                    self.writes.insert(delta.get_val().clone(), vec![]);
                }
                self.writes.get_mut(delta.get_val()).unwrap().push(current_idx);
            }
            
            for write in write_set {
                let mx = max(finish_time, *self.last_touched.get(write.0.get_val()).unwrap_or(&0u64));
                self.last_touched.insert(write.0.get_val().clone(), mx.into());

                if !self.writes.contains_key(write.0.get_val()) {
                    self.writes.insert(write.0.get_val().clone(), vec![]);
                }

                self.writes.insert(write.0.get_val().clone(), vec![current_idx]);
            }

            let mx = max(finish_time, *self.last_touched.get(&user_state_key).unwrap_or(&0u64));
            self.last_touched.insert(user_state_key.clone(), mx.into());

            self.writes.insert(user_state_key.clone(), vec![current_idx]);


            self.block.push(tx);

            if self.block.len() as u64 == self.max_txns {
                self.full = true;
                //println!("bla final gas6: {}", self.total_estimated_gas);
            }
        }

        for tx in &self.block {
            pending.remove(&(tx.sender(), tx.sequence_number()));
        }

        if !cache.is_empty() {
            println!("bla clear cache {}", cache.len());
            previous.extend(cache);
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
