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
    total_max_gas: u64,
    max_bytes: u64,
    max_txns: u64,
    cores: u32,

    total_bytes: u64,
    total_estimated_gas: u64,
    full: bool,

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
            total_max_gas: gas_per_core * cores as u64,
            max_bytes,
            max_txns,
            cores,
            total_bytes: 0,
            total_estimated_gas: 0,
            full: false,
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

        let mut skipped = 0;
        let mut len = 0;

        let mut last_touched: HashMap<StateKey, (u32, u16)> = HashMap::new();
        if let Ok(mut map) = SYNC_CACHE.lock() {

            let mut longest_chain = 0;
            for txinput in result
            {
                //let (speculation, status, tx) = previous.get(ind).unwrap();
                if self.full {
                    break;
                }

                if let Some((write_set, read_set, gas , tx)) = map.get(&(txinput.sender(), txinput.sequence_number())) {
                    let gas_used = (gas / 10) as u16;
                    if write_set.is_empty()
                    {
                        println!("bla Empty????");
                        continue;
                    }

                    // When transaction can start assuming unlimited resources.
                    let mut arrival_time = 0;
                    let mut dependencies = HashSet::new();

                    let mut hot_read_access = 0;
                    for read in read_set {
                        if let Some((time, key)) = last_touched.get(read) {
                            arrival_time = max(arrival_time, *time);

                            if arrival_time > (self.total_estimated_gas / len * 10) as u32 {
                                hot_read_access+=1;
                                println!("hot read!");
                            }
                            dependencies.insert(*key);
                        }
                        if len >= 1000 && hot_read_access >= 4 {
                            // In here I can detec if a transaction tries to connect two long paths and then just deny it. That's greedy for sure! Just need a good way to measure it.
                            // todo, if I got multiple reads, combining multiple longer paths. Prevent it. I can do something like. total tx to now = x. The total gas to now is x. The paths are long if at least 10%

                            // not skipping anything atm.
                            skipped+=1;
                            continue;
                        }
                    }

                    let user_state_key = StateKey::raw(tx.sender().to_vec());
                    if let Some((time, key)) = last_touched.get(&user_state_key) {
                        arrival_time = max(arrival_time, *time);

                        if arrival_time > (self.total_estimated_gas / len * 10) as u32 {
                            hot_read_access+=1;
                        }
                        dependencies.insert(*key);
                    }

                    // Check if there is room for the new block.
                    let finish_time = arrival_time + gas_used as u32;
                    if finish_time > self.gas_per_core as u32 {
                        //self.full = true;
                        //println!("bla skip {} {}", self.total_estimated_gas, finish_time);
                        skipped+=1;
                        continue;
                    }


                    if self.total_estimated_gas + gas_used as u64 > (self.total_max_gas) as u64 {
                        self.full = true;
                        println!("Reached max gas: {} {} {}", self.total_estimated_gas, gas_used, self.total_max_gas);
                        break;
                    }

                    self.total_estimated_gas += gas_used as u64;

                    let current_idx = self.block.len() as u16;
                    self.estimated_gas.push(gas_used);
                    // println!("len {}", dependencies.len());
                    self.dependency_graph.push(dependencies);


                    for write in write_set {
                        let curr_max = last_touched.get(&write.0).unwrap_or(&(0u32, 0)).0;
                        if finish_time > curr_max {
                            last_touched.insert(write.0.clone(), (finish_time, current_idx));
                        }
                    }

                    let curr_max = last_touched.get(&user_state_key).unwrap_or(&(0u32, 0)).0;
                    if finish_time > curr_max {
                        last_touched.insert(user_state_key.clone(), (finish_time, current_idx));
                    }

                    let(write_set, read_set, gas , full_tx) =  map.remove(&(txinput.sender(), txinput.sequence_number())).unwrap();
                    self.block.push(full_tx);
                    len+=1;

                    if len == self.max_txns {
                        self.full = true;
                    }
                } else {
                    return vec![];
                }
            }
            println!("longest chain {} {} {}", longest_chain, self.total_max_gas, self.total_estimated_gas);
        }

        println!("skipped: {}", skipped);
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
