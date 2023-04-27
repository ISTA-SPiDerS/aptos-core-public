use std::borrow::Borrow;
use std::cmp::max;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::mem;
use std::mem::size_of;
use dashmap::{DashMap, DashSet};
use rayon::iter::IntoParallelIterator;
use aptos_types::state_store::state_key::StateKey;
use aptos_types::transaction::{RAYON_EXEC_POOL, SignedTransaction, authenticator::TransactionAuthenticator};
use aptos_types::vm_status::VMStatus;
use aptos_vm_validator::vm_validator::{TransactionValidation, VMSpeculationResult};
use std::collections::HashMap;
use rayon::iter::IndexedParallelIterator;
use rayon::iter::ParallelIterator;
use anyhow::{anyhow, Result, Error};

pub trait BlockFiller {
    fn add(&mut self, txn: SignedTransaction) -> bool;
    fn add_all(
        &mut self,
        txn: VecDeque<SignedTransaction>,
        past_results: &DashMap<TransactionAuthenticator, (VMSpeculationResult, VMStatus)>,
    ) -> Vec<SignedTransaction>;

    fn get_block(self) -> Vec<SignedTransaction>;
    fn get_blockx(&mut self) -> Vec<SignedTransaction>;

    fn is_full(&self) -> bool;

    fn get_max_bytes(&self) -> u64;

    fn get_max_txn(&self) -> u64;
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
            block: vec![],
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
        past_results: &DashMap<TransactionAuthenticator, (VMSpeculationResult, VMStatus)>
    ) -> Vec<SignedTransaction> {
        let mut rejected = vec![];

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
        rejected
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
}

type TransactionIdx = u64;

pub struct DependencyFiller<'a, V: TransactionValidation, const C: u64> {
    transaction_validation: &'a mut V,
    gas_per_core: u64,
    max_bytes: u64,
    max_txns: u64,

    total_bytes: u64,
    total_estimated_gas: u64,
    full: bool,

    last_touched: BTreeMap<StateKey, u64>,
    writes: BTreeMap<StateKey, Vec<TransactionIdx>>,
    dependency_graph: Vec<HashSet<TransactionIdx>>,

    block: Vec<SignedTransaction>,
    estimated_gas: Vec<u64>,
}

impl<'a, V: TransactionValidation, const C: u64> DependencyFiller<'a, V, C> {
    pub fn new(
        transaction_validation: &'a mut V,
        gas_per_core: u64,
        max_bytes: u64,
        max_txns: u64)
        -> DependencyFiller<V, C> {
        Self {
            transaction_validation,
            gas_per_core,
            max_bytes,
            max_txns,
            total_bytes: 0,
            total_estimated_gas: 0,
            full: false,
            last_touched: BTreeMap::new(),
            writes: BTreeMap::new(),
            dependency_graph: vec![],
            block: vec![],
            estimated_gas: vec![],
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

impl<'a, V: TransactionValidation, const C: u64> BlockFiller for DependencyFiller<'a, V, C> {
    fn add(&mut self, txn: SignedTransaction) -> bool {
        if self.full {
            return false;
        }

        let txn_len = txn.raw_txn_bytes_len() as u64;
        if self.total_bytes + txn_len > self.max_bytes {
            self.full = true;
            return false;
        }

        if let anyhow::Result::Ok((speculation, status)) = self.transaction_validation.speculate_transaction(&txn) {
            let read_set = &speculation.input;
            let write_set = speculation.output.txn_output().write_set();
            let delta_set = speculation.output.delta_change_set();
            let gas_used = speculation.output.txn_output().gas_used();


            // When transaction can start assuming unlimited resources.
            let mut arrival_time = 0;
            for read in read_set {
                if self.last_touched.contains_key(&read) {
                    arrival_time = max(arrival_time, *self.last_touched.get(&read).unwrap())
                }
            }

            // Check if there is room for the new block.
            let finish_time = arrival_time + gas_used;
            if finish_time > self.gas_per_core {
                self.full = true;
                return false;
            }
            if self.total_estimated_gas + gas_used > self.gas_per_core * C {
                self.full = true;
                return false;
            }

            let mut dependencies = HashSet::new();
            for read in read_set {
                if self.writes.contains_key(&read) {
                    for parent_txn in self.writes.get(&read).unwrap() {
                        dependencies.insert(*parent_txn);
                    }
                }
            }

            if self.total_bytes + txn_len + (dependencies.len() as u64) * (size_of::<TransactionIdx>() as u64) + (size_of::<u64>() as u64) > self.max_bytes {
                self.full = true;
                return false;
            }

            self.total_bytes += txn_len + dependencies.len() as u64 * size_of::<TransactionIdx>() as u64 + size_of::<u64>() as u64;
            self.total_estimated_gas += gas_used;
            
            let current_idx = self.block.len() as TransactionIdx;

            self.block.push(txn);
            self.estimated_gas.push(gas_used);
            // println!("len {}", dependencies.len());
            self.dependency_graph.push(dependencies);

            //self.transaction_validation.add_write_set(write_set);

            // Update last touched time for used resources.
            const ZERO: u64 = 0u64;
            for (write, _op) in write_set {
                let mx = max(finish_time, *self.last_touched.get(write).unwrap_or(&ZERO));
                self.last_touched.insert(write.clone(), mx.into());

                if !self.writes.contains_key(write) {
                    self.writes.insert(write.clone(), vec![]);
                }

                self.writes.remove(write);
                self.writes.insert(write.clone(), vec![current_idx]);
            }

            if self.block.len() as u64 == self.max_txns {
                self.full = true;
            }

            return true;
        }

        return false;
    }

    fn add_all(
        &mut self,
        mut txn: VecDeque<SignedTransaction>,
        past_results: &DashMap<TransactionAuthenticator, (VMSpeculationResult, VMStatus)>,
    ) -> Vec<SignedTransaction> {
        let result : HashMap<usize, anyhow::Result<(VMSpeculationResult, VMStatus)>> = RAYON_EXEC_POOL.install(|| {
            (&txn)
                .into_par_iter()
                .enumerate()
                .map(|(i, tx)| {
                    match past_results.get(&tx.authenticator()) {
                        Some(result) => (i, Ok(result.value())),
                        None => {
                            let result = self.transaction_validation.speculate_transaction(&tx);
                            (i, match result {
                                Result::Ok((ref a, ref b)) => Ok(&past_results.insert(tx.authenticator(), ((a.clone(), b.clone()))).unwrap()),
                                Result::Err(ref e) => {
                                    println!("Error during pre execution {}", e);
                                    &Err(anyhow!("Error during pre execution"))
                                },
                            })
                        }
                    }
                })
                .collect()
        });

        let mut rejected = vec![];

        let mut index:usize = 0;
        while let Some(tx) = txn.pop_front()
        {
            let res = result.get(&index);
            if self.full {
                println!("bla estgas1 {}", self.total_estimated_gas);

                rejected.push(tx);
                rejected.extend(txn);
                println!("bla final gas1: {}", self.total_estimated_gas);

                return rejected;
            }

            let txn_len = tx.raw_txn_bytes_len() as u64;
            if self.total_bytes + txn_len > self.max_bytes {
                println!("bla estgas2 {}", self.total_estimated_gas);
                self.full = true;
                rejected.push(tx);
                rejected.extend(txn);
                println!("bla final gas2: {}", self.total_estimated_gas);

                return rejected;
            }

            if let anyhow::Result::Ok((speculation, status)) = res.unwrap() {
                match status {
                    VMStatus::Executed => {}
                    VMStatus::Error(e) => {println!("blub exec failure1 {:?}", e)}
                    VMStatus::MoveAbort(e1, e2) => {println!("blub exec failure2 {:?}", e1)}
                    VMStatus::ExecutionFailure { .. } => {println!("blub exec failure3")}
                }
                let read_set = &speculation.input;
                let write_set = speculation.output.txn_output().write_set();
                let delta_set = speculation.output.delta_change_set();
                let gas_used = speculation.output.txn_output().gas_used();

                if gas_used > 10000
                {
                    println!("bla Wat a big tx: {}", gas_used);
                }

                if write_set.is_empty() && delta_set.is_empty()
                {
                   println!("Empty????");
                }

                // When transaction can start assuming unlimited resources.
                let mut arrival_time = 0;
                for read in read_set {
                    if self.last_touched.contains_key(&read) {
                        arrival_time = max(arrival_time, *self.last_touched.get(&read).unwrap())
                    }
                }

                // Check if there is room for the new block.
                let finish_time = arrival_time + gas_used;
                if finish_time > 50000
                {
                    println!("bla Wat a long chain: {}", finish_time);
                }
                if finish_time > self.gas_per_core {
                    //self.full = true;
                    rejected.push(tx);
                    continue;
                }

                if self.total_estimated_gas + gas_used > self.gas_per_core * C {
                    self.full = true;
                    rejected.push(tx);
                    rejected.extend(txn);
                    println!("bla final gas4: {}", self.total_estimated_gas);
                    return rejected;
                }

                let mut dependencies = HashSet::new();
                for read in read_set {
                    if self.writes.contains_key(&read) {
                        for parent_txn in self.writes.get(&read).unwrap() {
                            dependencies.insert(*parent_txn);
                        }
                    }
                }

                if self.total_bytes + txn_len + (dependencies.len() as u64) * (size_of::<TransactionIdx>() as u64) + (size_of::<u64>() as u64) > self.max_bytes {
                    self.full = true;
                    rejected.push(tx);
                    rejected.extend(txn);
                    println!("bla final gas5: {}", self.total_estimated_gas);
                    return rejected;
                }

                self.total_bytes += txn_len + dependencies.len() as u64 * size_of::<TransactionIdx>() as u64 + size_of::<u64>() as u64;
                self.total_estimated_gas += gas_used;

                let current_idx = self.block.len() as TransactionIdx;

                past_results.remove(&tx.authenticator());
                self.block.push(tx);
                self.estimated_gas.push(gas_used);
                // println!("len {}", dependencies.len());
                self.dependency_graph.push(dependencies);

                //self.transaction_validation.add_write_set(write_set);

                // Update last touched time for used resources.
                for (write, _op) in write_set {
                    let mx = max(finish_time, *self.last_touched.get(write).unwrap_or(&0u64));
                    self.last_touched.insert(write.clone(), mx.into());

                    if !self.writes.contains_key(write) {
                        self.writes.insert(write.clone(), vec![]);
                    }

                    self.writes.remove(write);
                    self.writes.insert(write.clone(), vec![current_idx]);
                }

                if self.block.len() as u64 == self.max_txns {
                    self.full = true;
                    println!("bla estgas5 {}", self.total_estimated_gas);
                    rejected.extend(txn);
                    println!("bla final gas6: {}", self.total_estimated_gas);

                    return rejected;
                }
            }
            index+=1;
        }

        println!("bla final gas7: {}", self.total_estimated_gas);
        return rejected;
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
}
