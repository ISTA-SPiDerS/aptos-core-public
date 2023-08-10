// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

pub(crate) mod vm_wrapper;

use crate::{
    adapter_common::{preprocess_transaction, PreprocessedTransaction},
    block_executor::vm_wrapper::AptosExecutorTask,
    counters::{
        BLOCK_EXECUTOR_CONCURRENCY, BLOCK_EXECUTOR_EXECUTE_BLOCK_SECONDS,
        BLOCK_EXECUTOR_SIGNATURE_VERIFICATION_SECONDS,
    },
    AptosVM,
};
use aptos_aggregator::{delta_change_set::DeltaOp, transaction::TransactionOutputExt};
use aptos_block_executor::{
    errors::Error,
    task::{
        Transaction as BlockExecutorTransaction,
        TransactionOutput as BlockExecutorTransactionOutput,
    },
};
use aptos_state_view::StateView;
use aptos_types::{
    state_store::state_key::StateKey,
    transaction::{Transaction, TransactionOutput, TransactionStatus},
    write_set::{WriteOp, WriteSet, WriteSetMut},
};
use move_core_types::vm_status::VMStatus;
use rayon::prelude::*;
use aptos_block_executor::executor::BlockExecutor;
use aptos_types::transaction::{ExecutionMode, Profiler, RAYON_EXEC_POOL, TransactionRegister};

impl BlockExecutorTransaction for PreprocessedTransaction {
    type Key = StateKey;
    type Value = WriteOp;
}

// Wrapper to avoid orphan rule
pub(crate) struct AptosTransactionOutput(TransactionOutputExt);

impl AptosTransactionOutput {
    pub fn new(output: TransactionOutputExt) -> Self {
        Self(output)
    }

    pub fn into(self) -> TransactionOutputExt {
        self.0
    }
}

impl BlockExecutorTransactionOutput for AptosTransactionOutput {
    type Txn = PreprocessedTransaction;

    fn get_writes(&self) -> Vec<(StateKey, WriteOp)> {
        self.0
            .txn_output()
            .write_set()
            .iter()
            .map(|(key, op)| (key.clone(), op.clone()))
            .collect()
    }

    fn get_deltas(&self) -> Vec<(StateKey, DeltaOp)> {
        self.0
            .delta_change_set()
            .iter()
            .map(|(key, op)| (key.clone(), *op))
            .collect()
    }

    /// Execution output for transactions that comes after SkipRest signal.
    fn skip_output() -> Self {
        Self(TransactionOutputExt::from(TransactionOutput::new(
            WriteSet::default(),
            vec![],
            0,
            TransactionStatus::Retry,
        )))
    }
}

pub struct BlockAptosVM();

impl BlockAptosVM {
    pub fn execute_block<S: StateView + Sync>(
        transactions: TransactionRegister<Transaction>,
        state_view: &S,
        concurrency_level: usize,
        mode: ExecutionMode,
        profiler: &mut Profiler,
    ) -> Result<Vec<TransactionOutput>, VMStatus> {
        let timer = BLOCK_EXECUTOR_EXECUTE_BLOCK_SECONDS.start_timer();
        // Verify the signatures of all the transactions in parallel.
        // This is time consuming so don't wait and do the checking
        // sequentially while executing the transactions.
        let signature_verified_block: Vec<PreprocessedTransaction> =
                transactions.txns().to_vec()
                    .into_iter()
                    .map(preprocess_transaction::<AptosVM>)
                    .collect();

        let register = TransactionRegister::new(signature_verified_block, transactions.gas_estimates().clone(), transactions.dependency_graph().clone());
        
        BLOCK_EXECUTOR_CONCURRENCY.set(concurrency_level as i64);
        let executor = BlockExecutor::<PreprocessedTransaction, AptosExecutorTask<S>, S>::new(
            concurrency_level,
        );

        let ret = executor
            .execute_block(state_view, register, state_view, mode, profiler)
            .map(|results| {
                // Process the outputs in parallel, combining delta writes with other writes.
                RAYON_EXEC_POOL.lock().unwrap().install(|| {
                    results
                        .into_par_iter()
                        .map(|(output, delta_writes)| {
                            output      // AptosTransactionOutput
                            .into()     // TransactionOutputExt
                            .output_with_delta_writes(WriteSetMut::new(delta_writes))
                        })
                        .collect()
                })
            });

        let block_ex_timer = timer.stop_and_discard();
        println!("Execution time of block = {}", block_ex_timer);
        match ret {
            Ok(outputs) => {
                let ot:Vec<TransactionOutput> = outputs;
                for result in &ot {
                    match result.status() {
                        TransactionStatus::Keep(status) => { // do nothin
                        }
                        TransactionStatus::Discard(status) => println!("transaction discarded with {:?}", status),
                        TransactionStatus::Retry => println!("transaction status is retry"),
                    };
                }


                Ok(ot)
            },
            Err(Error::ModulePathReadWrite) => {
                unreachable!("[Execution]: Must be handled by sequential fallback")
            },
            Err(Error::UserError(err)) => return Err(err),
            _ => unreachable!("What")
        }


    }
}
