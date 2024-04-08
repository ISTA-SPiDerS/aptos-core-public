// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeSet;
use anyhow::Result;
use aptos_state_view::account_with_state_view::AsAccountWithStateView;
use aptos_storage_interface::{
    cached_state_view::CachedDbStateView,
    state_view::{DbStateView, LatestDbStateCheckpointView},
    DbReader,
};
use aptos_types::{
    account_address::AccountAddress,
    account_config::AccountSequenceInfo,
    account_view::AccountView,
    on_chain_config::OnChainConfigPayload,
    transaction::{SignedTransaction, VMValidatorResult},
};
use aptos_vm::AptosVM;
use fail::fail_point;
use std::sync::{Arc, Mutex};
use aptos_state_view::{StateView, StateViewId, TStateView};
use aptos_types::state_store::state_key::StateKey;
use aptos_types::state_store::state_storage_usage::StateStorageUsage;
use aptos_types::transaction::Transaction;
use aptos_types::vm_status::VMStatus;
use aptos_types::write_set::WriteSet;
use aptos_vm::data_cache::{IntoMoveResolver, StorageAdapterOwned};
use aptos_vm::logging::AdapterLogSchema;
use aptos_vm::adapter_common::{preprocess_transaction, VMAdapter};
use aptos_types::state_store::state_value::StateValue;
use aptos_aggregator::transaction::TransactionOutputExt;


#[cfg(test)]
#[path = "unit_tests/vm_validator_test.rs"]
mod vm_validator_test;

#[derive(Clone)]
pub struct VMSpeculationResult {
    pub input: TransactionReadSet,
    pub output: TransactionOutputExt,
}

pub trait TransactionValidation: Send + Sync + Clone {
    type ValidationInstance: aptos_vm::VMValidator;

    /// Validate a txn from client
    fn validate_transaction(&self, _txn: SignedTransaction) -> Result<VMValidatorResult>;

    /// Restart the transaction validation instance
    fn restart(&mut self, config: OnChainConfigPayload) -> Result<()>;

    /// Notify about new commit
    fn notify_commit(&mut self);

    fn speculate_transaction(
        &self,
        transaction: &SignedTransaction,
    ) -> anyhow::Result<(VMSpeculationResult, VMStatus)>;

    fn add_write_set(
        &mut self,
        write_set: &WriteSet
    );
}


type TransactionReadSet = BTreeSet<StateKey>;

pub struct ReadRecorderView<'a, S: StateView> {
    base_view: &'a S,
    captured_reads: Mutex<TransactionReadSet>,
}

impl<'a, S: StateView> ReadRecorderView<'a, S> {
    pub fn new_view(
        base_view: &'a S,
    ) -> StorageAdapterOwned<ReadRecorderView<'a, S>> {
        ReadRecorderView {
            base_view,
            captured_reads: Mutex::new(BTreeSet::new()),
        }.into_move_resolver()
    }

    pub fn reads(&mut self) -> TransactionReadSet {
        let mut reads = self.captured_reads.lock().unwrap();
        std::mem::take(&mut reads)
    }
}

impl<'a, S : StateView> TStateView for ReadRecorderView<'a, S> {
    type Key = StateKey;

    fn id(&self) -> StateViewId {
        self.base_view.id()
    }

    fn get_state_value(&self, state_key: &StateKey) -> Result<Option<StateValue>> {
        self.captured_reads.lock().unwrap().insert(state_key.clone());
        self.base_view.get_state_value(state_key)
    }

    fn is_genesis(&self) -> bool {
        self.base_view.is_genesis()
    }

    fn get_usage(&self) -> anyhow::Result<StateStorageUsage> {
        self.base_view.get_usage()
    }
}

pub struct VMValidator {
    db_reader: Arc<dyn DbReader>,
    state_view: CachedDbStateView,
    vm: AptosVM,
}

impl Clone for VMValidator {
    fn clone(&self) -> Self {
        Self::new(self.db_reader.clone())
    }
}

impl VMValidator {
    pub fn new(db_reader: Arc<dyn DbReader>) -> Self {
        let db_state_view = db_reader
            .latest_state_checkpoint_view()
            .expect("Get db view cannot fail");

        let vm = AptosVM::new_for_validation(&db_state_view);
        VMValidator {
            db_reader,
            state_view: db_state_view.into(),
            vm,
        }
    }
}

impl TransactionValidation for VMValidator {
    type ValidationInstance = AptosVM;

    fn validate_transaction(&self, txn: SignedTransaction) -> Result<VMValidatorResult> {
        fail_point!("vm_validator::validate_transaction", |_| {
            Err(anyhow::anyhow!(
                "Injected error in vm_validator::validate_transaction"
            ))
        });
        use aptos_vm::VMValidator;

        Ok(self.vm.validate_transaction(txn, &self.state_view))
    }

    fn restart(&mut self, _config: OnChainConfigPayload) -> Result<()> {
        self.notify_commit();

        self.vm = AptosVM::new_for_validation(&self.state_view);
        Ok(())
    }

    fn notify_commit(&mut self) {
        self.state_view = self
            .db_reader
            .latest_state_checkpoint_view()
            .expect("Get db view cannot fail")
            .into();
    }

    fn speculate_transaction(
        &self,
        transaction: &SignedTransaction,
    ) -> anyhow::Result<(VMSpeculationResult, VMStatus)> {
        let log_context = AdapterLogSchema::new(self.state_view.id(), 0);
        let mut read_recorder_view = ReadRecorderView::new_view(&self.state_view);
        let preprocessed_txn = preprocess_transaction::<AptosVM>(Transaction::UserTransaction(transaction.clone()));

        let (status, output, _) = self.vm.execute_single_transaction(&preprocessed_txn, &read_recorder_view, &log_context, true).unwrap();
        let input = read_recorder_view.reads();

        anyhow::Ok((VMSpeculationResult {input, output}, status))
    }

    fn add_write_set(
        &mut self,
        write_set: &WriteSet
    ) {

    }
}

/// returns account's sequence number from storage
pub fn get_account_sequence_number(
    state_view: &DbStateView,
    address: AccountAddress,
) -> Result<AccountSequenceInfo> {
    fail_point!("vm_validator::get_account_sequence_number", |_| {
        Err(anyhow::anyhow!(
            "Injected error in get_account_sequence_number"
        ))
    });

    let account_state_view = state_view.as_account_with_state_view(&address);

    match account_state_view.get_account_resource()? {
        Some(account_resource) => Ok(AccountSequenceInfo::Sequential(
            account_resource.sequence_number(),
        )),
        None => Ok(AccountSequenceInfo::Sequential(0)),
    }
}
