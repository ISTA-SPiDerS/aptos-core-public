// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Objects used by/related to shared mempool
use crate::{
    core_mempool::CoreMempool,
    network::{MempoolNetworkInterface, MempoolSyncMsg},
};
use anyhow::Result;
use aptos_config::{
    config::{MempoolConfig, RoleType},
    network_id::PeerNetworkId,
};
use aptos_consensus_types::common::{RejectedTransactionSummary, TransactionSummary};
use aptos_crypto::HashValue;
use aptos_infallible::{Mutex, RwLock};
use aptos_network::{
    application::interface::NetworkClientInterface, transport::ConnectionMetadata,
};
use aptos_storage_interface::DbReader;
use aptos_types::{mempool_status::MempoolStatus, PeerId, transaction::SignedTransaction, vm_status::DiscardedVMStatus};
use aptos_vm_validator::vm_validator::{TransactionValidation, VMSpeculationResult};
use futures::{channel::{mpsc::UnboundedSender, oneshot}, future::Future, StreamExt, task::{Context, Poll}};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fmt,
    pin::Pin,
    sync::Arc,
    task::Waker,
    time::{Instant, SystemTime},
};
use std::cmp::max;
use std::collections::{HashMap, VecDeque};
use std::ptr::null;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::atomic::Ordering::{Relaxed, SeqCst};
use std::sync::mpsc;
use std::time::Duration;
use dashmap::{DashMap, DashSet};
use tokio::runtime::Handle;
use aptos_types::transaction::authenticator::TransactionAuthenticator;
use aptos_types::transaction::RAYON_EXEC_POOL2;
use aptos_types::vm_status::{StatusCode, VMStatus};
use once_cell::sync::{Lazy, OnceCell};
use rayon::iter::ParallelIterator;
use rayon::iter::IntoParallelIterator;
use rayon::prelude::*;
use aptos_types::state_store::state_key::StateKey;
use aptos_types::write_set::{WriteOp, WriteSet};
use crate::core_mempool::{BlockFiller, DependencyFiller, TxnPointer};

pub static SYNC_CACHE: Lazy<std::sync::Mutex<HashMap<TxnPointer, (WriteSet, BTreeSet<StateKey>, u32, SignedTransaction)>>> = Lazy::new(|| { std::sync::Mutex::new(
    HashMap::new()
)});

/// Struct that owns all dependencies required by shared mempool routines.
#[derive(Clone)]
pub(crate) struct SharedMempool<NetworkClient, TransactionValidator> {
    pub mempool: Arc<Mutex<CoreMempool>>,
    pub config: MempoolConfig,
    pub network_interface: MempoolNetworkInterface<NetworkClient>,
    pub db: Arc<dyn DbReader>,
    pub validator: Arc<RwLock<TransactionValidator>>,
    pub subscribers: Vec<UnboundedSender<SharedMempoolNotification>>,
    pub broadcast_within_validator_network: Arc<RwLock<bool>>,
    pub tx_channel: std::sync::mpsc::Sender<SignedTransaction>,
    pub block_channel: kanal::Receiver<DependencyFiller>
}

impl<
        NetworkClient: NetworkClientInterface<MempoolSyncMsg>,
        TransactionValidator: TransactionValidation + 'static,
    > SharedMempool<NetworkClient, TransactionValidator>
{
    pub fn new(
        mempool: Arc<Mutex<CoreMempool>>,
        config: MempoolConfig,
        network_client: NetworkClient,
        db: Arc<dyn DbReader>,
        validator: Arc<RwLock<TransactionValidator>>,
        subscribers: Vec<UnboundedSender<SharedMempoolNotification>>,
        role: RoleType,
        peer_id: PeerId
    ) -> Self {
        let network_interface = MempoolNetworkInterface::new(network_client, role, config.clone(), peer_id);

        let (tx_sender, tx_receiver) =  std::sync::mpsc::channel();
        let (block_sender, block_receiver) = kanal::unbounded();

        let tem_locked_val = validator.clone();
        std::thread::spawn(move ||  {
                let locked_val = tem_locked_val.clone();

                let mut input: Vec<SignedTransaction> = vec![];
                let num_threads = RAYON_EXEC_POOL2.current_num_threads();
                let mut thread_local_cache: DashMap<TxnPointer, (WriteSet, BTreeSet<StateKey>, u32, SignedTransaction)> = DashMap::new();
                loop
                {
                    let mut time = Instant::now();
                    loop
                    {
                        if input.len() >= num_threads * 4 {
                            break;
                        }
                        if let Ok(x) = tx_receiver.try_recv() {
                            input.push(x);
                        } else {
                            let elapsed = time.elapsed().as_millis();
                            if elapsed > 50 {
                                break;
                            }
                        }
                    }

                    let count = input.len();
                    if count > 0 {
                        let exec_counter = AtomicUsize::new(0);
                        {
                            let transaction_validation = locked_val.write();

                            RAYON_EXEC_POOL2.scope(|s| {
                                for _ in 0..4 {
                                    s.spawn(|_| {

                                        let mut current_index = exec_counter.fetch_add(1, Relaxed);
                                        while current_index < count {
                                            let  tx = &input[current_index];
                                            let result = transaction_validation.speculate_transaction(&tx);
                                            let (a, b) = result.unwrap();
                                            match b {
                                                VMStatus::Executed => {
                                                    thread_local_cache.insert((tx.sender(), tx.sequence_number()), (a.output.txn_output().write_set().clone(), a.input, a.output.txn_output().gas_used() as u32, tx.clone()));
                                                }
                                                _ => {
                                                    // Do nothing.
                                                }
                                            }
                                            current_index = exec_counter.fetch_add(1, Relaxed);
                                        }
                                    });
                                }
                            });
                        }

                        input.clear();


                        if thread_local_cache.len() > 0 {
                            if let Ok(mut cache) = SYNC_CACHE.try_lock() {
                                cache.extend(thread_local_cache);
                                thread_local_cache = DashMap::new();
                            }
                        }
                    }

                    if input.is_empty() && thread_local_cache.is_empty() {
                        if let Ok(res) = tx_receiver.recv() {
                            input.push(res);
                        }
                        continue
                    }
                }
        });

        SharedMempool {
            mempool,
            config,
            network_interface,
            db,
            validator,
            subscribers,
            broadcast_within_validator_network: Arc::new(RwLock::new(true)),
            tx_channel: tx_sender,
            block_channel: block_receiver
        }
    }

    pub fn broadcast_within_validator_network(&self) -> bool {
        // This value will be changed true -> false via onchain config when quorum store is enabled.
        // On the transition from true -> false, all transactions in mempool will be eligible for
        // at least one of mempool broadcast or quorum store batch.
        // A transition from false -> true is unexpected -- it would only be triggered if quorum
        // store needs an emergency rollback. In this case, some transactions may not be propagated,
        // they will neither go through a mempool broadcast or quorum store batch.
        *self.broadcast_within_validator_network.read()
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum SharedMempoolNotification {
    PeerStateChange,
    NewTransactions,
    ACK,
    Broadcast,
}

pub(crate) fn notify_subscribers(
    event: SharedMempoolNotification,
    subscribers: &[UnboundedSender<SharedMempoolNotification>],
) {
    for subscriber in subscribers {
        let _ = subscriber.unbounded_send(event);
    }
}

/// A future that represents a scheduled mempool txn broadcast
pub(crate) struct ScheduledBroadcast {
    /// Time of scheduled broadcast
    deadline: Instant,
    peer: PeerNetworkId,
    backoff: bool,
    waker: Arc<Mutex<Option<Waker>>>,
}

impl ScheduledBroadcast {
    pub fn new(deadline: Instant, peer: PeerNetworkId, backoff: bool, executor: Handle) -> Self {
        let waker: Arc<Mutex<Option<Waker>>> = Arc::new(Mutex::new(None));
        let waker_clone = waker.clone();

        if deadline > Instant::now() {
            let tokio_instant = tokio::time::Instant::from_std(deadline);
            executor.spawn(async move {
                tokio::time::sleep_until(tokio_instant).await;
                let mut waker = waker_clone.lock();
                if let Some(waker) = waker.take() {
                    waker.wake()
                }
            });
        }

        Self {
            deadline,
            peer,
            backoff,
            waker,
        }
    }
}

impl Future for ScheduledBroadcast {
    type Output = (PeerNetworkId, bool);

    // (peer, whether this broadcast was scheduled as a backoff broadcast)

    fn poll(self: Pin<&mut Self>, context: &mut Context) -> Poll<Self::Output> {
        if Instant::now() < self.deadline {
            let waker_clone = context.waker().clone();
            let mut waker = self.waker.lock();
            *waker = Some(waker_clone);

            Poll::Pending
        } else {
            Poll::Ready((self.peer, self.backoff))
        }
    }
}

/// Message sent from QuorumStore to Mempool.
pub enum QuorumStoreRequest {
    GetBatchRequest(
        // max batch size
        u64,
        // max byte size
        u64,
        // transactions to exclude from the requested batch
        Vec<TransactionSummary>,
        // callback to respond to
        oneshot::Sender<Result<QuorumStoreResponse>>,
    ),
    // TODO: Do we use it in the real QS as well?
    /// Notifications about *rejected* committed txns.
    RejectNotification(
        // rejected transactions from consensus
        Vec<RejectedTransactionSummary>,
        // callback to respond to
        oneshot::Sender<Result<QuorumStoreResponse>>,
    ),
}

impl fmt::Display for QuorumStoreRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let payload = match self {
            QuorumStoreRequest::GetBatchRequest(max_txns, max_bytes, excluded_txns, _) => {
                format!(
                    "GetBatchRequest [max_txns: {}, max_bytes: {}, excluded_txns_length: {}]",
                    max_txns,
                    max_bytes,
                    excluded_txns.len()
                )
            },
            QuorumStoreRequest::RejectNotification(rejected_txns, _) => {
                format!(
                    "RejectNotification [rejected_txns_length: {}]",
                    rejected_txns.len()
                )
            },
        };
        write!(f, "{}", payload)
    }
}

/// Response sent from mempool to consensus.
#[derive(Debug)]
pub enum QuorumStoreResponse {
    /// Block to submit to consensus
    GetBatchResponse(Vec<SignedTransaction>, Vec<u16>, Vec<Vec<u16>>),
    CommitResponse(),
}

pub type SubmissionStatus = (MempoolStatus, Option<DiscardedVMStatus>);

pub type SubmissionStatusBundle = (SignedTransaction, SubmissionStatus);

pub enum MempoolClientRequest {
    SubmitTransaction(SignedTransaction, oneshot::Sender<Result<SubmissionStatus>>),
    GetTransactionByHash(HashValue, oneshot::Sender<Option<SignedTransaction>>),
}

pub type MempoolClientSender = futures::channel::mpsc::Sender<MempoolClientRequest>;
pub type MempoolEventsReceiver = futures::channel::mpsc::Receiver<MempoolClientRequest>;

/// State of last sync with peer:
/// `timeline_id` is position in log of ready transactions
/// `is_alive` - is connection healthy
#[derive(Clone, Debug)]
pub(crate) struct PeerSyncState {
    pub timeline_id: MultiBucketTimelineIndexIds,
    pub broadcast_info: BroadcastInfo,
    pub metadata: ConnectionMetadata,
}

impl PeerSyncState {
    pub fn new(metadata: ConnectionMetadata, num_broadcast_buckets: usize) -> Self {
        PeerSyncState {
            timeline_id: MultiBucketTimelineIndexIds::new(num_broadcast_buckets),
            broadcast_info: BroadcastInfo::new(),
            metadata,
        }
    }
}

/// Identifier for a broadcasted batch of txns.
/// For BatchId(`start_id`, `end_id`), (`start_id`, `end_id`) is the range of timeline IDs read from
/// the core mempool timeline index that produced the txns in this batch.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct BatchId(pub u64, pub u64);

impl PartialOrd for BatchId {
    fn partial_cmp(&self, other: &BatchId) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BatchId {
    fn cmp(&self, other: &BatchId) -> std::cmp::Ordering {
        (other.0, other.1).cmp(&(self.0, self.1))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct MultiBucketTimelineIndexIds {
    pub id_per_bucket: Vec<u64>,
}

impl MultiBucketTimelineIndexIds {
    pub(crate) fn new(num_buckets: usize) -> Self {
        Self {
            id_per_bucket: vec![0; num_buckets],
        }
    }

    pub(crate) fn update(&mut self, batch_id: &MultiBatchId) {
        if self.id_per_bucket.len() != batch_id.0.len() {
            return;
        }

        for (cur, &(_start, end)) in self.id_per_bucket.iter_mut().zip(batch_id.0.iter()) {
            *cur = std::cmp::max(*cur, end)
        }
    }
}

impl From<Vec<u64>> for MultiBucketTimelineIndexIds {
    fn from(timeline_ids: Vec<u64>) -> Self {
        Self {
            id_per_bucket: timeline_ids,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct MultiBatchId(pub Vec<(u64, u64)>);

impl MultiBatchId {
    pub(crate) fn from_timeline_ids(
        old: &MultiBucketTimelineIndexIds,
        new: &MultiBucketTimelineIndexIds,
    ) -> Self {
        Self(
            old.id_per_bucket
                .iter()
                .cloned()
                .zip(new.id_per_bucket.iter().cloned())
                .collect(),
        )
    }
}

impl PartialOrd for MultiBatchId {
    fn partial_cmp(&self, other: &MultiBatchId) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// Note: in rev order to check significant pairs first (right -> left)
impl Ord for MultiBatchId {
    fn cmp(&self, other: &MultiBatchId) -> std::cmp::Ordering {
        for (&self_pair, &other_pair) in self.0.iter().rev().zip(other.0.iter().rev()) {
            let ordering = self_pair.cmp(&other_pair);
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        Ordering::Equal
    }
}

#[cfg(test)]
mod test {
    use crate::shared_mempool::types::{MultiBatchId, MultiBucketTimelineIndexIds};

    #[test]
    fn test_multi_bucket_timeline_ids_update() {
        let mut timeline_ids = MultiBucketTimelineIndexIds {
            id_per_bucket: vec![1, 2, 3],
        };
        let batch_id = MultiBatchId(vec![(1, 3), (1, 1), (3, 6)]);
        timeline_ids.update(&batch_id);
        assert_eq!(vec![3, 2, 6], timeline_ids.id_per_bucket);
    }

    #[test]
    fn test_multi_batch_id_ordering() {
        let left = MultiBatchId(vec![(0, 3), (1, 4), (2, 5)]);
        let right = MultiBatchId(vec![(2, 5), (1, 4), (0, 3)]);

        assert!(left > right);
    }
}

/// Txn broadcast-related info for a given remote peer.
#[derive(Clone, Debug)]
pub struct BroadcastInfo {
    // Sent broadcasts that have not yet received an ack.
    pub sent_batches: BTreeMap<MultiBatchId, SystemTime>,
    // Broadcasts that have received a retry ack and are pending a resend.
    pub retry_batches: BTreeSet<MultiBatchId>,
    // Whether broadcasting to this peer is in backoff mode, e.g. broadcasting at longer intervals.
    pub backoff_mode: bool,
}

impl BroadcastInfo {
    fn new() -> Self {
        Self {
            sent_batches: BTreeMap::new(),
            retry_batches: BTreeSet::new(),
            backoff_mode: false,
        }
    }
}
