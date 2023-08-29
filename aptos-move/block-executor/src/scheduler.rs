// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

use aptos_infallible::Mutex;
use std::sync::Mutex as MyMut;
use crossbeam::utils::CachePadded;
use parking_lot::{RwLock, RwLockUpgradableReadGuard};
use tracing::info;
use tracing_subscriber::EnvFilter;
use std::{
    io,
    cmp::{min, Ordering as cmpOrdering,max},
    hint,
    ops::DerefMut,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Condvar,
    },
    collections::{
        BTreeSet,
        BTreeMap,
        HashMap,
    },
    ops::ControlFlow,
    future::Future,
    pin::Pin,
    task::{Context,Poll, Waker},
};
use std::sync::atomic::AtomicU16;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use color_eyre::Report;
use dashmap::{DashMap, DashSet};
use itertools::equal;
use aptos_types::transaction::{ExecutionMode, Profiler, RAYON_EXEC_POOL, TransactionStatus};
use crossbeam_skiplist::SkipSet;
use rand::prelude::*;
use tokio::sync::mpsc;
use tokio::sync::mpsc::{UnboundedSender, UnboundedReceiver};
use tokio::sync::mpsc::error::SendError;
use tracing_subscriber::fmt::time;
use crate::scheduler::SchedulerTask::NoTask;
// use async_priority_channel::*;

const TXN_IDX_MASK: u64 = (1 << 32) - 1;
const BATCH_SIZE: usize = 30;

// Type aliases.
pub type TxnIndex = usize;
pub type Incarnation = usize;
pub type Wave = u32;
pub type Version = (TxnIndex, Incarnation);
type DependencyCondvar = Arc<(Mutex<bool>, Condvar)>;

/// A holder for potential task returned from the Scheduler. ExecutionTask and ValidationTask
/// each contain a version of transaction that must be executed or validated, respectively.
/// NoTask holds no task (similar None if we wrapped tasks in Option), and Done implies that
/// there are no more tasks and the scheduler is done.

// pub struct MyFuture {
//     shared_state: Arc<Mutex<SharedState>>,

// }

// struct SharedState {
//     completed: bool,
//     waker: Option<Waker>,
// }
// impl Future for MyFuture {
//     type Output = ();


//     fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
//         let mut shared_state = self.shared_state.lock();
//         if shared_state.completed {
//             Poll::Ready(())

//         }
//         else {
//             shared_state.waker = Some(cx.waker().clone());
//             Poll::Pending
//         }
//     }


pub enum SchedulerTask {
    ExecutionTask(Version, Option<DependencyCondvar>),
    ValidationTask(Version, Wave),
    SigTask(usize),
    PrologueTask,
    NoTask,
    Done,
}

#[derive(Copy, Clone, Eq, PartialEq)]
pub struct Task {
    bottomlevel: usize,
    index: TxnIndex,
}

impl Ord for Task {
    fn cmp(&self, other: &Self) -> cmpOrdering {
        other.bottomlevel.cmp(&self.bottomlevel).then_with(|| self.index.cmp(&other.index))
    }
}

impl PartialOrd for Task {
    fn partial_cmp(&self, other: &Self) -> Option<cmpOrdering> {
        Some(self.cmp(other))
    }
}

pub struct Parent {
    idx: TxnIndex,
    finish_time: usize,
}

/////////////////////////////// Explanation for ExecutionStatus ///////////////////////////////
/// All possible execution status for each transaction. In the explanation below, we abbreviate
/// 'execution status' as 'status'. Each status contains the latest incarnation number,
/// where incarnation = i means it is the i-th execution instance of the transaction.
///
/// 'ReadyToExecute' means that the corresponding incarnation should be executed and the scheduler
/// must eventually create a corresponding execution task. The scheduler ensures that exactly one
/// execution task gets created, changing the status to 'Executing' in the process. If a dependency
/// condition variable is set, then an execution of a prior incarnation is waiting on it with
/// a read dependency resolved (when dependency was encountered, the status changed to Suspended,
/// and suspended changed to ReadyToExecute when the dependency finished its execution). In this case
/// the caller need not create a new execution task, but just nofity the suspended execution.
///
/// 'Executing' status of an incarnation turns into 'Executed' if the execution task finishes, or
/// if a dependency is encountered, it becomes 'ReadyToExecute(incarnation + 1)' once the
/// dependency is resolved. An 'Executed' status allows creation of validation tasks for the
/// corresponding incarnation, and a validation failure leads to an abort. The scheduler ensures
/// that there is exactly one abort, changing the status to 'Aborting' in the process. Once the
/// thread that successfully aborted performs everything that's required, it sets the status
/// to 'ReadyToExecute(incarnation + 1)', allowing the scheduler to create an execution
/// task for the next incarnation of the transaction.
///
/// Status transition diagram:
/// Ready(i)
///    |  try_incarnate (incarnate successfully)
///    |
///    ↓         suspend (waiting on dependency)                resume
/// Executing(i) -----------------------------> Suspended(i) ------------> Ready(i)
///    |
///    |  finish_execution
///    ↓
/// Executed(i) (pending for (re)validations) ---------------------------> Committed(i)
///    |
///    |  try_abort (abort successfully)
///    ↓                finish_abort
/// Aborting(i) ---------------------------------------------------------> Ready(i+1)
///
#[derive(Debug)]
enum ExecutionStatus {
    ReadyToExecute(Incarnation, Option<DependencyCondvar>),
    Executing(Incarnation),
    Suspended(Incarnation, DependencyCondvar),
    Executed(Incarnation),
    Committed(Incarnation),
    Aborting(Incarnation),
}

impl PartialEq for ExecutionStatus {
    fn eq(&self, other: &Self) -> bool {
        use ExecutionStatus::*;
        match (self, other) {
            (&ReadyToExecute(ref a, _), &ReadyToExecute(ref b, _))
            | (&Executing(ref a), &Executing(ref b))
            | (&Suspended(ref a, _), &Suspended(ref b, _))
            | (&Executed(ref a), &Executed(ref b))
            | (&Committed(ref a), &Committed(ref b))
            | (&Aborting(ref a), &Aborting(ref b)) => a == b,
            _ => false,
        }
    }
}

/////////////////////////////// Explanation for ValidationStatus ///////////////////////////////
/// All possible validation status for each transaction. In the explanation below, we abbreviate
/// 'validation status' as 'status'. Each status contains three wave numbers, each with different
/// meanings, but in general the concept of 'wave' keeps track of the version number of the validation.
///
/// 'max_triggered_wave' records the maximum wave that was triggered at the transaction index, and
/// will be incremented every time when the validation_idx is decreased. Initialized as 0.
///
/// 'maybe_max_validated_wave' records the maximum wave among successful validations of the corresponding
/// transaction, will be incremented upon successful validation (finish_validation). Initialized as None.
///
/// 'required_wave' in addition records the wave that must be successfully validated in order
/// for the transaction to be committed, required to handle the case of the optimization in
/// finish_execution when only the transaction itself is validated (if last incarnation
/// didn't write outside of the previous write-set). Initilized as 0.
///
/// Other than ValidationStatus, the 'wave' information is also recorded in 'validation_idx' and 'commit_state'.
/// Below is the description of the wave meanings and how they are updated. More details can be
/// found in the definition of 'validation_idx' and 'commit_state'.
///
/// In 'validation_idx', the first 32 bits identifies a validation wave while the last 32 bits
/// contain an index that tracks the minimum of all transaction indices that require validation.
/// The wave is incremented whenever the validation_idx is reduced due to transactions requiring
/// validation, in particular, after aborts and executions that write outside of the write set of
/// the same transaction's previous incarnation.
///
/// In 'commit_state', the first element records the next transaction to commit, and the
/// second element records the lower bound on the wave of a validation that must be successful
/// in order to commit the next transaction. The wave is updated in try_commit, upon seeing an
/// executed txn with higher max_triggered_wave. Note that the wave is *not* updated with the
/// required_wave of the txn that is being committed.
///
///
/////////////////////////////// Algorithm Description for Updating Waves ///////////////////////////////
/// In the following, 'update' means taking the maximum.
/// (1) Upon decreasing validation_idx, increment validation_idx.wave and update txn's max_triggered_wave <- validation_idx.wave;
/// (2) Upon finishing execution of txn that is below validation_idx, if this execution does not write new places,
/// then only this txn needs validation but not all later txns, update txn's required_wave <- validation_idx.wave;
/// (3) Upon validating a txn successfully, update txn's maybe_max_validated_wave <- validation_idx.wave;
/// (4) Upon trying to commit an executed txn, update commit_state.wave <- txn's max_triggered_wave.
/// (5) If txn's maybe_max_validated_wave >= max(commit_state.wave, txn's required_wave), can commit the txn.
///
/// Remark: commit_state.wave is updated only with max_triggered_wave but not required_wave. The reason is that we rely on
/// the first txn's max_triggered_wave being incremented during a new wave (due to decreasing validation_idx).
/// Then, since commit_state.wave is updated with the first txn's max_triggered_wave, all later txns also
/// need to have a maybe_max_validated_wave in order to be committed, which indicates they have the up-to-date validation
/// wave. Similarly, for required_wave which is incremented only when the single txn needs validation,
/// the commit_state.wave is not updated since later txns do not need new wave of validation.

#[derive(Debug)]
struct ValidationStatus {
    max_triggered_wave: Wave,
    required_wave: Wave,
    maybe_max_validated_wave: Option<Wave>,
}

impl ValidationStatus {
    pub fn new() -> Self {
        ValidationStatus {
            max_triggered_wave: 0,
            required_wave: 0,
            maybe_max_validated_wave: None,
        }
    }
}

pub struct Scheduler {
    /// Number of txns to execute, immutable.
    pub(crate) num_txns: usize,

    /// A shared index that tracks the minimum of all transaction indices that require execution.
    /// The threads increment the index and attempt to create an execution task for the corresponding
    /// transaction, if the status of the txn is 'ReadyToExecute'. This implements a counting-based
    /// concurrent ordered set. It is reduced as necessary when transactions become ready to be
    /// executed, in particular, when execution finishes and dependencies are resolved.
    execution_idx: AtomicUsize,
    /// The first 32 bits identifies a validation wave while the last 32 bits contain an index
    /// that tracks the minimum of all transaction indices that require validation.
    /// The threads increment this index and attempt to create a validation task for the
    /// corresponding transaction (if the status of the txn is 'Executed'), associated with the
    /// observed wave in the first 32 bits. Each validation wave represents the sequence of
    /// validations that must happen due to the fixed serialization order of transactions.
    /// The index is reduced as necessary when transactions require validation, in particular,
    /// after aborts and executions that write outside of the write set of the same transaction's
    /// previous incarnation. This also creates a new wave of validations, identified by the
    /// monotonically increasing index stored in the first 32 bits.
    validation_idx: AtomicU64,
    /// Next transaction to commit, and sweeping lower bound on the wave of a validation that must
    /// be successful in order to commit the next transaction.
    commit_state: Mutex<(TxnIndex, Wave)>,

    /// Shared marker that is set when a thread detects that all txns can be committed.
    done_marker: AtomicBool,

    /// An index i maps to indices of other transactions that depend on transaction i, i.e. they
    /// should be re-executed once transaction i's next incarnation finishes.
    txn_dependency: Vec<CachePadded<RwLock<HashMap<TxnIndex, usize>>>>,
    opt_txn_dependency: DashSet<(TxnIndex, TxnIndex)>,

    /// An index i maps to the most up-to-date status of transaction i.
    txn_status: Vec<CachePadded<(RwLock<ExecutionStatus>, RwLock<ValidationStatus>)>>,

    hint_graph: Vec<CachePadded<Vec<TxnIndex>>>,

    use_hints: bool,

    on_thread: Vec<CachePadded<Mutex<usize>>>,

    // round_number: AtomicUsize, //might need to make this AtomicUsize, no actual need but Rust

    my_last_stop: Vec<Mutex<usize>>,

    max: Mutex<usize>,

    concurrency_level: usize,

    gas_estimates: CachePadded<Vec<u64>>,

    sched_lock: AtomicUsize,

    condvars:Vec<(Mutex<bool>,Condvar)>,

    heap: SkipSet<Task>,

    finish_time: Vec<Mutex<usize>>,

    mapping: RwLock<Vec<usize>>,

    incoming: Mutex<Vec<usize>>,

    children: Vec<Vec<TxnIndex>>,

    end_comp:  Mutex<Vec<usize>>,

    bottomlevels: Vec<usize>,

    nscheduled:AtomicUsize,

    pub(crate) channels: Vec<(UnboundedSender<TxnIndex>, Mutex<UnboundedReceiver<TxnIndex>>)>,

    pub(crate) priochannels: Vec<(UnboundedSender<TxnIndex>, Mutex<UnboundedReceiver<TxnIndex>>)>,

    valock: MyMut<bool>,

    pub(crate) sig_val_idx: AtomicUsize,

    pub prologue_map: HashMap<u16, (bool, MyMut<bool>)>,

    pub prologue_index: AtomicU16,
    pub mode: ExecutionMode,
    pub channel_size: Vec<AtomicUsize>,
}

/// Public Interfaces for the Scheduler
impl Scheduler {
    pub fn new(num_txns: usize, dependencies: &Vec<Vec<u64>>, gas_estimates: &Vec<u64>, concurrency_level: &usize, mode: ExecutionMode, map: HashMap<u16, (bool, MyMut<bool>)>) -> Self {

        let mut mapping : Vec<usize> = (0..num_txns +1).map(|_| usize::MAX).collect();
        let mut incoming: Vec<usize> = (0..num_txns).map(|_| 0).collect();
        let mut children: Vec<Vec<TxnIndex>> = (0..num_txns).map(|_|{Vec::new()}).collect();
        let mut heap = SkipSet::new();
        let hint_graph : Vec<CachePadded<Vec<TxnIndex>>>  = (0..num_txns)
            .map(|idx| CachePadded::new(dependencies[idx].iter().map(|v| *v as TxnIndex).collect()))
            .collect();

        mapping[0] = 0;
        let mut bottomlevels: Vec<TxnIndex> = vec![0; num_txns];
        for i in (0..num_txns).rev() {
            for node in &*hint_graph[i] {

                incoming[i] += 1;
                children[*node].push(i);
                if bottomlevels[*node] < bottomlevels[i] + 1 {
                    bottomlevels[*node] = bottomlevels[i] + 1;
                }
            }
            if hint_graph[i].is_empty() {
                heap.insert(Task {bottomlevel: bottomlevels[i], index: i});
            }
        }

        Self {
            num_txns,
            execution_idx: AtomicUsize::new(0),
            validation_idx: AtomicU64::new(0),
            commit_state: Mutex::new((0, 0)),
            done_marker: AtomicBool::new(false),
            opt_txn_dependency: DashSet::with_capacity(num_txns),

            txn_dependency: (0..num_txns)
                .map(|_| CachePadded::new(RwLock::new(HashMap::new())))
                .collect(),
            txn_status: (0..num_txns)
                .map(|_| {
                    CachePadded::new((
                        RwLock::new(ExecutionStatus::ReadyToExecute(0, None)),
                        RwLock::new(ValidationStatus::new()),
                    ))
                })
                .collect(),
            hint_graph,
            // use_hints: (x==1),
            use_hints: true,
            //change capacity later
            on_thread: Vec::with_capacity(num_txns),
            // round_number: AtomicUsize::new(0),
            my_last_stop: (0..(*concurrency_level-1))
                .map(|i| Mutex::new(i))
                .collect(),
            max: Mutex::new(42),
            concurrency_level: *concurrency_level,
            gas_estimates: CachePadded::new(gas_estimates.clone()),
            sched_lock: AtomicUsize::new(usize::MAX),
            condvars: (0..(*concurrency_level))
                .map(|_| (Mutex::new(false),Condvar::new()))
                .collect(),
            heap,
            finish_time: (0..num_txns +1).map(|_| Mutex::new(0)).collect(),
            mapping: RwLock::new(mapping),
            incoming: Mutex::new(incoming),
            children,
            end_comp: Mutex::new((0..*concurrency_level).map(|_| 0).collect()),
            bottomlevels,
            nscheduled: AtomicUsize::new(0),
            channels: (0..*concurrency_level).map(|_| tokio::sync::mpsc::unbounded_channel()).map(|(a,b)| (a, Mutex::new(b))).collect(),
            priochannels: (0..*concurrency_level).map(|_| tokio::sync::mpsc::unbounded_channel()).map(|(a,b)| (a, Mutex::new(b))).collect(),
            valock: MyMut::new(false),
            sig_val_idx: AtomicUsize::new(0),
            prologue_map: map,
            prologue_index: AtomicU16::new(0),
            mode,
            channel_size: (0..*concurrency_level).map(|_| AtomicUsize::new(0)).collect()
        }
    }

    /// If successful, returns Some(TxnIndex), the index of committed transaction.
    /// The current implementation has one dedicated thread to try_commit.
    pub fn try_commit(&self) -> Option<TxnIndex> {
        let mut commit_state_mutex = self.commit_state.lock();
        let commit_state = commit_state_mutex.deref_mut();
        let (commit_idx, commit_wave) = (&mut commit_state.0, &mut commit_state.1);
        // //println!("commit idx =  {}", *commit_idx);

        if *commit_idx == self.num_txns
        {
            if !matches!(self.mode, ExecutionMode::Pythia_Sig) || self.sig_val_idx.load(Ordering::Acquire) >= self.num_txns
            {
                // All txns have been committed, the parallel execution can finish.
                self.done_marker.store(true, Ordering::SeqCst);
                for i in 0..self.concurrency_level {
                    let (lock, cvar) = &self.condvars[i];
                    *lock.lock() = true;
                    cvar.notify_one();
                }
            }
            return None;
        }

        if let Some(validation_status) = self.txn_status[*commit_idx].1.try_read() {
            // Acquired the validation status read lock.
            if let Some(status) = self.txn_status[*commit_idx].0.try_upgradable_read() {
                // Acquired the execution status read lock, which can be upgrade to write lock if necessary.
                if let ExecutionStatus::Executed(incarnation) = *status {
                    // Status is executed and we are holding the lock.

                    // Note we update the wave inside commit_state only with max_triggered_wave,
                    // since max_triggered_wave records the new wave when validation index is
                    // decreased thus affecting all later txns as well,
                    // while required_wave only records the new wave for one single txn.
                    *commit_wave = max(*commit_wave, validation_status.max_triggered_wave);
                    if let Some(validated_wave) = validation_status.maybe_max_validated_wave {
                        if validated_wave >= max(*commit_wave, validation_status.required_wave) {
                            let mut status_write = RwLockUpgradableReadGuard::upgrade(status);
                            // Upgrade the execution status read lock to write lock.
                            // Can commit.
                            *status_write = ExecutionStatus::Committed(incarnation);
                            *commit_idx += 1;
                            return Some(*commit_idx - 1);
                        }
                    }
                }
            }
        }
        None
    }

    #[cfg(test)]
    /// Return the TxnIndex and Wave of current commit index
    pub fn commit_state(&self) -> (usize, u32) {
        let commit_state = self.commit_state.lock();
        (commit_state.0, commit_state.1)
    }

    /// Try to abort version = (txn_idx, incarnation), called upon validation failure.
    /// When the invocation manages to update the status of the transaction, it changes
    /// Executed(incarnation) => Aborting(incarnation), it returns true. Otherwise,
    /// returns false. Since incarnation numbers never decrease, this also ensures
    /// that the same version may not successfully abort more than once.
    pub fn try_abort(&self, txn_idx: TxnIndex, incarnation: Incarnation) -> bool {
        // lock the execution status.
        // Note: we could upgradable read, then upgrade and write. Similar for other places.
        // However, it is likely an overkill (and overhead to actually upgrade),
        // while unlikely there would be much contention on a specific index lock.
        let mut status = self.txn_status[txn_idx].0.write();

        if *status == ExecutionStatus::Executed(incarnation) {
            *status = ExecutionStatus::Aborting(incarnation);
            true
        } else {
            false
        }
    }

    /// Return the next task for the thread.
    pub fn next_task(&self, commiting: bool, profiler: &mut Profiler, thread_id: usize, local_flag: &mut bool, defaultChannel: &mut UnboundedReceiver<TxnIndex>, prioChannel: &mut UnboundedReceiver<TxnIndex>, finished_val_flag: &mut bool, ever_ran_anything: &mut bool) -> SchedulerTask {
        //profiler.start_timing(&"try_exec".to_string());
        //profiler.start_timing(&"exec_crit".to_string());
        //profiler.start_timing(&"try_val".to_string());

        // //println!("nscheduled = {}", self.nscheduled.load(Ordering::SeqCst));
        if !*local_flag && *finished_val_flag && self.done() {
            return SchedulerTask::Done;
        }
        /* This should only happen once to calculate the bottomlevels */
        // //println!("SCHED_SETUP");

        if *local_flag && (*ever_ran_anything || thread_id == 0) && self.nscheduled.load(Ordering::SeqCst) < self.num_txns {
            let mut just_scheduled = false;

            if self.channel_size[thread_id].load(Ordering::Relaxed) < 10  {
                if let Ok(_) = self.sched_lock.compare_exchange(usize::MAX, thread_id, Ordering::SeqCst, Ordering::SeqCst) {
                    //profiler.start_timing(&"newScheduler".to_string());
                    // //println!("In here");
                    // //println!("Thread id {thread_id} scheduling chunk at {:?}", SystemTime::now().duration_since(UNIX_EPOCH).expect("anything").as_millis());
                    self.sched_next_chunk(profiler);
                    just_scheduled = true;
                    let start = SystemTime::now();
                    let since_the_epoch = start
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards");
                    println!("{:?}", since_the_epoch);
                    println!("bla scheduling {:?}", since_the_epoch.as_millis());
                    //profiler.end_timing(&"newScheduler".to_string());
                }
            }


            //profiler.start_timing(&"SCHEDULING".to_string());

            if !*finished_val_flag {
                if let Some((version_to_validate, guard)) = self.try_validate_next_version(finished_val_flag) {
                    // //println!("validate: {:?}", version_to_validate);
                    let val = SchedulerTask::ValidationTask(version_to_validate, guard);
                    //profiler.end_timing(&"try_val".to_string());
                    //profiler.end_timing(&"SCHEDULING".to_string());
                    if just_scheduled {
                        // Release
                        self.sched_lock.store(usize::MAX, Ordering::SeqCst);
                    }
                    return val;
                }
            }

            let ex = self.try_exec(thread_id, profiler, commiting, defaultChannel, prioChannel);
            if !matches!(ex, NoTask) {
                *ever_ran_anything = true;
            }
            //profiler.end_timing(&"SCHEDULING".to_string());
            if just_scheduled {
                // Release
                self.sched_lock.store(usize::MAX, Ordering::SeqCst);
            }
            return ex;
        } else {
            *local_flag = false;
            *ever_ran_anything = true;
            //profiler.start_timing(&"SCHEDULING".to_string());

            //here subtract 1 so that thread_id == 1 -> thread_buffer[0]
            // //println!("Stuck here");
            // profiler.start_timing(&"peek_lock_ex".to_string());
            // profiler.end_timing(&"peek_lock_ex".to_string());

            // let max = self.thread_buffer.clone().into_iter().map(|btreeset|{
            //     btreeset.len()}).max().unwrap();
            // *self.max.lock() = max;
            //let (idx_to_validate, _) = Self::unpack_validation_idx(self.validation_idx.load(Ordering::Acquire));
            // if my_len == 0 && idx_to_validate >= self.num_txns {
            //     return if self.done() {
            //         SchedulerTask::Done
            //     } else {
            //         if !commiting {
            //             //println!("STUCK HERE");
            //             hint::spin_loop();
            //         }
            //         SchedulerTask::NoTask
            //     };
            // }

            if !*finished_val_flag {
                if let Some((version_to_validate, guard)) = self.try_validate_next_version(finished_val_flag) {
                    // //println!("validate: {:?}", version_to_validate);
                    let val = SchedulerTask::ValidationTask(version_to_validate, guard);
                    //profiler.end_timing(&"SCHEDULING".to_string());
                    //profiler.end_timing(&"try_val".to_string());
                    return val;
                }
            }

            let ex = self.try_exec(thread_id, profiler, commiting, defaultChannel, prioChannel);
            //profiler.end_timing(&"SCHEDULING".to_string());
            return ex;
        }
    }

    fn sched_next_chunk(&self, profiler: &mut Profiler) {
        //println!("bla estimates {:?}", self.gas_estimates);
        //println!("bla deps {:?}", self.hint_graph);

        // let mut end_comp: Vec<usize> = vec![0; self.concurrency_level];
        let mut begin_time: usize;
        let mut best_begin_time: usize;
        let mut ready: usize;
        let mut best_proc: usize = usize::MAX;
        let mut counter: usize = 0;
        let mut mapping_lock = self.mapping.write();
        let mut end_comp_lock = self.end_comp.lock();
        let mut incoming_lock = self.incoming.lock();
        let mut chunks : Vec<usize> = (0..self.concurrency_level).map(|_| 0).collect();

        while let Some(ui) = self.heap.pop_front()
        {
            // //println!("bottomlevel = {} for idx {}", bottomlevels[ui.index], ui.index);
            /* Sort Pred[ui] in a non-decreasing order of finish times */
            let mut parents: Vec<Parent> = self.hint_graph[ui.index]
                .iter()
                .filter_map(|i| {
                    if self.is_executed(*i, true).is_some() {
                        return None;
                    }
                    else {
                        return Some(Parent { idx: *i, finish_time: *self.finish_time[*i].lock() });
                    }
                })
                .collect::<Vec<Parent>>();

            // parents.sort_by(|a, b| {
            //     if a.finish_time < b.finish_time {
            //         cmpOrdering::Less
            //     } else if  a.finish_time == b.finish_time {
            //         cmpOrdering::Equal
            //     } else {
            //         cmpOrdering::Greater
            //     }
            // });
            // //println!("{:?}", parents[0].idx);

            /* find the best proc for task */
            best_begin_time = usize::MAX;
            for proc in 0..self.concurrency_level {
                begin_time = end_comp_lock[proc];
                // //println!("begin_time = {}", begin_time);

                for dad in &parents {
                    // //println!("parent {}", dad.idx);
                    if mapping_lock[dad.idx] == proc {
                        ready = *self.finish_time[dad.idx].lock();
                        // //println!("ready = {} if same proc = {}",ready, proc);
                    } else {
                        ready = *self.finish_time[dad.idx].lock() +  (self.gas_estimates[dad.idx] as usize / 10);
                        // //println!("ready = {} if not same proc = {}", ready, proc);
                    }
                    begin_time = if begin_time < ready {ready} else {begin_time};
                }
                if best_begin_time > begin_time {
                    best_begin_time = begin_time;
                    best_proc = proc;
                }
            }
            mapping_lock[ui.index] = best_proc;
            // self.thread_buffer[best_proc].insert(ui.index);

            let (tx, rx) = &self.channels[best_proc];
            profiler.count(format!("go-to {}", best_proc.to_string()), 1);
            tx.send(ui.index);
            chunks[best_proc] += 1;
            // info!("Sent {} to {} ", ui.index, best_proc);

            let (lock,cvar) = &self.condvars[best_proc];
            *lock.lock() = true;
            cvar.notify_one();

            /* run time estimation*/
            end_comp_lock[best_proc] = best_begin_time + (self.gas_estimates[ui.index] as usize);
            // //println!("gas estimate = {}", self.gas_estimates[ui.index]);
            *self.finish_time[ui.index].lock() = end_comp_lock[best_proc];
            // let bottomlock = &*self.bottomlevels.lock();
            for child in &*self.children[ui.index] {
                incoming_lock[*child] -= 1;
                if incoming_lock[*child] == 0 {
                    self.heap.insert(Task {bottomlevel: self.bottomlevels[*child], index: *child});
                }
            }
            counter += 1;
            if counter >= 1000 {
                break
            }
        }
        println!("nschedule: {}", counter);
        self.nscheduled.fetch_add(counter, Ordering::SeqCst);

        let mut index: usize = 0;
        for chunk in chunks {
            self.channel_size[index].fetch_add(chunk, Ordering::Relaxed);
            index+=1;
        }
        //reset sched_lock
    }


    fn try_exec(&self, thread_id: usize, profiler: &mut Profiler, commiting: bool, defaultChannel: &mut UnboundedReceiver<TxnIndex>, prioChannel: &mut UnboundedReceiver<TxnIndex>) -> SchedulerTask {
        // info!("{} TRYIN TO EXEC", thread_id);

        if self.channel_size[thread_id].load(Ordering::Relaxed) <= 0 {
            return SchedulerTask::NoTask;
        }
        if let Ok(txn_to_exec) = prioChannel.try_recv() {

            //info!("PRIO Received {} on {}", txn_to_exec, thread_id);

            if let Some((version_to_execute, maybe_condvar)) =
                self.try_execute_next_version(profiler, txn_to_exec, thread_id)
            {
                //profiler.end_timing(&"try_exec".to_string());
                //profiler.end_timing(&"exec_crit".to_string());
                return SchedulerTask::ExecutionTask(version_to_execute, maybe_condvar);
            }
            else {
                self.channel_size[thread_id].fetch_sub(1, Ordering::Relaxed);
            }
        }

        if let Ok(txn_to_exec) = defaultChannel.try_recv() {
            //info!("Received {}", txn_to_exec);

            if let Some((version_to_execute, maybe_condvar)) =
                self.try_execute_next_version(profiler, txn_to_exec, thread_id)
            {
                // let my_global_idx_mutex = self.tglobalidx[thread_id -1].lock();
                // if localidx < *my_global_idx_mutex {
                //     //println!("AHAHAAHAAHAAHAHAHAHAHAHAHAHAHAHAHAHAHAHAHHAHAHAHAHAAHHAHAHAHAHA {} with local ={} and global = {} and at buffer {}", *txn_to_exec, localidx, my_global_idx_mutex, thread_id -1);
                // }
                // //println!("MY LOCAL IDX = {}, MY GLOBAL IDX = {} in buff {}", localidx, *my_global_idx_mutex, thread_id -1);
                // drop(my_global_idx_mutex);
                //profiler.end_timing(&"try_exec".to_string());
                //profiler.end_timing(&"exec_crit".to_string());

                return SchedulerTask::ExecutionTask(version_to_execute, maybe_condvar);
            }
            else {
                self.channel_size[thread_id].fetch_sub(1, Ordering::Relaxed);
            }
        }
        return SchedulerTask::NoTask;
    }


    fn add_dependency(
        &self,
        txn_idx: TxnIndex,
        dep_txn_idx: TxnIndex,
        idx: usize,
        profiler: &mut Profiler,
    ) -> bool {
        let thread_id = idx;
        //info!("Try to acquire txn_dependency [{}] lock on thread_id {}",dep_txn_idx,thread_id);
        let mut stored_deps = self.txn_dependency[dep_txn_idx].write();
        //info!("acquired txn_dependency [{}] lock on thread_id {}",dep_txn_idx,thread_id);

        if self.is_executed(dep_txn_idx, true).is_some() {
            return false;
        }
        if stored_deps.contains_key(&txn_idx) {
            return true;
        }
        // println!("inserting txn_idx {} as a dependency on {}, at my thread_id {}", txn_idx, dep_txn_idx, thread_id);
        stored_deps.insert(txn_idx, thread_id);
        true
    }

    /// When a txn depends on another txn, adds it to the dependency list of the other txn.
    /// Returns true if successful, or false, if the dependency got resolved in the meantime.
    /// If true is returned, Scheduler guarantees that later (dep_txn_idx will finish execution)
    /// transaction txn_idx will be resumed, and corresponding execution task created.
    /// If false is returned, it is caller's responsibility to repeat the read that caused the
    /// dependency and continue the ongoing execution of txn_idx.
    pub fn wait_for_dependency(
        &self,
        txn_idx: TxnIndex,
        dep_txn_idx: TxnIndex,
    ) -> Option<DependencyCondvar> {
        // Note: Could pre-check that txn dep_txn_idx isn't in an executed state, but the caller
        // usually has just observed the read dependency.

        // Create a condition variable associated with the dependency.
        panic!("This should not be called");
        let thread_id = RAYON_EXEC_POOL.lock().unwrap().current_thread_index().unwrap();
        let dep_condvar = Arc::new((Mutex::new(false), Condvar::new()));


        {
            if self.is_executed(dep_txn_idx, true).is_some() {
                // Current status of dep_txn_idx is 'executed', so the dependency got resolved.
                // To avoid zombie dependency (and losing liveness), must return here and
                // not add a (stale) dependency.

                // Note: acquires (a different, status) mutex, while holding (dependency) mutex.
                // Only place in scheduler where a thread may hold >1 mutexes, hence, such
                // acquisitions always happens in the same order (this function), may not deadlock.

                return None;
            }

            self.suspend(txn_idx, dep_condvar.clone());

            // Safe to add dependency here (still holding the lock) - finish_execution of txn
            // dep_txn_idx is guaranteed to acquire the same lock later and clear the dependency.
            self.txn_dependency[dep_txn_idx].write().insert(txn_idx, thread_id);
        }

        Some(dep_condvar)
    }

    pub fn finish_validation(&self, txn_idx: TxnIndex, wave: Wave) {
        let mut validation_status = self.txn_status[txn_idx].1.write();
        validation_status.maybe_max_validated_wave = Some(
            validation_status
                .maybe_max_validated_wave
                .map_or(wave, |prev_wave| max(prev_wave, wave)),
        );
    }

    /// After txn is executed, schedule its dependencies for re-execution.
    /// If revalidate_suffix is true, decrease validation_idx to schedule all higher transactions
    /// for (re-)validation. Otherwise, in some cases (if validation_idx not already lower),
    /// return a validation task of the transaction to the caller (otherwise NoTask).
    pub fn finish_execution(
        &self,
        txn_idx: TxnIndex,
        incarnation: Incarnation,
        revalidate_suffix: bool,
        profiler: &mut Profiler,
        thread_id: usize,
    ) -> SchedulerTask {
        // Note: It is preferable to hold the validation lock throughout the finish_execution,
        // in particular before updating execution status. The point was that we don't want
        // any validation to come before the validation status is correspondingly updated.
        // It may be possible to make work more granularly, but shouldn't make performance
        // difference and like this correctness argument is much easier to see, in fact also
        // the reason why we grab write lock directly, and never release it during the whole function.
        // So even validation status readers have to wait if they somehow end up at the same index.
        //info!("started finish execution of {} on thread_id {}", txn_idx, thread_id);

        self.set_executed_status(txn_idx, incarnation);
        //info!("set executed status in finish execution of {}", txn_idx);

        // Reduce finish time for next scheduler phase.
        {
            let mut finish_time_lock = self.finish_time[thread_id].lock();
            let runtimecost = self.gas_estimates[txn_idx] as usize;
            if *finish_time_lock >= runtimecost
            {
                *finish_time_lock = *finish_time_lock - runtimecost;
            }
        }
        self.channel_size[thread_id].fetch_sub(1, Ordering::Relaxed);

        //info!("acquired txn_dependency_lock in finish execution of {}", txn_idx);
        //info!("{:?} txn_idx = {}",stored_deps,txn_idx);

        //println!("stored_deps of txn_idx {} : {:?}", txn_idx, stored_deps);
        {
            let mut stored_deps = self.txn_dependency[txn_idx].read();
            for (txn, target) in stored_deps.iter() {
                if self.is_executed(*txn, true).is_none() {
                    &self.priochannels[*target].0.send(*txn);
                    profiler.count_one("prio-queue".to_string());
                    self.channel_size[thread_id].fetch_add(1, Ordering::Relaxed);
                }
            }

            /*let read_lock = self.txn_dependency[txn_idx].read();
            for (txn, target) in read_lock.iter() {
                let mut ready = true;
                for tx in self.hint_graph[*txn].iter() {
                    let status = self.txn_status[*tx].0.read();
                    match *status {
                        ExecutionStatus::Executed(incarnation) => Some(incarnation),
                        ExecutionStatus::Executing(incarnation) => Some(incarnation),
                        ExecutionStatus::Committed(incarnation) => Some(incarnation),
                        _ =>  {
                            ready = false;
                            break;
                        },
                    };
                }
                if ready {
                    println!("rdy {}", txn);
                    &self.priochannels[*target].0.send(*txn);
                }
                else {
                    println!("not rdy {}", txn);
                }
            }*/
        }

        /*
        // Mark dependencies as resolved and find the minimum index among them.
        let min_dep = txn_deps
            .into_iter()
            .map(|dep| {
                // Mark the status of dependencies as 'ReadyToExecute' since dependency on
                // transaction txn_idx is now resolved.
                self.resume(dep);

                dep
            })
            .min();


        if let Some(execution_target_idx) = min_dep {
            // Decrease the execution index as necessary to ensure resolved dependencies
            // get a chance to be re-executed.
            self.execution_idx
                .fetch_min(execution_target_idx, Ordering::SeqCst);
        }
        */
        /*
        let (cur_val_idx, cur_wave) =
            Self::unpack_validation_idx(self.validation_idx.load(Ordering::Acquire));
        */

        // If validation_idx is already lower than txn_idx, all required transactions will be
        // considered for validation, and there is nothing to do.
        /*
        if cur_val_idx > txn_idx {
            if revalidate_suffix  {
                // The transaction execution required revalidating all higher txns (not
                // only itself), currently happens when incarnation writes to a new path
                // (w.r.t. the write-set of its previous completed incarnation).
                if let Some(wave) = self.decrease_validation_idx(txn_idx) {
                    // Under lock, current wave monotonically increasing, can simply write.
                    validation_status.max_triggered_wave = wave;
                }
            } else {
                // Only transaction txn_idx requires validation. Return validation task
                // back to the caller.
                // Under lock, current wave is monotonically increasing, can simply write.
                validation_status.required_wave = cur_wave;
                return SchedulerTask::ValidationTask((txn_idx, incarnation), cur_wave);
            }
        }
        */
        // info!("finished execution of {} on thread id {}", txn_idx, thread_id);

        SchedulerTask::NoTask
    }

    /// Finalize a validation task of version (txn_idx, incarnation). In some cases,
    /// may return a re-execution task back to the caller (otherwise, NoTask).
    pub fn finish_abort(&self, txn_idx: TxnIndex, incarnation: Incarnation, profiler: &mut Profiler, thread_id: usize) -> SchedulerTask {
        // Similar reason as in finish_execution to hold the validation lock throughout the
        // function. Also note that we always lock validation status before execution status
        // which is good to have a fixed order to avoid potential deadlocks.
        let mut validation_status = self.txn_status[txn_idx].1.write();
        self.set_aborted_status(txn_idx, incarnation);

        // Schedule higher txns for validation, could skip txn_idx itself (needs to be
        // re-executed first), but used to couple with the locked validation status -
        // should never attempt to commit until validation status is updated.
        if let Some(wave) = self.decrease_validation_idx(txn_idx) {
            // Under lock, current wave monotonically increasing, can simply write.
            validation_status.max_triggered_wave = wave;
        }

        // txn_idx must be re-executed, and if execution_idx is lower, it will be.
        if self.execution_idx.load(Ordering::Acquire) > txn_idx {
            // Optimization: execution_idx is higher than txn_idx, but decreasing it may
            // lead to wasted work for all indices between txn_idx and execution_idx.
            // Instead, attempt to create a new incarnation and return the corresponding
            // re-execution task back to the caller. If incarnation fails, there is
            // nothing to do, as another thread must have succeeded to incarnate and
            // obtain the task for re-execution.
            if let Some((new_incarnation, maybe_condvar)) = self.try_incarnate(txn_idx, profiler, thread_id) {
                return SchedulerTask::ExecutionTask((txn_idx, new_incarnation), maybe_condvar);
            }
        }

        SchedulerTask::NoTask
    }
}

// fn check_index(localidx: &mut usize, my_global_idx_mutex: &aptos_infallible::MutexGuard<usize>) -> ControlFlow<()> {
//     if *localidx < **my_global_idx_mutex {
//         *localidx += 1;
//         return ControlFlow::Break(());
//     }
//     ControlFlow::Continue(())
// }

/// Public functions of the Scheduler
impl Scheduler {
    fn unpack_validation_idx(validation_idx: u64) -> (TxnIndex, Wave) {
        (
            (validation_idx & TXN_IDX_MASK) as TxnIndex,
            (validation_idx >> 32) as Wave,
        )
    }

    /// Decreases the validation index, adjusting the wave and validation status as needed.
    fn decrease_validation_idx(&self, target_idx: TxnIndex) -> Option<Wave> {
        if let Ok(prev_val_idx) =
            self.validation_idx
                .fetch_update(Ordering::Acquire, Ordering::SeqCst, |val_idx| {
                    let (txn_idx, wave) = Self::unpack_validation_idx(val_idx);
                    if txn_idx > target_idx {
                        // Pack into validation index.
                        Some((target_idx as u64) | ((wave as u64 + 1) << 32))
                    } else {
                        None
                    }
                })
        {
            let (_, wave) = Self::unpack_validation_idx(prev_val_idx);
            // Note that 'wave' is the previous wave value, and we must update it to 'wave + 1'.
            Some(wave + 1)
        } else {
            None
        }
    }

    /// Try and incarnate a transaction. Only possible when the status is
    /// ReadyToExecute(incarnation), in which case Some(incarnation) is returned and the
    /// status is (atomically, due to the mutex) updated to Executing(incarnation).
    /// An unsuccessful incarnation returns None. Since incarnation numbers never decrease
    /// for each transaction, incarnate function may not succeed more than once per version.
    fn try_incarnate<'a>(&self, txn_idx: TxnIndex, profiler: &mut Profiler, idx: usize) -> Option<(Incarnation, Option<DependencyCondvar>)> {
        if txn_idx >= self.txn_status.len() {
            return None;
        }

        let thread_id = idx;
        profiler.start_timing(&"schedule#1".to_string());

        // Note: we could upgradable read, then upgrade and write. Similar for other places.
        // However, it is likely an overkill (and overhead to actually upgrade),
        // while unlikely there would be much contention on a specific index lock.
        let mut status = self.txn_status[txn_idx].0.write();
        if let ExecutionStatus::ReadyToExecute(incarnation, maybe_condvar) = &*status {
            for dependency in &*self.hint_graph[txn_idx] {
                // unsafe {
                //     count += 1;
                //     if count % 100 == 0 {
                //         //println!("{}", count);
                //     }
                // }

                if self.is_executed(*dependency, true).is_none() {
                    if self.add_dependency(txn_idx, *dependency, thread_id, profiler) {
                        // profiler.count_one("addDep".to_string());
                        // // //println!("Popping from thread_buffer due to dependency");
                        // // //println!("before popping {:?}, index {}", self.thread_buffer[thread_id-1], thread_id -1);
                        // let status = self.txn_status[*dependency].0.read();
                        // if let ExecutionStatus::Executed(_) = *status {
                        //     continue;
                        // }
                        // profiler.start_timing(&"pop_lock_ex".to_string());
                        // let value = self.thread_buffer[thread_id-1].lock().remove(&txn_idx);
                        // profiler.end_timing(&"pop_lock_ex".to_string());
                        // //println!("Popped {} = {} from thread_buffer [{}] due to dependency from {}", txn_idx, value, thread_id - 1, *dependency);
                        return None;
                    }
                }
            }

            let ret = (*incarnation, maybe_condvar.clone());
            *status = ExecutionStatus::Executing(*incarnation);
            profiler.end_timing(&"schedule#1".to_string());



            Some(ret)
        } else {
            None
        }
    }

    /// If the status of transaction is Executed(incarnation), returns Some(incarnation),
    /// Useful to determine when a transaction can be validated, and to avoid a race in
    /// dependency resolution.
    /// If include_committed is true (which is when calling from wait_for_dependency),
    /// then committed transaction is also considered executed (for dependency resolution
    /// purposes). If include_committed is false (which is when calling from
    /// try_validate_next_version), then we are checking if a transaction may be validated,
    /// and a committed (in between) txn does not need to be scheduled for validation -
    /// so can return None.
    fn is_executed(&self, txn_idx: TxnIndex, include_committed: bool) -> Option<Incarnation> {
        if txn_idx >= self.txn_status.len() {
            return None;
        }

        let status = self.txn_status[txn_idx].0.read();
        match *status {
            ExecutionStatus::Executed(incarnation) => Some(incarnation),
            ExecutionStatus::Committed(incarnation) => {
                if include_committed {
                    // Committed txns are also considered executed for dependency resolution purposes.
                    Some(incarnation)
                } else {
                    // Committed txns do not need to be scheduled for validation in try_validate_next_version.
                    None
                }
            },
            _ => None,
        }
    }

    /// Grab an index to try and validate next (by fetch-and-incrementing validation_idx).
    /// - If the index is out of bounds, return None (and invoke a check of whethre
    /// all txns can be committed).
    /// - If the transaction is ready for validation (EXECUTED state), return the version
    /// to the caller.
    /// - Otherwise, return None.
    fn try_validate_next_version(&self, finished_val_flag: &mut bool) -> Option<(Version, Wave)> {
        if let Ok(ref mut mutex) = self.valock.try_lock() {
            let (mut idx_to_validate, mut wave) =
                Self::unpack_validation_idx(self.validation_idx.load( Ordering::SeqCst));

            if idx_to_validate >= self.num_txns {
                // Avoid pointlessly spinning, and give priority to other threads that may
                // be working to finish the remaining tasks.
                hint::spin_loop();
                *finished_val_flag = true;
                return None;
            }

            // If incarnation was last executed, and thus ready for validation,
            // return version and wave for validation task, otherwise None.
            let executed = self.is_executed(idx_to_validate, false);
            if executed == None {
                return None
            }
            (idx_to_validate, wave) = Self::unpack_validation_idx(self.validation_idx.fetch_add(1, Ordering::SeqCst));
            // info!("validate {}", idx_to_validate);
            executed.map(|incarnation| ((idx_to_validate, incarnation), wave))
        }
        else {
            None
        }
    }

    /// Grab an index to try and execute next (by fetch-and-incrementing execution_idx).
    /// - If the index is out of bounds, return None (and invoke a check of whethre
    /// all txns can be committed).
    /// - If the transaction is ready for execution (ReadyToExecute state), attempt
    /// to create the next incarnation (should happen exactly once), and if successful,
    /// return the version to the caller for the corresponding ExecutionTask.
    /// - Otherwise, return None.
    fn try_execute_next_version(&self, profiler: &mut Profiler, idx_to_execute: TxnIndex, thread_id: usize) -> Option<(Version, Option<DependencyCondvar>)> {
        // let (idx_to_validate, _) =
        //     Self::unpack_validation_idx(self.validation_idx.load(Ordering::Acquire));


        if idx_to_execute >= self.num_txns {
            return None;
        }

        // If successfully incarnated (changed status from ready to executing),
        // return version for execution task, otherwise None.
        self.try_incarnate(idx_to_execute, profiler, thread_id)
            .map(|(incarnation, maybe_condvar)| ((idx_to_execute, incarnation), maybe_condvar))
    }

    /// Put a transaction in a suspended state, with a condition variable that can be
    /// used to wake it up after the dependency is resolved.
    fn suspend(&self, txn_idx: TxnIndex, dep_condvar: DependencyCondvar) {
        let mut status = self.txn_status[txn_idx].0.write();

        if let ExecutionStatus::Executing(incarnation) = *status {
            *status = ExecutionStatus::Suspended(incarnation, dep_condvar);
        } else {
            unreachable!();
        }
    }

    /// When a dependency is resolved, mark the transaction as ReadyToExecute with an
    /// incremented incarnation number.
    /// The caller must ensure that the transaction is in the Suspended state.
    fn resume(&self, txn_idx: TxnIndex) {
        let mut status = self.txn_status[txn_idx].0.write();

        if let ExecutionStatus::Suspended(incarnation, dep_condvar) = &*status {
            *status = ExecutionStatus::ReadyToExecute(*incarnation, Some(dep_condvar.clone()));
        }
    }

    /// Set status of the transaction to Executed(incarnation).
    fn set_executed_status(&self, txn_idx: TxnIndex, incarnation: Incarnation) {
        let mut status = self.txn_status[txn_idx].0.write();

        // Only makes sense when the current status is 'Executing'.
        debug_assert!(*status == ExecutionStatus::Executing(incarnation));

        *status = ExecutionStatus::Executed(incarnation);
    }

    /// After a successful abort, mark the transaction as ready for re-execution with
    /// an incremented incarnation number.
    fn set_aborted_status(&self, txn_idx: TxnIndex, incarnation: Incarnation) {
        let mut status = self.txn_status[txn_idx].0.write();

        // Only makes sense when the current status is 'Aborting'.
        debug_assert!(*status == ExecutionStatus::Aborting(incarnation));

        *status = ExecutionStatus::ReadyToExecute(incarnation + 1, None);
    }

    /// Checks whether the done marker is set. The marker can only be set by 'try_commit'.
    fn done(&self) -> bool {
        self.done_marker.load(Ordering::Acquire)
    }
}
