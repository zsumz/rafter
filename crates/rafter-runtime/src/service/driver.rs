//! Service ownership, recovery, and readiness observations.

use super::{CommittedCommand, ServiceAdapterError, ServiceError, ServiceOptions, ServiceState};
use crate::application::{ApplicationEntry, ApplicationWorker, DurableApplication};
use crate::pipelined::{PipelineProgress, ThreadedPipelinedRaftNode};
use crate::{DurableRaftNode, DurableRaftNodeStorage};
use rafter::{LogIndex, NodeConfig, Output, SnapshotChunkSource};
use rafter_storage::{RaftHardStateStore, RaftLogSegment, RaftSnapshotStore};
use std::{collections::VecDeque, sync::Arc};

/// One owner coordinating the two durable fences and bounded dispatch.
///
/// See the [module contract](super) for memory, scheduling, transport, and
/// application obligations. This type deliberately does not implement the
/// synchronous `PersistedRaftRuntime` contract while work can be pending.
pub struct DurableService<H, L, S, T: ApplicationEntry, A: DurableApplication<T>> {
    pub(super) node: ThreadedPipelinedRaftNode<H, L, S>,
    pub(super) persistence_telemetry: crate::pipelined::PersistenceWorkerTelemetry,
    pub(super) application: Option<ApplicationWorker<T, A>>,
    pub(super) adapter: Box<dyn FnMut(CommittedCommand) -> Result<T, ServiceAdapterError> + Send>,
    pub(super) outputs: VecDeque<Output>,
    pub(super) pending: VecDeque<T>,
    pub(super) dispatched: LogIndex,
    pub(super) snapshot_floor: Option<LogIndex>,
    pub(super) options: ServiceOptions,
    pub(super) wake: Arc<dyn Fn() + Send + Sync>,
    pub(super) recovering: bool,
    pub(super) draining: bool,
    pub(super) terminal: Option<Terminal>,
}

impl<H, L, S, T, A> DurableService<H, L, S, T, A>
where
    H: RaftHardStateStore + Send + 'static,
    L: RaftLogSegment + Send + 'static,
    S: RaftSnapshotStore + SnapshotChunkSource + Send + 'static,
    T: ApplicationEntry,
    A: DurableApplication<T>,
{
    /// Recovers Raft using the application's durable floor and starts workers.
    ///
    /// Recovery outputs remain owned here until polling applies them. If Raft's
    /// snapshot is newer than application state, snapshot installation precedes
    /// replay. `adapter` must preserve the command range and application meaning;
    /// its allocation estimates include outcomes and any shared backing storage.
    ///
    /// # Errors
    /// Returns storage, floor, option, or worker-start failures. Construction
    /// consumes the stores; after failure reopen them from durable media.
    pub fn recover(
        config: NodeConfig,
        storage: DurableRaftNodeStorage<H, L, S>,
        application: A,
        options: ServiceOptions,
        adapter: impl FnMut(CommittedCommand) -> Result<T, ServiceAdapterError> + Send + 'static,
    ) -> Result<Self, ServiceError> {
        Self::recover_with_wake(config, storage, application, options, adapter, || {})
    }

    /// Recovers with notifications from both workers, including after restarts.
    ///
    /// `wake` may run concurrently on either worker thread. It must be bounded,
    /// nonblocking, non-panicking, and must not reenter this owner. It remains
    /// installed across snapshot handoff and local compaction. A retained,
    /// coalescing notification prevents a completion between polling and waiting
    /// from being lost. Poll [`Self::poll_next`] until `ServicePoll::Pending`
    /// before waiting; after local input or snapshot resumption poll again.
    /// The caller schedules its own consensus ticks and transport readiness.
    ///
    /// # Errors
    /// Has the same consuming recovery failure contract as [`Self::recover`].
    pub fn recover_with_wake(
        config: NodeConfig,
        storage: DurableRaftNodeStorage<H, L, S>,
        application: A,
        options: ServiceOptions,
        adapter: impl FnMut(CommittedCommand) -> Result<T, ServiceAdapterError> + Send + 'static,
        wake: impl Fn() + Send + Sync + 'static,
    ) -> Result<Self, ServiceError> {
        if options.max_proposals == 0 || options.max_proposal_bytes == 0 {
            return Err(ServiceError::Start(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "proposal limits must be nonzero",
            )));
        }
        let floor = application.applied_through();
        let recovered = DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
            config,
            storage.hard_state_store,
            storage.log_segment,
            storage.snapshot_store,
            floor,
        )
        .map_err(ServiceError::Recovery)?;
        let (node, outputs) = recovered.into_parts();
        if floor > node.commit_index() {
            return Err(ServiceError::ApplicationFloor {
                expected: node.commit_index(),
                actual: floor,
            });
        }
        let mut outputs = VecDeque::from(outputs);
        if let Some(snapshot) = node
            .snapshot()
            .filter(|s| s.metadata.last_included_index > floor)
        {
            outputs.push_front(Output::ApplySnapshot {
                snapshot: snapshot.clone(),
            });
        }
        let wake: Arc<dyn Fn() + Send + Sync> = Arc::new(wake);
        let persistence_wake = Arc::clone(&wake);
        let node =
            ThreadedPipelinedRaftNode::start_with_wake(node, options.persistence, move || {
                persistence_wake();
            })
            .map_err(|error| ServiceError::Start(error.into_parts().0))?;
        let application_wake = Arc::clone(&wake);
        let application = ApplicationWorker::start(application, options.application, move || {
            application_wake();
        })
        .map_err(ServiceError::Start)?;
        Ok(Self {
            node,
            persistence_telemetry: Vec::new(),
            application: Some(application),
            adapter: Box::new(adapter),
            outputs,
            pending: VecDeque::new(),
            dispatched: floor,
            snapshot_floor: None,
            options,
            wake,
            recovering: true,
            draining: false,
            terminal: None,
        })
    }

    /// Returns the current admission/lifecycle state without consuming events.
    #[must_use]
    pub fn state(&self) -> ServiceState {
        if self.terminal == Some(Terminal::Failed) {
            ServiceState::Failed
        } else if self.terminal == Some(Terminal::Stopped) {
            ServiceState::Stopped
        } else if self.snapshot_floor.is_some() {
            ServiceState::InstallingSnapshot
        } else if self.draining {
            ServiceState::Draining
        } else if self.recovering {
            ServiceState::Recovering
        } else if self.node.persistence_pending()
            || !self.outputs.is_empty()
            || !self.pending.is_empty()
            || self
                .application
                .as_ref()
                .is_some_and(|app| !app.is_accepting())
        {
            ServiceState::Busy
        } else {
            ServiceState::Ready
        }
    }

    /// Returns the runtime when persistence does not own it, for inspection or
    /// reading stored snapshot chunks. No mutable stepping escape hatch exists.
    #[must_use]
    pub fn ready_node(&self) -> Option<&DurableRaftNode<H, L, S>> {
        self.node.ready_node()
    }

    /// Returns the distinct consensus progress positions.
    #[must_use]
    pub const fn progress(&self) -> PipelineProgress {
        self.node.progress()
    }

    /// Borrows diagnostics from the last consumed successful worker completion.
    ///
    /// Enable collection through [`crate::pipelined::PersistenceWorkerOptions`].
    /// Empty when disabled or before a completion is consumed. Counters are
    /// cumulative for this persistence thread and reset with a recovered service.
    /// They exclude recovery and synchronous owner-thread work. A pending or
    /// failed operation may leave this sample stale; it establishes no durability
    /// or health guarantee. Observation polls no work and invokes no callbacks.
    /// The last sample remains available after shutdown. Disable diagnostics for
    /// timing runs, as specified by the worker options.
    #[must_use]
    pub fn persistence_telemetry(&self) -> &[(&'static str, rafter_storage::telemetry::Metric)] {
        &self.persistence_telemetry
    }

    /// Returns the last dispatched application position (not its durable floor).
    #[must_use]
    pub const fn dispatched_through(&self) -> LogIndex {
        self.dispatched
    }

    /// Returns the application durable floor, or `None` while its store is outside
    /// the owner for snapshot installation or after shutdown.
    #[must_use]
    pub fn durable_through(&self) -> Option<LogIndex> {
        self.application
            .as_ref()
            .map(ApplicationWorker::durable_through)
    }

    pub(super) fn idle(&self) -> bool {
        !self.node.persistence_pending()
            && self.outputs.is_empty()
            && self.pending.is_empty()
            && self.application.as_ref().is_some_and(|app| !app.is_busy())
    }

    pub(super) fn poison<E>(&mut self, error: ServiceError) -> Result<E, ServiceError> {
        self.terminal = Some(Terminal::Failed);
        Err(error)
    }
}

impl<H, L, S, T: ApplicationEntry, A: DurableApplication<T>> std::fmt::Debug
    for DurableService<H, L, S, T, A>
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DurableService")
            .field("output_count", &self.outputs.len())
            .field("dispatched", &self.dispatched)
            .field("terminal", &self.terminal)
            .finish_non_exhaustive()
    }
}

/// Irreversible terminal lifecycle, independent of drain and recovery progress.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Terminal {
    Stopped,
    Failed,
}
