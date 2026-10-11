//! Service ownership, recovery, and readiness observations.

use super::{CommittedCommand, ServiceAdapterError, ServiceError, ServiceOptions, ServiceState};
use crate::application::{ApplicationEntry, ApplicationWorker, DurableApplication};
use crate::pipelined::{PipelineProgress, ThreadedPipelinedRaftNode};
use crate::{DurableRaftNode, DurableRaftNodeStorage};
use rafter::{LogIndex, NodeConfig, Output, SnapshotChunkSource};
use rafter_storage::{RaftHardStateStore, RaftLogSegment, RaftSnapshotStore};
use std::collections::VecDeque;

/// One owner coordinating the two durable fences and bounded dispatch.
///
/// See the [module contract](super) for memory, scheduling, transport, and
/// application obligations. This type deliberately does not implement the
/// synchronous `PersistedRaftRuntime` contract while work can be pending.
pub struct DurableService<H, L, S, T: ApplicationEntry, A: DurableApplication<T>> {
    pub(super) node: ThreadedPipelinedRaftNode<H, L, S>,
    pub(super) application: Option<ApplicationWorker<T, A>>,
    pub(super) adapter: Box<dyn FnMut(CommittedCommand) -> Result<T, ServiceAdapterError> + Send>,
    pub(super) outputs: VecDeque<Output>,
    pub(super) pending: VecDeque<T>,
    pub(super) dispatched: LogIndex,
    pub(super) snapshot_floor: Option<LogIndex>,
    pub(super) options: ServiceOptions,
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
        let node = ThreadedPipelinedRaftNode::start(node, options.persistence)
            .map_err(|error| ServiceError::Start(error.into_parts().0))?;
        let application = ApplicationWorker::start(application, options.application, || {})
            .map_err(ServiceError::Start)?;
        Ok(Self {
            node,
            application: Some(application),
            adapter: Box::new(adapter),
            outputs,
            pending: VecDeque::new(),
            dispatched: floor,
            snapshot_floor: None,
            options,
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
