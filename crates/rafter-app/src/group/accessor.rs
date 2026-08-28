//! Read-only views of a group, plus the two waiter takes.
//!
//! Nothing here steps the runtime or moves the state machine's applied
//! floor. These are the values a driver reads between steps to correlate
//! its own clients and to decide whether this group may still be driven.

use super::{
    ErrorCause, GroupFatalState, LocalProposalId, NodeId, PersistedRaftRuntime, PoisonedWaiters,
    RaftGroup, ReadId,
};

impl<G, A, R> RaftGroup<G, A, R> {
    /// Returns this group's caller-defined group ID.
    #[must_use]
    pub fn group_id(&self) -> &G {
        &self.group_id
    }

    /// Returns the local Raft node ID owned by this group.
    #[must_use]
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    /// Returns the latest leader hint known by the underlying Raft runtime.
    #[must_use]
    pub fn leader_hint(&self) -> Option<NodeId>
    where
        R: PersistedRaftRuntime,
    {
        self.raft.leader_hint()
    }

    /// Highest local proposal ID consumed by this group, if any.
    ///
    /// `rafter-app` requires strictly increasing `LocalProposalId`s for the
    /// lifetime of a group. IDs less than or equal to this watermark will be
    /// rejected.
    #[must_use]
    pub fn local_proposal_id_watermark(&self) -> Option<LocalProposalId> {
        self.last_seen_local_proposal_id
    }

    /// Highest read-index ID consumed by this group, if any.
    ///
    /// `rafter-app` requires strictly increasing `ReadId`s for read-index
    /// operations over the lifetime of a group. IDs less than or equal to this
    /// watermark will be rejected.
    #[must_use]
    pub fn read_id_watermark(&self) -> Option<ReadId> {
        self.last_seen_read_id
    }

    /// Returns the group's fatal health state.
    #[must_use]
    pub fn fatal_state(&self) -> &GroupFatalState {
        &self.fatal_state
    }

    /// Returns the error that poisoned this group, if it is poisoned and the
    /// poison came from a typed failure.
    ///
    /// [`GroupFatalState`] says *that* a group is poisoned and is published in
    /// every metrics snapshot, so it stays a plain comparable value. The cause
    /// is a diagnostic held beside it and is never published: a metrics
    /// snapshot is cloned and compared on every step and must not carry a
    /// `dyn Error`.
    #[must_use]
    pub fn poison_cause(&self) -> Option<&ErrorCause> {
        self.poison_cause.as_ref()
    }

    /// Returns shared access to the owned replicated state machine.
    #[must_use]
    pub fn state_machine(&self) -> &A {
        &self.app
    }

    /// Returns mutable access to the owned state machine.
    ///
    /// This is intended for caller-owned maintenance hooks and test fixtures.
    /// Do not mutate the durable applied floor behind the group; restart paths
    /// should use [`RaftGroup::with_applied_index`] to seed that boundary.
    pub fn state_machine_mut(&mut self) -> &mut A {
        &mut self.app
    }

    /// Returns a shared reference to the owned persisted runtime.
    ///
    /// This is useful for inspection and for fake runtimes in integration
    /// tests. Protocol progress should still flow through [`RaftGroup::step`]
    /// and the other group APIs.
    #[must_use]
    pub fn runtime(&self) -> &R {
        &self.raft
    }

    /// Returns waiters that were drained when the group entered poison.
    #[must_use]
    pub fn poisoned_waiters(&self) -> &PoisonedWaiters {
        &self.poisoned_waiters
    }

    /// Drains and returns waiters that were resolved by poison handling.
    #[must_use]
    pub fn drain_poisoned_waiters(&mut self) -> PoisonedWaiters {
        std::mem::take(&mut self.poisoned_waiters)
    }
}
