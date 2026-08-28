//! The error vocabulary of the synchronous app/group driver.
//!
//! One variant per category a caller branches on. Preserved causes travel
//! inside the variants rather than being rendered into their messages, so
//! the category and the failure stay separately readable.

use std::sync::Arc;

use rafter::{LocalProposalId, LogIndex, NodeId, ReadId, Term};

use crate::read::ReadConsistency;

use super::{ErrorCause, StateMachineOperation};

/// Errors returned by the synchronous app/group driver.
#[derive(Debug)]
#[non_exhaustive]
pub enum GroupError<E, R> {
    /// The underlying Raft runtime failed.
    Runtime(R),
    /// The application state machine failed.
    ///
    /// `source` is shared rather than owned because a failure that poisons the
    /// group has two owners: the group retains it as its
    /// [`crate::group::RaftGroup::poison_cause`] so every later refusal can
    /// report what broke, and the same error travels here to the caller that
    /// triggered it. `ReplicatedStateMachine::Error` is deliberately not
    /// `Clone`, so the two share one allocation.
    StateMachine {
        /// Callback that failed.
        operation: StateMachineOperation,
        /// Exact application error returned by the callback.
        source: Arc<E>,
    },
    /// The state machine returned the wrong number of apply results.
    ApplyResultCountMismatch {
        /// Number of committed entries presented to the state machine.
        expected: usize,
        /// Number of results returned by the state machine.
        actual: usize,
    },
    /// An apply result did not preserve the committed entry's identity.
    ApplyResultMetadataMismatch {
        /// Committed log index.
        expected_index: LogIndex,
        /// Log index reported by the state machine.
        actual_index: LogIndex,
        /// Committed log term.
        expected_term: Term,
        /// Log term reported by the state machine.
        actual_term: Term,
        /// Local proposal identifier attached to the committed entry.
        expected_local_proposal_id: Option<LocalProposalId>,
        /// Local proposal identifier reported by the state machine.
        actual_local_proposal_id: Option<LocalProposalId>,
    },
    /// The state machine claims an entry awaiting apply was already applied.
    ApplyEntryAlreadyApplied {
        /// Index of the entry the group attempted to apply.
        entry_index: LogIndex,
        /// Durable applied index reported by the state machine.
        app_applied_index: LogIndex,
        /// Applied index previously accepted by the group.
        group_applied_index: LogIndex,
    },
    /// The state machine's applied index is behind the group's required floor.
    AppliedIndexBehind {
        /// Minimum applied index required by the group.
        required: LogIndex,
        /// Durable applied index reported by the state machine.
        actual: LogIndex,
    },
    /// The state machine is below the runtime's snapshot boundary, so the
    /// entries it is missing are compacted out of the Raft log and will never
    /// be delivered to it.
    ///
    /// The kernel raises a declared applied floor to its snapshot boundary,
    /// because it can neither emit the covered entries nor restore a state
    /// machine from a snapshot whose bytes it does not hold. That raise is
    /// silent, and this group refuses to run on top of it: every
    /// [`crate::group::RaftGroup::step`] and every
    /// [`crate::group::RaftGroup::begin_proposal`] would advance the protocol
    /// for, and every [`crate::group::RaftGroup::read`] would answer from, a
    /// state machine missing acknowledged entries, with nothing reporting the
    /// gap.
    ///
    /// [`crate::group::RaftGroup::apply_raft_outputs`] never returns this
    /// error, and neither does [`crate::group::RaftGroup::metrics`]. A replica
    /// that crashed between promoting an inbound snapshot and installing it is
    /// legitimately below its boundary while it drains its recovery outputs,
    /// and metrics reporting `applied_index` beside `snapshot_index` is how an
    /// operator sees the gap. The refusal falls on the first step or read
    /// after a restore that never came.
    ///
    /// The repair is to restore the state machine from the snapshot the
    /// boundary names, before or after constructing the group, or to discard
    /// this replica's Raft state so it rejoins empty and is sent one.
    AppliedIndexBelowSnapshotBoundary {
        /// Durable applied index reported by the state machine.
        app_applied_index: LogIndex,
        /// Compacted snapshot boundary retained by the Raft runtime.
        snapshot_index: LogIndex,
    },
    /// A committed application entry arrived for a state machine that has not
    /// yet installed the snapshot underneath it.
    ///
    /// **The refusal that keeps a recovery from laundering a gap into durable
    /// state.** Applying an entry above the boundary onto a state machine below
    /// it produces exactly one artifact: an application holding a prefix, then
    /// a hole where the compacted entries were, then the suffix — and every
    /// index, every readiness probe, and every metrics snapshot reports it as
    /// caught up, because each of them only ever compares numbers. Nothing
    /// later can find the hole, so it has to be refused before the first entry
    /// lands.
    ///
    /// This is not [`GroupError::AppliedIndexBelowSnapshotBoundary`] and it
    /// does not poison. The two name the same *gap* at two different moments,
    /// and the difference is whether a repair is still available. This one is
    /// raised before the application is touched, while the snapshot the
    /// boundary names is still installable; the repair is to install it and
    /// apply the suffix afterwards, which is what
    /// [`crate::group::RaftGroup::apply_recovery_outputs`] does as one
    /// operation. The other is the permanent verdict taken when a state machine
    /// that was never restored tries to answer for the replica.
    ///
    /// A caller reaching this from
    /// [`crate::group::RaftGroup::apply_raft_outputs`] is draining a recovery
    /// suffix through the raw pump. Route it through
    /// [`crate::group::RaftGroup::apply_recovery_outputs`] instead. The
    /// application is untouched either way.
    SnapshotRestoreRequired {
        /// Durable applied index reported by the state machine.
        app_applied_index: LogIndex,
        /// Compacted snapshot boundary retained by the Raft runtime.
        snapshot_index: LogIndex,
        /// Lowest committed entry the refused batch would have applied.
        entry_index: LogIndex,
    },
    /// The Raft runtime emitted snapshot metadata that the group cannot apply.
    MalformedSnapshot {
        /// Stable explanation of the invalid snapshot output.
        reason: String,
    },
    /// A Raft-driven snapshot install reached a state machine that declared
    /// [`crate::state_machine::SnapshotSupport::Unsupported`].
    ///
    /// The state machine was not called. This replica has fallen behind the
    /// leader's compacted prefix and cannot catch up, so the group poisons.
    SnapshotsUnsupported {
        /// Index of the snapshot that must be installed.
        snapshot_index: LogIndex,
    },
    /// A state machine that declared
    /// [`crate::state_machine::SnapshotSupport::Supported`] refused the
    /// install as unsupported, which means it inherited a provided method body
    /// while declaring support.
    SnapshotSupportMisdeclared {
        /// Index of the snapshot the state machine refused.
        snapshot_index: LogIndex,
    },
    /// The group is permanently poisoned.
    ///
    /// `cause` is the error that poisoned the group, when the poison came from
    /// a typed failure. It is `None` for a poison with no underlying error,
    /// such as a malformed snapshot output or a state machine that broke an
    /// apply-result invariant.
    Poisoned {
        /// Stable explanation of the failure that poisoned the group.
        reason: String,
        /// Preserved typed cause, when the poison originated in a callback.
        cause: Option<ErrorCause>,
    },
    /// An input names another Raft group.
    WrongGroup,
    /// An inbound message targets another local replica.
    WrongRecipient {
        /// Local node identifier required by this group.
        expected: NodeId,
        /// Recipient carried by the inbound message.
        actual: NodeId,
    },
    /// A proposal identifier did not increase over the prior local proposal.
    NonMonotonicLocalProposalId {
        /// Reused or decreasing proposal identifier.
        local_proposal_id: LocalProposalId,
        /// Greatest proposal identifier previously accepted locally.
        last_seen_local_proposal_id: LocalProposalId,
    },
    /// A read identifier is already active.
    DuplicateReadId {
        /// Identifier already owned by an in-flight read.
        read_id: ReadId,
    },
    /// A read identifier did not increase over the prior local read.
    NonMonotonicReadId {
        /// Reused or decreasing read identifier.
        read_id: ReadId,
        /// Greatest read identifier previously accepted locally.
        last_seen_read_id: ReadId,
    },
    /// The synchronous group does not implement the requested read mode.
    UnsupportedReadConsistency {
        /// Read mode rejected by the group.
        consistency: ReadConsistency,
    },
    /// The runtime emitted an output that this group integration cannot handle.
    UnsupportedOutput {
        /// Stable name of the unsupported output variant.
        output: &'static str,
    },
}
