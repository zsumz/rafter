//! Synchronous Raft group driver types.
//!
//! The group layer owns one local durable Raft node, one application state
//! machine, pending proposal/read correlation, poison state, and explicit step
//! reports. It does not own networking or spawn background work.

use std::cmp::max;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;

use rafter::{
    ClientProposalInput, Input as RaftInput, LeadershipTransferRejection, LocalProposalDropReason,
    LocalProposalId, LogIndex, MembershipConfig, Message, NodeId, Output as RaftOutput,
    ProposalRejection, RaftSnapshot, ReadId, ReadIndexCancelReason, ReadIndexRejection,
    SharedPayload, Term,
};
use rafter_runtime_api::PersistedRaftRuntime;

use crate::error::{GroupError, StateMachineOperation};
use crate::membership::{MembershipChange, MembershipEvent};
use crate::metrics::RaftGroupMetrics;
use crate::proposal::{
    ClientRequestId, Proposal, ProposalBegin, ProposalEvent, ProposalUnknownOutcomeReason,
};
use crate::read::{
    ReadBarrierRequest, ReadConsistency, ReadEvent, ReadOutcome, ReadProof, ReadProofOutcome,
    ReadRequest,
};
use crate::snapshot::SnapshotEvent;
use crate::state_machine::{
    ApplicationSnapshot, ApplyBatch, ApplyEntry, ApplyResult, ReadBarrier, ReplicatedStateMachine,
};
use crate::transport::PeerEnvelope;

mod apply;
mod membership;
mod output;
mod poison;
mod proposal;
mod read;
mod snapshot;
mod transfer;
mod types;
mod validation;

pub use types::{
    GroupFatalState, GroupInput, GroupStepReport, LeadershipTransferEvent, PoisonedWaiters,
    ProposalBatchBeginReport, ProposalBeginReport, RaftGroup, ReadBarrierBeginReport,
    StepReportOptions,
};

use types::{
    report_has_proposal_lifecycle, ApplyEntryResult, CompletedQueryRead, GroupResult,
    MembershipStepContext, PendingQueryRead, PendingRead, ProposalBatchBeginReportResult,
    ProposalBeginReportResult, ProposalBeginResult, ReadBarrierBeginReportResult,
    ReadOutcomeResult, RuntimeGroupError, StepReportResult,
};
