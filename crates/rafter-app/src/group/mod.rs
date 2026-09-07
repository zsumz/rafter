//! Synchronous Raft group driver types.
//!
//! The group layer owns one local durable Raft node, one application state
//! machine, pending proposal/read correlation, poison state, and explicit step
//! reports. It does not own networking or spawn background work.

use std::cmp::max;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::sync::Arc;

use rafter::{
    ClientProposalInput, Input as RaftInput, LeadershipTransferRejection, LocalProposalDropReason,
    LocalProposalId, LogIndex, MembershipConfig, Message, NodeId, Output as RaftOutput,
    ProposalRejection, RaftSnapshot, ReadId, ReadIndexCancelReason, ReadIndexRejection,
    SharedPayload, Term,
};
use rafter_runtime_api::PersistedRaftRuntime;

use crate::error::{ErrorCause, GroupError, StateMachineOperation};
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
    ApplicationSnapshot, ApplicationSnapshotError, ApplyBatch, ApplyEntry, ApplyResult,
    ReadBarrier, ReplicatedStateMachine, SnapshotSupport,
};
use crate::transport::PeerEnvelope;

mod accessor;
mod apply;
mod construct;
mod input;
mod membership;
mod membership_mark;
mod observe;
mod outcome;
mod output;
mod parts;
mod poison;
mod proposal;
mod proposal_step;
mod read;
mod read_barrier;
mod read_query;
mod read_state;
mod recovery;
mod snapshot;
mod state;
mod step;
mod transfer;
mod validation;

pub use input::GroupInput;
pub use membership_mark::MembershipReportMark;
pub use outcome::{
    ProposalBatchBeginReport, ProposalBeginReport, ReadBarrierBeginReport, ReadReport,
};
pub use parts::RaftGroupParts;
pub use poison::{GroupFatalState, PoisonedWaiters};
pub use state::{GroupStepReport, RaftGroup, StepReportOptions};
pub use transfer::LeadershipTransferEvent;

use construct::{
    ApplyEntryResult, GroupResult, ProposalBatchBeginReportResult, ProposalBeginReportResult,
    ProposalBeginResult, ReadBarrierBeginReportResult, ReadOutcomeResult, ReadReportResult,
    RuntimeGroupError, StepReportResult,
};
use outcome::report_has_proposal_lifecycle;
use read_state::{CompletedQueryRead, GrantedReadIndex, PendingQueryRead, PendingRead};
