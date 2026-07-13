use std::fmt;

use rafter::NodeId;

use super::super::Action;

pub(super) const TLA_NODE_COUNT: u64 = 3;
pub(super) const TLA_VALUE_SYMBOLS: [&str; 2] = ["v1", "v2"];
pub(super) const TLA_READ_REQUEST_SYMBOLS: [&str; 2] = ["r1", "r2"];

/// Abstract TLA+ action vocabulary that a Rust simulator trace can project to.
///
/// This enum is exhaustive because it mirrors the current supported abstract
/// action subset in `specs/tla/raft/Raft.tla`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlaAction {
    Timeout { node_id: NodeId },
    ClientAppend { node_id: NodeId },
    RegisterRead { node_id: NodeId },
    Restart { node_id: NodeId },
    DeliverRequestVote { from: NodeId, to: NodeId },
    DeliverAppend { from: NodeId, to: NodeId },
}

impl TlaAction {
    /// Returns the corresponding action name in `specs/tla/raft/Raft.tla`.
    #[must_use]
    pub const fn tla_name(self) -> &'static str {
        match self {
            Self::Timeout { .. } => "Timeout",
            Self::ClientAppend { .. } => "ClientAppend",
            Self::RegisterRead { .. } => "RegisterRead",
            Self::Restart { .. } => "Restart",
            Self::DeliverRequestVote { .. } => "DeliverRequestVote",
            Self::DeliverAppend { .. } => "DeliverAppend",
        }
    }
}

impl fmt::Display for TlaAction {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout { node_id } => write!(formatter, "Timeout({node_id})"),
            Self::ClientAppend { node_id } => write!(formatter, "ClientAppend({node_id})"),
            Self::RegisterRead { node_id } => write!(formatter, "RegisterRead({node_id})"),
            Self::Restart { node_id } => write!(formatter, "Restart({node_id})"),
            Self::DeliverRequestVote { from, to } => {
                write!(formatter, "DeliverRequestVote({from}->{to})")
            }
            Self::DeliverAppend { from, to } => write!(formatter, "DeliverAppend({from}->{to})"),
        }
    }
}

/// Named reason a Rust trace step is outside the current abstract TLA+ model.
///
/// This enum is exhaustive because abstraction gaps are reported from a closed
/// set of known unsupported trace shapes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlaAbstractionGap {
    RequestVoteResponse,
    AppendEntriesResponse,
    SnapshotTransfer,
    PreVote,
    MembershipChange,
    ApplicationStateLoss,
}

impl TlaAbstractionGap {
    /// Returns a stable identifier for the abstraction gap.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::RequestVoteResponse => "request_vote_response_abstracted",
            Self::AppendEntriesResponse => "append_entries_response_abstracted",
            Self::SnapshotTransfer => "snapshot_transfer_not_in_tla_model",
            Self::PreVote => "pre_vote_not_in_tla_model",
            Self::MembershipChange => "membership_change_not_in_tla_model",
            Self::ApplicationStateLoss => "application_state_loss_not_in_tla_model",
        }
    }
}

impl fmt::Display for TlaAbstractionGap {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

/// Projection result for one Rust model-checking action.
///
/// This enum is exhaustive because every projection is either an abstract TLA+
/// action or a named abstraction gap.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TlaProjection {
    Action(TlaAction),
    Gap(TlaAbstractionGap),
}

/// A Rust trace step paired with its TLA+ projection.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TlaTraceStep {
    pub(super) rust_action: Action,
    pub(super) projection: TlaProjection,
}

impl TlaTraceStep {
    /// Returns the original Rust model-checking action.
    #[must_use]
    pub const fn rust_action(&self) -> &Action {
        &self.rust_action
    }

    /// Returns the TLA+ action or named abstraction gap for this step.
    #[must_use]
    pub const fn projection(&self) -> TlaProjection {
        self.projection
    }
}

/// TLC-checkable TLA+ module and config generated from a projected Rust trace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TlaTraceSpec {
    pub(super) module: String,
    pub(super) config: String,
}

impl TlaTraceSpec {
    /// Returns the generated TLA+ module text.
    #[must_use]
    pub fn module(&self) -> &str {
        &self.module
    }

    /// Returns the generated TLC config text.
    #[must_use]
    pub fn config(&self) -> &str {
        &self.config
    }
}
