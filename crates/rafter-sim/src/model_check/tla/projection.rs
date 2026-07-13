use super::super::{Action, MessageKind};
use super::errors::TlaProjectionFailure;
use super::types::{TlaAbstractionGap, TlaAction, TlaProjection, TlaTraceStep};

/// Projects a Rust model-checking trace into the abstract TLA+ action boundary.
#[must_use]
pub fn project_raft_trace_to_tla(trace: &[Action]) -> Vec<TlaTraceStep> {
    trace
        .iter()
        .cloned()
        .map(|rust_action| {
            let projection = project_raft_action_to_tla(&rust_action);
            TlaTraceStep {
                rust_action,
                projection,
            }
        })
        .collect()
}

/// Returns TLA+ actions only when every Rust trace step fits the abstract model.
///
/// # Errors
///
/// Returns [`TlaProjectionFailure`] at the first Rust action that belongs to an
/// implementation-level behavior the current TLA+ spec intentionally omits.
pub fn require_tla_projectable_raft_trace(
    trace: &[Action],
) -> Result<Vec<TlaAction>, TlaProjectionFailure> {
    trace
        .iter()
        .cloned()
        .enumerate()
        .map(
            |(action_index, action)| match project_raft_action_to_tla(&action) {
                TlaProjection::Action(tla_action) => Ok(tla_action),
                TlaProjection::Gap(gap) => Err(TlaProjectionFailure {
                    action_index,
                    action,
                    gap,
                }),
            },
        )
        .collect()
}

fn project_raft_action_to_tla(action: &Action) -> TlaProjection {
    match action {
        Action::Tick(node_id) => TlaProjection::Action(TlaAction::Timeout { node_id: *node_id }),
        Action::Propose { to, .. } => {
            TlaProjection::Action(TlaAction::ClientAppend { node_id: *to })
        }
        Action::ReadIndex { to, .. } => {
            TlaProjection::Action(TlaAction::RegisterRead { node_id: *to })
        }
        Action::Restart(node_id) => TlaProjection::Action(TlaAction::Restart { node_id: *node_id }),
        Action::ApplicationLossRestart(_) => {
            TlaProjection::Gap(TlaAbstractionGap::ApplicationStateLoss)
        }
        Action::Deliver {
            from,
            to,
            message: MessageKind::RequestVote,
            ..
        } => TlaProjection::Action(TlaAction::DeliverRequestVote {
            from: *from,
            to: *to,
        }),
        Action::Deliver {
            from,
            to,
            message: MessageKind::AppendEntries,
            ..
        } => TlaProjection::Action(TlaAction::DeliverAppend {
            from: *from,
            to: *to,
        }),
        Action::Deliver {
            message: MessageKind::RequestVoteResponse,
            ..
        } => TlaProjection::Gap(TlaAbstractionGap::RequestVoteResponse),
        Action::Deliver {
            message: MessageKind::AppendEntriesResponse,
            ..
        } => TlaProjection::Gap(TlaAbstractionGap::AppendEntriesResponse),
        Action::Deliver {
            message:
                MessageKind::InstallSnapshot
                | MessageKind::InstallSnapshotChunk
                | MessageKind::InstallSnapshotResponse,
            ..
        } => TlaProjection::Gap(TlaAbstractionGap::SnapshotTransfer),
        // The pre-vote extension (thesis 9.6) is not part of the abstract
        // TLA+ election model, which only knows term-incrementing elections.
        Action::Deliver {
            message: MessageKind::PreVote | MessageKind::PreVoteResponse | MessageKind::TimeoutNow,
            ..
        } => TlaProjection::Gap(TlaAbstractionGap::PreVote),
        Action::AddLearner { .. }
        | Action::RemoveLearner { .. }
        | Action::PromoteLearner { .. }
        | Action::RemoveVoter { .. }
        | Action::EnterJoint { .. }
        | Action::LeaveJoint { .. } => TlaProjection::Gap(TlaAbstractionGap::MembershipChange),
    }
}
