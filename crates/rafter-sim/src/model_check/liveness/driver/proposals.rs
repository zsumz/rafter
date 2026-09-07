//! Client-proposal liveness probes and their terminal outcomes.
//!
//! A proposal only counts as issued once the recorder accepted it and its
//! payload is visible in applied or logged state, so a monitor never waits on
//! a proposal the cluster never took.

use std::collections::BTreeSet;

use rafter::NodeId;

use crate::model_check::{
    helpers::proposal_payload,
    scheduling::Operation,
    soak::{SoakAction, SoakActionKind},
    state::apply_to_state,
    state::{ClientWriteStatus, ExplorationState},
    ProposalId,
};

use super::ProposalTerminalOutcome;

impl ProposalTerminalOutcome {
    pub(in crate::model_check::liveness) const fn as_str(self) -> &'static str {
        match self {
            Self::Committed => "committed",
            Self::Rejected => "rejected",
            Self::Unknown => "unknown",
        }
    }
}

pub(in crate::model_check::liveness) fn issue_liveness_proposal(
    state: &mut ExplorationState,
    leader: NodeId,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
) -> Option<ProposalId> {
    let proposal_id = ProposalId(state.proposals_issued() + 1);
    let payload = proposal_payload(proposal_id);
    apply_to_state(
        state,
        Operation::Propose {
            to: leader,
            proposal_id,
            stale_leader: false,
        },
    );
    trace.push(SoakAction::Propose {
        to: leader,
        proposal_id,
    });
    observed_actions.insert(SoakActionKind::Propose);

    if !state
        .client_history()
        .writes
        .get(&proposal_id)
        .is_some_and(|write| {
            matches!(
                write.status,
                ClientWriteStatus::Accepted { .. } | ClientWriteStatus::Completed { .. }
            )
        })
        || !liveness_payload_visible(state, &payload)
    {
        return None;
    }

    Some(proposal_id)
}

pub(in crate::model_check::liveness) fn liveness_proposal_completed(
    state: &ExplorationState,
    proposal_id: ProposalId,
) -> bool {
    state
        .client_history()
        .writes
        .get(&proposal_id)
        .is_some_and(|write| matches!(write.status, ClientWriteStatus::Completed { .. }))
}

pub(in crate::model_check::liveness) fn liveness_proposal_accepted(
    state: &ExplorationState,
    proposal_id: ProposalId,
) -> bool {
    state
        .client_history()
        .writes
        .get(&proposal_id)
        .is_some_and(|write| {
            matches!(
                write.status,
                ClientWriteStatus::Accepted { .. } | ClientWriteStatus::Completed { .. }
            )
        })
}

pub(in crate::model_check::liveness) fn liveness_proposal_terminal_outcome(
    state: &ExplorationState,
    proposal_id: ProposalId,
) -> Option<ProposalTerminalOutcome> {
    let write = state.client_history().writes.get(&proposal_id)?;
    match write.status {
        ClientWriteStatus::Completed { .. } => Some(ProposalTerminalOutcome::Committed),
        ClientWriteStatus::Rejected => Some(ProposalTerminalOutcome::Rejected),
        ClientWriteStatus::Unknown { .. } => Some(ProposalTerminalOutcome::Unknown),
        ClientWriteStatus::Pending | ClientWriteStatus::Accepted { .. } => None,
    }
}

fn liveness_payload_visible(state: &ExplorationState, payload: &[u8]) -> bool {
    state
        .cluster()
        .applied()
        .iter()
        .any(|applied| applied.payload.as_slice() == payload)
        || state.cluster().nodes.keys().any(|node_id| {
            state
                .cluster()
                .bootstrap_state(*node_id)
                .log
                .iter()
                .any(|entry| entry.kind.application_payload() == Some(payload))
        })
}
