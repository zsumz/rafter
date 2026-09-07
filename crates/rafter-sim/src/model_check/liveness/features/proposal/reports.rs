//! Evidence rendering for the two LV-02 proposal features.
//!
//! A report is written only after its detector has already accepted the run,
//! so these builders restate the leader, budget, and terminal outcome that were
//! observed rather than re-deciding them. Nothing here can fail a check.

use rafter::NodeId;

use super::super::super::driver::{single_leader, LivenessRoundBudget, ProposalTerminalOutcome};
use super::super::{
    FaultStateRequirement, LivenessFeatureReport, LivenessPreconditionProbe, LivenessPreconditions,
    ProposalEvidence, StableLeaderEvidence, LV_02_PROGRESS_CLAUSE_IDS,
    LV_02_TERMINATION_CLAUSE_IDS,
};
use crate::model_check::{state::ExplorationState, ProposalId};

pub(super) fn proposal_progress_report(
    state: &ExplorationState,
    leader: NodeId,
    proposal_id: ProposalId,
    round_budget: LivenessRoundBudget,
    round_limit: usize,
    rounds_used: usize,
) -> LivenessFeatureReport {
    LivenessFeatureReport {
        invariant_id: "LV-02",
        clause_ids: LV_02_PROGRESS_CLAUSE_IDS,
        feature_id: "proposal-progress",
        scenario_id: "stable-leader-reachable-quorum-v1",
        observation_id: "accepted_completed_liveness_proposals",
        preconditions: LivenessPreconditions::capture(
            state,
            LivenessPreconditionProbe {
                leader: Some(leader),
                fault_requirement: FaultStateRequirement::Stopped,
                stable_leader_observed: Some(single_leader(state) == Some(leader)),
                accepted_proposal_observed: Some(true),
                authority_loss_observed: None,
            },
        ),
        round_budget,
        round_limit,
        rounds_used,
        fault_cycle: None,
        stable_leader: Some(StableLeaderEvidence {
            leader,
            stable_rounds: rounds_used.max(1),
            remained_leader_through_probe: true,
        }),
        proposal: Some(ProposalEvidence {
            proposal_id,
            outcome: ProposalTerminalOutcome::Committed,
        }),
        operation: None,
    }
}

pub(super) fn proposal_termination_report(
    state: &ExplorationState,
    proposal_id: ProposalId,
    outcome: ProposalTerminalOutcome,
    stable_leader_at_acceptance: bool,
    round_budget: LivenessRoundBudget,
    round_limit: usize,
    rounds_used: usize,
) -> LivenessFeatureReport {
    LivenessFeatureReport {
        invariant_id: "LV-02",
        clause_ids: LV_02_TERMINATION_CLAUSE_IDS,
        feature_id: "proposal-termination",
        scenario_id: "accepted-proposal-authority-loss-v1",
        observation_id: "terminated_liveness_proposals",
        preconditions: LivenessPreconditions::capture(
            state,
            LivenessPreconditionProbe {
                leader: single_leader(state),
                fault_requirement: FaultStateRequirement::Stopped,
                stable_leader_observed: Some(stable_leader_at_acceptance),
                accepted_proposal_observed: Some(true),
                authority_loss_observed: Some(single_leader(state) != Some(NodeId(1))),
            },
        ),
        round_budget,
        round_limit,
        rounds_used,
        fault_cycle: None,
        stable_leader: Some(StableLeaderEvidence {
            leader: NodeId(1),
            stable_rounds: 1,
            remained_leader_through_probe: false,
        }),
        proposal: Some(ProposalEvidence {
            proposal_id,
            outcome,
        }),
        operation: None,
    }
}
