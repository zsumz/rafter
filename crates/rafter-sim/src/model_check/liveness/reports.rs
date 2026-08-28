//! Evidence rendering for the two post-heal LV-01 features.
//!
//! A report is only written once its scenario has already succeeded, so these
//! builders restate the observed convergence and usability evidence rather
//! than judging it. Deciding whether the run passed belongs to the parent;
//! nothing here can fail a check.

use super::driver::{single_leader, LivenessRoundBudget, ProposalTerminalOutcome};
use super::features::{
    FaultCycleEvidence, FaultStateRequirement, LivenessFeatureReport, LivenessPreconditionProbe,
    LivenessPreconditions, ProposalEvidence, StableLeaderEvidence, LV_01_CONVERGENCE_CLAUSE_IDS,
    LV_01_USABILITY_CLAUSE_IDS,
};
use super::{driver, ExplorationState};

pub(super) fn successful_post_heal_convergence_report(
    state: &ExplorationState,
    round_budget: LivenessRoundBudget,
    convergence: driver::LeaderConvergence,
    fault_cycle: FaultCycleEvidence,
) -> LivenessFeatureReport {
    LivenessFeatureReport {
        invariant_id: "LV-01",
        clause_ids: LV_01_CONVERGENCE_CLAUSE_IDS,
        feature_id: "leader-convergence",
        scenario_id: "post-heal-stable-quorum-v1",
        observation_id: "post_heal_quiescent_leaders",
        preconditions: LivenessPreconditions::capture(
            state,
            LivenessPreconditionProbe {
                leader: Some(convergence.leader),
                fault_requirement: FaultStateRequirement::Stopped,
                stable_leader_observed: Some(single_leader(state) == Some(convergence.leader)),
                accepted_proposal_observed: None,
                authority_loss_observed: None,
            },
        ),
        round_budget,
        round_limit: round_budget.round_limit(),
        rounds_used: convergence
            .rounds_used
            .saturating_add(fault_cycle.partitioned_rounds),
        fault_cycle: Some(fault_cycle),
        stable_leader: Some(StableLeaderEvidence {
            leader: convergence.leader,
            stable_rounds: convergence.stable_rounds,
            remained_leader_through_probe: true,
        }),
        proposal: None,
        operation: None,
    }
}

pub(super) fn successful_post_heal_usability_report(
    state: &ExplorationState,
    round_budget: LivenessRoundBudget,
    convergence: driver::LeaderConvergence,
    completion: driver::BoundedRun,
    proposal_id: crate::model_check::ProposalId,
    accepted_proposal: bool,
) -> LivenessFeatureReport {
    LivenessFeatureReport {
        invariant_id: "LV-01",
        clause_ids: LV_01_USABILITY_CLAUSE_IDS,
        feature_id: "leader-usability",
        scenario_id: "post-heal-stable-quorum-v1",
        observation_id: "post_heal_stable_leader_usability_windows",
        preconditions: LivenessPreconditions::capture(
            state,
            LivenessPreconditionProbe {
                leader: Some(convergence.leader),
                fault_requirement: FaultStateRequirement::Stopped,
                stable_leader_observed: Some(single_leader(state) == Some(convergence.leader)),
                accepted_proposal_observed: Some(accepted_proposal),
                authority_loss_observed: None,
            },
        ),
        round_budget,
        round_limit: round_budget.round_limit(),
        rounds_used: completion.rounds_used,
        fault_cycle: None,
        stable_leader: Some(StableLeaderEvidence {
            leader: convergence.leader,
            stable_rounds: convergence.stable_rounds,
            remained_leader_through_probe: true,
        }),
        proposal: Some(ProposalEvidence {
            proposal_id,
            outcome: ProposalTerminalOutcome::Committed,
        }),
        operation: None,
    }
}
