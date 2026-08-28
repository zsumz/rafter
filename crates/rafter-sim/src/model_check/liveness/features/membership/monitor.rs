//! What the membership feature watches, and the report it owes when it stops.
//!
//! The monitor fixes the leader, the removed voter, and the rejection floor
//! before the operation is issued, so a terminal outcome is read against the
//! premise the run started from rather than the state it drifted to. Driving
//! the operation belongs to the sibling module.

use rafter::{MembershipConfig, MembershipSet, NodeId};

use crate::model_check::liveness::driver::{quiescent_leader, LivenessRoundBudget};
use crate::model_check::liveness::features::{
    LivenessFeatureReport, LivenessPreconditions, OperationEvidence, OperationTerminalOutcome,
    TerminalRecorderMode, LV_03_MEMBERSHIP_CLAUSE_IDS,
};
use crate::model_check::state::ExplorationState;
use crate::records::ProposalRejected;

pub(super) struct MembershipMonitor {
    pub(super) leader: NodeId,
    pub(super) removed_voter: NodeId,
    pub(super) target: MembershipSet,
    pub(super) rejection_floor: usize,
    pub(super) convergence_budget: usize,
    pub(super) operation_budget: usize,
    pub(super) round_budget: LivenessRoundBudget,
    pub(super) convergence_rounds: usize,
    pub(super) preconditions: LivenessPreconditions,
    pub(super) recorder_mode: TerminalRecorderMode,
}

impl MembershipMonitor {
    pub(super) fn report(
        &self,
        operation_rounds: usize,
        operation: OperationEvidence,
    ) -> LivenessFeatureReport {
        membership_report(
            self.convergence_budget,
            self.operation_budget,
            self.round_budget,
            self.convergence_rounds,
            operation_rounds,
            self.preconditions.clone(),
            operation,
        )
    }

    pub(super) fn outcome(
        &self,
        state: &ExplorationState,
        rejection_node: NodeId,
    ) -> Option<OperationTerminalOutcome> {
        if membership_operation_rejected(state, self.rejection_floor, rejection_node) {
            Some(OperationTerminalOutcome::Rejected)
        } else if membership_transition_completed(state, &self.target) {
            Some(OperationTerminalOutcome::Committed)
        } else {
            None
        }
    }
}

fn membership_report(
    convergence_budget: usize,
    operation_budget: usize,
    round_budget: LivenessRoundBudget,
    convergence_rounds: usize,
    operation_rounds: usize,
    preconditions: LivenessPreconditions,
    operation: OperationEvidence,
) -> LivenessFeatureReport {
    LivenessFeatureReport {
        invariant_id: "LV-03",
        clause_ids: LV_03_MEMBERSHIP_CLAUSE_IDS,
        feature_id: "membership-transition",
        scenario_id: "stable-remove-voter-joint-consensus-v1",
        observation_id: "terminated_stable_membership_operations",
        preconditions,
        round_budget,
        round_limit: convergence_budget.saturating_add(operation_budget),
        rounds_used: convergence_rounds.saturating_add(operation_rounds),
        fault_cycle: None,
        stable_leader: None,
        proposal: None,
        operation: Some(operation),
    }
}

fn membership_operation_rejected(
    state: &ExplorationState,
    rejection_floor: usize,
    leader: NodeId,
) -> bool {
    membership_rejection_observed(
        state.cluster().proposal_rejections(),
        rejection_floor,
        leader,
    )
}

pub(super) fn membership_rejection_observed(
    rejections: &[ProposalRejected],
    rejection_floor: usize,
    leader: NodeId,
) -> bool {
    rejections[rejection_floor..]
        .iter()
        .any(|rejection| rejection.node_id == leader && rejection.proposal_id.is_none())
}

pub(super) fn membership_liveness_target(
    state: &ExplorationState,
    leader: NodeId,
) -> Option<(NodeId, MembershipSet)> {
    let MembershipConfig::Stable(current) = state.cluster().effective_membership(leader) else {
        return None;
    };
    let removed_voter = current
        .voters()
        .iter()
        .copied()
        .filter(|node_id| *node_id != leader)
        .max_by_key(|node_id| node_id.0)?;
    let voters = current
        .voters()
        .iter()
        .copied()
        .filter(|node_id| *node_id != removed_voter)
        .collect::<Vec<_>>();
    let target = MembershipSet::new(voters, current.learners().to_vec()).ok()?;
    Some((removed_voter, target))
}

pub(super) fn membership_transition_ready_to_leave(
    state: &ExplorationState,
    target: &MembershipSet,
) -> Option<NodeId> {
    let leader = quiescent_leader(state)?;
    let effective = state.cluster().effective_membership(leader);
    let committed = state.cluster().committed_membership(leader);
    match (&effective, &committed) {
        (MembershipConfig::Joint(joint), MembershipConfig::Joint(_))
            if joint.new_membership() == target && committed == effective =>
        {
            Some(leader)
        }
        _ => None,
    }
}

fn membership_transition_completed(state: &ExplorationState, target: &MembershipSet) -> bool {
    let Some(leader) = quiescent_leader(state) else {
        return false;
    };
    stable_membership_matches(&state.cluster().effective_membership(leader), target)
        && stable_membership_matches(&state.cluster().committed_membership(leader), target)
}

fn stable_membership_matches(config: &MembershipConfig, target: &MembershipSet) -> bool {
    matches!(config, MembershipConfig::Stable(membership) if membership == target)
}
