use std::collections::BTreeSet;

use rafter::{MembershipConfig, MembershipSet, NodeId};

use super::super::driver::{
    check_soak_safety, drive_soak_liveness_round, drive_until_stable_leader, quiescent_leader,
    soak_liveness_failure, LivenessRoundBudget,
};
use super::{
    FaultStateRequirement, LivenessFeatureReport, LivenessPreconditionProbe, LivenessPreconditions,
    LV_03_MEMBERSHIP_CLAUSE_IDS,
};
use crate::model_check::{
    catalog,
    scheduling::Operation,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::apply_to_state,
    state::ExplorationState,
};

pub(super) fn run_membership_transition_liveness_check(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    budget: usize,
) -> Result<LivenessFeatureReport, SoakFailure> {
    let round_budget = LivenessRoundBudget::capture(state, config, 2);
    let Some(convergence) =
        drive_until_stable_leader(state, config, trace, observed_actions, budget)?
    else {
        return Err(soak_liveness_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            format!("no leader elected within {budget} membership-transition liveness rounds"),
        ));
    };
    let leader = convergence.leader;
    let preconditions = LivenessPreconditions::capture(
        state,
        LivenessPreconditionProbe {
            leader: Some(leader),
            fault_requirement: FaultStateRequirement::Stopped,
            stable_leader_observed: None,
            accepted_proposal_observed: None,
            authority_loss_observed: None,
        },
    );

    let Some((removed_voter, target)) = membership_liveness_target(state, leader) else {
        return Err(soak_liveness_failure(
            state,
            config,
            trace,
            catalog::LV_03_FEATURE_OPERATION_PROGRESS,
            "membership liveness precondition was not reached: no removable voter".to_owned(),
        ));
    };

    apply_to_state(
        state,
        Operation::RemoveVoter {
            to: leader,
            voter_id: removed_voter,
        },
    );
    trace.push(SoakAction::RemoveVoter {
        to: leader,
        voter_id: removed_voter,
    });
    observed_actions.insert(SoakActionKind::RemoveVoter);
    check_soak_safety(state, config, trace)?;

    let mut leave_issued = false;
    for round in 0..budget {
        if membership_transition_completed(state, &target) {
            return Ok(membership_report(
                budget,
                round_budget,
                convergence.rounds_used,
                round,
                preconditions,
            ));
        }

        if !leave_issued {
            if let Some(leader) = membership_transition_ready_to_leave(state, &target) {
                apply_to_state(state, Operation::LeaveJoint { to: leader });
                trace.push(SoakAction::LeaveJoint { to: leader });
                observed_actions.insert(SoakActionKind::LeaveJoint);
                leave_issued = true;
                check_soak_safety(state, config, trace)?;
                if membership_transition_completed(state, &target) {
                    return Ok(membership_report(
                        budget,
                        round_budget,
                        convergence.rounds_used,
                        round,
                        preconditions,
                    ));
                }
            }
        }

        drive_soak_liveness_round(state, config, trace, observed_actions, round)?;
        check_soak_safety(state, config, trace)?;
    }

    Err(soak_liveness_failure(
        state,
        config,
        trace,
        catalog::LV_03_FEATURE_OPERATION_PROGRESS,
        format!(
            "membership transition removing {removed_voter} did not reach stable target {target:?} within {budget} post-heal rounds"
        ),
    ))
}

fn membership_report(
    budget: usize,
    round_budget: LivenessRoundBudget,
    convergence_rounds: usize,
    operation_rounds: usize,
    preconditions: LivenessPreconditions,
) -> LivenessFeatureReport {
    LivenessFeatureReport {
        invariant_id: "LV-03",
        clause_ids: LV_03_MEMBERSHIP_CLAUSE_IDS,
        feature_id: "membership-transition",
        scenario_id: "stable-remove-voter-joint-consensus-v1",
        observation_id: "completed_stable_membership_transitions",
        preconditions,
        round_budget,
        round_limit: budget.saturating_mul(2),
        rounds_used: convergence_rounds.saturating_add(operation_rounds),
        fault_cycle: None,
        stable_leader: None,
        proposal: None,
    }
}

fn membership_liveness_target(
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

fn membership_transition_ready_to_leave(
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
