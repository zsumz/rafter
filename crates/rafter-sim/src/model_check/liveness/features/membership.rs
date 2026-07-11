use std::collections::BTreeSet;

use rafter::{MembershipConfig, MembershipSet, NodeId};

use super::super::driver::{
    check_soak_safety, drive_soak_liveness_round, drive_until_quiescent_leader, quiescent_leader,
    soak_liveness_failure,
};
use crate::model_check::{
    application::apply_to_state,
    catalog,
    scheduling::Operation,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::ExplorationState,
};

pub(super) fn run_membership_transition_liveness_check(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    budget: usize,
) -> Result<(), SoakFailure> {
    let Some(leader) =
        drive_until_quiescent_leader(state, config, trace, observed_actions, budget)?
    else {
        return Err(soak_liveness_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            format!("no leader elected within {budget} membership-transition liveness rounds"),
        ));
    };

    let Some((removed_voter, target)) = membership_liveness_target(state, leader) else {
        return Ok(());
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
            return Ok(());
        }

        if !leave_issued {
            if let Some(leader) = membership_transition_ready_to_leave(state, &target) {
                apply_to_state(state, Operation::LeaveJoint { to: leader });
                trace.push(SoakAction::LeaveJoint { to: leader });
                observed_actions.insert(SoakActionKind::LeaveJoint);
                leave_issued = true;
                check_soak_safety(state, config, trace)?;
                if membership_transition_completed(state, &target) {
                    return Ok(());
                }
            }
        }

        drive_soak_liveness_round(state, trace, observed_actions, round);
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

fn membership_liveness_target(
    state: &ExplorationState,
    leader: NodeId,
) -> Option<(NodeId, MembershipSet)> {
    let MembershipConfig::Stable(current) = state.cluster.effective_membership(leader) else {
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
    let effective = state.cluster.effective_membership(leader);
    let committed = state.cluster.committed_membership(leader);
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
    stable_membership_matches(&state.cluster.effective_membership(leader), target)
        && stable_membership_matches(&state.cluster.committed_membership(leader), target)
}

fn stable_membership_matches(config: &MembershipConfig, target: &MembershipSet) -> bool {
    matches!(config, MembershipConfig::Stable(membership) if membership == target)
}
