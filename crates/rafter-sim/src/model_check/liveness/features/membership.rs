use std::collections::BTreeSet;

use rafter::{MembershipConfig, MembershipSet, NodeId};

use super::super::driver::{
    check_soak_safety, drive_soak_liveness_round_until_terminal, drive_until_stable_leader,
    quiescent_leader, soak_liveness_coverage_failure, soak_liveness_harness_error,
    soak_liveness_invariant_failure, FairRoundDriver, LivenessRoundBudget,
};
use super::{
    FaultStateRequirement, LivenessFeatureReport, LivenessPreconditionProbe, LivenessPreconditions,
    OperationEvidence, OperationTerminalOutcome, TerminalEvidenceRecorder, TerminalRecorderMode,
    LV_03_MEMBERSHIP_CLAUSE_IDS,
};
use crate::model_check::{
    catalog,
    scheduling::Operation,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::apply_to_state,
    state::ExplorationState,
};
use crate::records::ProposalRejected;

pub(super) fn run_membership_transition_liveness_check(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    convergence_budget: usize,
    operation_budget: usize,
) -> Result<LivenessFeatureReport, SoakFailure> {
    run_membership_transition_liveness_detector(
        state,
        config,
        trace,
        observed_actions,
        convergence_budget,
        operation_budget,
        TerminalRecorderMode::Production,
    )
}

pub(super) fn run_membership_transition_liveness_detector(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    convergence_budget: usize,
    operation_budget: usize,
    recorder_mode: TerminalRecorderMode,
) -> Result<LivenessFeatureReport, SoakFailure> {
    let round_budget = LivenessRoundBudget::capture(state, config, 2);
    let Some(convergence) =
        drive_until_stable_leader(state, config, trace, observed_actions, convergence_budget)?
    else {
        return Err(soak_liveness_coverage_failure(
            state,
            config,
            trace,
            catalog::LV_01_POST_HEAL_LEADER_CONVERGENCE,
            format!(
                "no leader elected within {convergence_budget} membership-transition liveness rounds"
            ),
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
        return Err(soak_liveness_coverage_failure(
            state,
            config,
            trace,
            catalog::LV_03_FEATURE_OPERATION_PROGRESS,
            "membership liveness precondition was not reached: no removable voter".to_owned(),
        ));
    };

    let monitor = MembershipMonitor {
        leader,
        removed_voter,
        target,
        rejection_floor: state.cluster().proposal_rejections().len(),
        convergence_budget,
        operation_budget,
        round_budget,
        convergence_rounds: convergence.rounds_used,
        preconditions,
        recorder_mode,
    };
    run_membership_operation(state, config, trace, observed_actions, &monitor)
}

struct MembershipMonitor {
    leader: NodeId,
    removed_voter: NodeId,
    target: MembershipSet,
    rejection_floor: usize,
    convergence_budget: usize,
    operation_budget: usize,
    round_budget: LivenessRoundBudget,
    convergence_rounds: usize,
    preconditions: LivenessPreconditions,
    recorder_mode: TerminalRecorderMode,
}

impl MembershipMonitor {
    fn report(
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

    fn outcome(
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

fn run_membership_operation(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    monitor: &MembershipMonitor,
) -> Result<LivenessFeatureReport, SoakFailure> {
    let mut terminal_recorder = TerminalEvidenceRecorder::new(
        format!(
            "remove-voter:{}:{}",
            monitor.leader.0, monitor.removed_voter.0
        ),
        monitor.recorder_mode,
    );
    apply_to_state(
        state,
        Operation::RemoveVoter {
            to: monitor.leader,
            voter_id: monitor.removed_voter,
        },
    );
    trace.push(SoakAction::RemoveVoter {
        to: monitor.leader,
        voter_id: monitor.removed_voter,
    });
    observed_actions.insert(SoakActionKind::RemoveVoter);
    check_soak_safety(state, config, trace)?;
    if terminal_recorder.observe(monitor.outcome(state, monitor.leader)) {
        return report_recorded_membership_operation(
            state,
            config,
            trace,
            monitor,
            &terminal_recorder,
            0,
        );
    }

    let mut leave_issued = false;
    let mut fair_rounds = FairRoundDriver::new(config.seed);
    for round in 0..monitor.operation_budget {
        if terminal_recorder.observe(monitor.outcome(state, monitor.leader)) {
            return report_recorded_membership_operation(
                state,
                config,
                trace,
                monitor,
                &terminal_recorder,
                operation_rounds(round, false),
            );
        }

        if !leave_issued {
            if let Some(leave_leader) =
                issue_leave_joint_if_ready(state, config, trace, observed_actions, monitor)?
            {
                leave_issued = true;
                if terminal_recorder.observe(monitor.outcome(state, leave_leader)) {
                    return report_recorded_membership_operation(
                        state,
                        config,
                        trace,
                        monitor,
                        &terminal_recorder,
                        operation_rounds(round, false),
                    );
                }
            }
        }

        let terminal_latched = drive_soak_liveness_round_until_terminal(
            &mut fair_rounds,
            state,
            config,
            trace,
            observed_actions,
            round,
            |state| terminal_recorder.observe(monitor.outcome(state, monitor.leader)),
        )?;
        check_soak_safety(state, config, trace)?;
        if terminal_latched {
            return report_recorded_membership_operation(
                state,
                config,
                trace,
                monitor,
                &terminal_recorder,
                operation_rounds(round, true),
            );
        }
    }

    Err(soak_liveness_invariant_failure(
        state,
        config,
        trace,
        catalog::LV_03_FEATURE_OPERATION_PROGRESS,
        format!(
            "membership transition removing {} did not reach stable target {:?} within {} post-heal rounds",
            monitor.removed_voter, monitor.target, monitor.operation_budget
        ),
    ))
}

fn report_recorded_membership_operation(
    state: &ExplorationState,
    config: SoakConfig,
    trace: &[SoakAction],
    monitor: &MembershipMonitor,
    recorder: &TerminalEvidenceRecorder,
    operation_rounds: usize,
) -> Result<LivenessFeatureReport, SoakFailure> {
    let Some(operation) = recorder.evidence() else {
        return Err(soak_liveness_harness_error(
            state,
            config,
            trace,
            "membership terminal recorder completed without evidence",
        ));
    };
    Ok(monitor.report(operation_rounds, operation))
}

fn issue_leave_joint_if_ready(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    monitor: &MembershipMonitor,
) -> Result<Option<NodeId>, SoakFailure> {
    let Some(leader) = membership_transition_ready_to_leave(state, &monitor.target) else {
        return Ok(None);
    };
    apply_to_state(state, Operation::LeaveJoint { to: leader });
    trace.push(SoakAction::LeaveJoint { to: leader });
    observed_actions.insert(SoakActionKind::LeaveJoint);
    check_soak_safety(state, config, trace)?;
    Ok(Some(leader))
}

const fn operation_rounds(round: usize, drove_round: bool) -> usize {
    round.saturating_add(drove_round as usize)
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

fn membership_rejection_observed(
    rejections: &[ProposalRejected],
    rejection_floor: usize,
    leader: NodeId,
) -> bool {
    rejections[rejection_floor..]
        .iter()
        .any(|rejection| rejection.node_id == leader && rejection.proposal_id.is_none())
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

#[cfg(test)]
mod tests {
    use rafter::{LocalProposalId, NodeId};

    use super::{membership_rejection_observed, operation_rounds};
    use crate::records::ProposalRejected;

    #[test]
    fn completion_after_final_driven_round_is_within_the_exact_bound() {
        let budget = 8;
        assert_eq!(operation_rounds(budget - 1, true), budget);
        assert_eq!(operation_rounds(budget - 1, false), budget - 1);
    }

    #[test]
    fn explicit_configuration_rejection_matches_only_after_its_floor() {
        let rejections = [
            ProposalRejected {
                node_id: NodeId(1),
                proposal_id: Some(LocalProposalId(7)),
            },
            ProposalRejected {
                node_id: NodeId(1),
                proposal_id: None,
            },
        ];

        assert!(membership_rejection_observed(&rejections, 1, NodeId(1)));
        assert!(!membership_rejection_observed(&rejections, 2, NodeId(1)));
        assert!(!membership_rejection_observed(&rejections, 0, NodeId(2)));
    }
}
