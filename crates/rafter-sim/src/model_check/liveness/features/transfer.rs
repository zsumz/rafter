use std::collections::BTreeSet;

use rafter::NodeId;

use super::super::driver::{
    check_soak_safety, drive_liveness_rounds_until_observed, drive_until_stable_leader,
    quiescent_leader, soak_liveness_coverage_failure, soak_liveness_harness_error,
    soak_liveness_invariant_failure, LivenessRoundBudget,
};
use super::{
    FaultStateRequirement, LivenessFeatureReport, LivenessPreconditionProbe, LivenessPreconditions,
    OperationTerminalOutcome, TerminalEvidenceRecorder, TerminalRecorderMode,
    LV_03_TRANSFER_CLAUSE_IDS,
};
use crate::model_check::{
    catalog,
    scheduling::Operation,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::apply_to_state,
    state::ExplorationState,
};
use crate::records::TransferRejected;

pub(super) fn run_leadership_transfer_liveness_check(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    convergence_budget: usize,
    operation_budget: usize,
) -> Result<LivenessFeatureReport, SoakFailure> {
    run_leadership_transfer_liveness_detector(
        state,
        config,
        trace,
        observed_actions,
        convergence_budget,
        operation_budget,
        TerminalRecorderMode::Production,
    )
}

pub(super) fn run_leadership_transfer_liveness_detector(
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
                "no leader elected within {convergence_budget} leadership-transfer liveness rounds"
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

    let Some(target) = leadership_transfer_liveness_target(state, leader) else {
        return Err(soak_liveness_coverage_failure(
            state,
            config,
            trace,
            catalog::LV_03_FEATURE_OPERATION_PROGRESS,
            "leadership-transfer liveness precondition was not reached: no caught-up voter"
                .to_owned(),
        ));
    };

    let rejection_floor = state.cluster().transfer_rejections().len();
    issue_liveness_transfer(state, leader, target);
    trace.push(SoakAction::Transfer {
        from: leader,
        target,
    });
    observed_actions.insert(SoakActionKind::Transfer);
    check_soak_safety(state, config, trace)?;

    let mut terminal_recorder = TerminalEvidenceRecorder::new(
        format!("transfer:{}->{}", leader.0, target.0),
        recorder_mode,
    );
    let completion = drive_liveness_rounds_until_observed(
        state,
        config,
        trace,
        observed_actions,
        operation_budget,
        |state| {
            terminal_recorder.observe(leadership_transfer_outcome(
                state,
                rejection_floor,
                leader,
                target,
            ))
        },
        |_| true,
    )?;
    if completion.completed {
        let Some(operation) = terminal_recorder.evidence() else {
            return Err(soak_liveness_harness_error(
                state,
                config,
                trace,
                "terminal recorder reported leadership-transfer completion without evidence",
            ));
        };
        Ok(LivenessFeatureReport {
            invariant_id: "LV-03",
            clause_ids: LV_03_TRANSFER_CLAUSE_IDS,
            feature_id: "leadership-transfer",
            scenario_id: "caught-up-voter-transfer-v1",
            observation_id: "terminated_target_leadership_transfers",
            preconditions,
            round_budget,
            round_limit: convergence_budget.saturating_add(operation_budget),
            rounds_used: convergence
                .rounds_used
                .saturating_add(completion.rounds_used),
            fault_cycle: None,
            stable_leader: None,
            proposal: None,
            operation: Some(operation),
        })
    } else {
        Err(soak_liveness_invariant_failure(
            state,
            config,
            trace,
            catalog::LV_03_FEATURE_OPERATION_PROGRESS,
            format!(
                "leadership transfer {leader}->{target} did not make the target the quiescent leader within {operation_budget} post-heal rounds"
            ),
        ))
    }
}

fn leadership_transfer_outcome(
    state: &ExplorationState,
    rejection_floor: usize,
    leader: NodeId,
    target: NodeId,
) -> Option<OperationTerminalOutcome> {
    if quiescent_leader(state) == Some(target) {
        Some(OperationTerminalOutcome::Completed)
    } else if leadership_transfer_rejected(state, rejection_floor, leader, target) {
        Some(OperationTerminalOutcome::Rejected)
    } else {
        None
    }
}

fn leadership_transfer_rejected(
    state: &ExplorationState,
    rejection_floor: usize,
    leader: NodeId,
    target: NodeId,
) -> bool {
    transfer_rejection_observed(
        state.cluster().transfer_rejections(),
        rejection_floor,
        leader,
        target,
    )
}

fn transfer_rejection_observed(
    rejections: &[TransferRejected],
    rejection_floor: usize,
    leader: NodeId,
    target: NodeId,
) -> bool {
    rejections[rejection_floor..]
        .iter()
        .any(|rejection| rejection.node_id == leader && rejection.target == target)
}

fn leadership_transfer_liveness_target(state: &ExplorationState, leader: NodeId) -> Option<NodeId> {
    let membership = state.cluster().effective_membership(leader);
    let leader_last_log_index = state.cluster().last_log_index(leader);
    state
        .cluster()
        .leader_replication_progress(leader)
        .into_iter()
        .filter(|progress| {
            progress.follower_id != leader
                && membership.contains_voter(progress.follower_id)
                && progress.match_index == leader_last_log_index
        })
        .map(|progress| progress.follower_id)
        .min_by_key(|node_id| node_id.0)
}

fn issue_liveness_transfer(state: &mut ExplorationState, leader: NodeId, target: NodeId) {
    apply_to_state(
        state,
        Operation::Transfer {
            from: leader,
            target,
        },
    );
}

#[cfg(test)]
mod tests {
    use rafter::NodeId;

    use super::transfer_rejection_observed;
    use crate::records::TransferRejected;

    #[test]
    fn explicit_transfer_rejection_matches_only_the_exact_request_after_its_floor() {
        let rejections = [TransferRejected {
            node_id: NodeId(1),
            target: NodeId(2),
        }];

        assert!(transfer_rejection_observed(
            &rejections,
            0,
            NodeId(1),
            NodeId(2)
        ));
        assert!(!transfer_rejection_observed(
            &rejections,
            1,
            NodeId(1),
            NodeId(2)
        ));
        assert!(!transfer_rejection_observed(
            &rejections,
            0,
            NodeId(1),
            NodeId(3)
        ));
        assert!(!transfer_rejection_observed(
            &rejections,
            0,
            NodeId(2),
            NodeId(2)
        ));
    }
}
