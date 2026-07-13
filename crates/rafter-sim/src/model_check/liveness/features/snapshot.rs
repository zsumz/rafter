use std::collections::BTreeSet;

use rafter::NodeId;

use super::super::driver::{
    check_soak_safety, drive_liveness_rounds_until_observed, single_leader,
    soak_liveness_invariant_failure, soak_liveness_round_budget, LivenessRoundBudget,
};

pub(super) fn snapshot_liveness_round_budget(config: SoakConfig) -> usize {
    let snapshot_state = RestartSnapshotState::snapshot_transfer();
    soak_liveness_round_budget(&snapshot_state.state, config)
}
use super::{
    FaultStateRequirement, LivenessFeatureReport, LivenessPreconditionProbe, LivenessPreconditions,
    OperationTerminalOutcome, TerminalEvidenceRecorder, TerminalRecorderMode,
    LV_03_SNAPSHOT_CLAUSE_IDS,
};
use crate::model_check::{
    catalog,
    soak::{SoakConfig, SoakFailure},
    state::{self, ExplorationState, RestartSnapshotState},
};

pub(in crate::model_check) fn run_snapshot_catchup_liveness_check(
    config: SoakConfig,
    budget: usize,
) -> Result<LivenessFeatureReport, SoakFailure> {
    run_snapshot_catchup_liveness_detector(config, budget, TerminalRecorderMode::Production)
}

pub(super) fn run_snapshot_catchup_liveness_detector(
    config: SoakConfig,
    budget: usize,
    recorder_mode: TerminalRecorderMode,
) -> Result<LivenessFeatureReport, SoakFailure> {
    let mut snapshot_state = RestartSnapshotState::snapshot_transfer();
    let round_budget = LivenessRoundBudget::capture(&snapshot_state.state, config, 1);
    let expected = snapshot_state
        .expected_snapshot
        .clone()
        .expect("snapshot liveness fixture declares expected snapshot");
    let mut trace = Vec::new();
    let mut observed_actions = BTreeSet::new();
    let mut terminal_recorder = TerminalEvidenceRecorder::new(
        format!("snapshot:{}", expected.snapshot.transfer_id()),
        recorder_mode,
    );

    check_soak_safety(&snapshot_state.state, config, &trace)?;
    let completion = drive_liveness_rounds_until_observed(
        &mut snapshot_state.state,
        config,
        &mut trace,
        &mut observed_actions,
        budget,
        |state| {
            terminal_recorder.observe(
                snapshot_catchup_completed(state, &expected)
                    .then_some(OperationTerminalOutcome::Installed),
            )
        },
        |_| true,
    )?;
    if completion.completed {
        let operation = terminal_recorder.evidence().ok_or_else(|| {
            super::super::driver::soak_liveness_harness_error(
                &snapshot_state.state,
                config,
                &trace,
                "terminal recorder reported snapshot completion without evidence",
            )
        })?;
        Ok(LivenessFeatureReport {
            invariant_id: "LV-03",
            clause_ids: LV_03_SNAPSHOT_CLAUSE_IDS,
            feature_id: "snapshot-catch-up",
            scenario_id: "restart-snapshot-transfer-v1",
            observation_id: "completed_expected_snapshot_catchups",
            preconditions: LivenessPreconditions::capture(
                &snapshot_state.state,
                LivenessPreconditionProbe {
                    leader: single_leader(&snapshot_state.state),
                    fault_requirement: FaultStateRequirement::Stopped,
                    stable_leader_observed: None,
                    accepted_proposal_observed: None,
                    authority_loss_observed: None,
                },
            ),
            round_budget,
            round_limit: budget,
            rounds_used: completion.rounds_used,
            fault_cycle: None,
            stable_leader: None,
            proposal: None,
            operation: Some(operation),
        })
    } else {
        Err(soak_liveness_invariant_failure(
            &snapshot_state.state,
            config,
            &trace,
            catalog::LV_03_FEATURE_OPERATION_PROGRESS,
            format!(
                "snapshot catch-up to node 2 did not install snapshot {} within {budget} bounded rounds",
                expected.snapshot.transfer_id()
            ),
        ))
    }
}

fn snapshot_catchup_completed(
    state: &ExplorationState,
    expected: &state::ExpectedSnapshot,
) -> bool {
    let follower = NodeId(2);
    state.cluster().bootstrap_state(follower).snapshot == Some(expected.snapshot.clone())
        && state
            .cluster()
            .snapshot_payload(follower, &expected.snapshot)
            .is_some_and(|payload| payload == expected.payload.as_ref())
        && state.cluster().snapshot_installs().iter().any(|install| {
            install.node_id == follower
                && install.last_included_index == expected.snapshot.metadata.last_included_index
                && install.last_included_term == expected.snapshot.metadata.last_included_term
        })
}
