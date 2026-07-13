use std::collections::BTreeSet;

use super::super::driver::{
    check_soak_safety, drive_liveness_rounds_until_observed, drive_until_stable_leader,
    single_leader, soak_liveness_coverage_failure, soak_liveness_harness_error,
    soak_liveness_invariant_failure, LivenessRoundBudget, StableLeaderGuard,
};
use super::{
    FaultStateRequirement, LivenessFeatureReport, LivenessPreconditionProbe, LivenessPreconditions,
    OperationTerminalOutcome, StableLeaderEvidence, TerminalEvidenceRecorder, TerminalRecorderMode,
    LV_03_READ_CLAUSE_IDS,
};
use crate::model_check::{
    catalog,
    scheduling::Operation,
    soak::{SoakAction, SoakActionKind, SoakConfig, SoakFailure},
    state::apply_to_state,
    state::{ClientReadOutcome, ExplorationState},
};

pub(super) fn run_read_barrier_liveness_check(
    state: &mut ExplorationState,
    config: SoakConfig,
    trace: &mut Vec<SoakAction>,
    observed_actions: &mut BTreeSet<SoakActionKind>,
    convergence_budget: usize,
    operation_budget: usize,
) -> Result<LivenessFeatureReport, SoakFailure> {
    run_read_barrier_liveness_detector(
        state,
        config,
        trace,
        observed_actions,
        convergence_budget,
        operation_budget,
        TerminalRecorderMode::Production,
    )
}

pub(super) fn run_read_barrier_liveness_detector(
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
            format!("no leader elected within {convergence_budget} read-barrier liveness rounds"),
        ));
    };
    let leader = convergence.leader;

    let request_id = state.read_indexes_issued() + 1;
    let read_operation_id = state.cluster().read_registrations().len() as u64;
    apply_to_state(
        state,
        Operation::ReadIndex {
            to: leader,
            request_id,
        },
    );
    trace.push(SoakAction::ReadIndex {
        to: leader,
        request_id,
    });
    observed_actions.insert(SoakActionKind::ReadIndex);
    check_soak_safety(state, config, trace)?;

    let mut terminal_recorder =
        TerminalEvidenceRecorder::new(format!("read:{read_operation_id}"), recorder_mode);
    let mut guard = StableLeaderGuard::new(leader, operation_budget);
    let completion = drive_liveness_rounds_until_observed(
        state,
        config,
        trace,
        observed_actions,
        operation_budget,
        |state| terminal_recorder.observe(liveness_read_outcome(state, read_operation_id)),
        |state| guard.observe(single_leader(state)).is_ok(),
    )?;
    if !completion.observer_held {
        return Err(soak_liveness_coverage_failure(
            state,
            config,
            trace,
            catalog::LV_03_FEATURE_OPERATION_PROGRESS,
            format!(
                "stable-leader premise for read-index request {request_id} was lost during the bounded operation window"
            ),
        ));
    }
    if completion.completed {
        let Some(operation) = terminal_recorder.evidence() else {
            return Err(soak_liveness_harness_error(
                state,
                config,
                trace,
                "terminal recorder reported read completion without evidence",
            ));
        };
        return Ok(LivenessFeatureReport {
            invariant_id: "LV-03",
            clause_ids: LV_03_READ_CLAUSE_IDS,
            feature_id: "read-barrier",
            scenario_id: "stable-leader-read-barrier-v1",
            observation_id: "terminated_liveness_read_barriers",
            preconditions: LivenessPreconditions::capture(
                state,
                LivenessPreconditionProbe {
                    leader: Some(leader),
                    fault_requirement: FaultStateRequirement::Stopped,
                    stable_leader_observed: Some(true),
                    accepted_proposal_observed: None,
                    authority_loss_observed: None,
                },
            ),
            round_budget,
            round_limit: convergence_budget.saturating_add(operation_budget),
            rounds_used: convergence
                .rounds_used
                .saturating_add(completion.rounds_used),
            fault_cycle: None,
            stable_leader: Some(StableLeaderEvidence {
                leader,
                stable_rounds: convergence.stable_rounds,
                remained_leader_through_probe: true,
            }),
            proposal: None,
            operation: Some(operation),
        });
    }
    Err(soak_liveness_invariant_failure(
        state,
        config,
        trace,
        catalog::LV_03_FEATURE_OPERATION_PROGRESS,
        format!(
            "read-index request {request_id} to leader {leader} did not complete or terminate explicitly within {operation_budget} post-heal rounds"
        ),
    ))
}

fn liveness_read_outcome(
    state: &ExplorationState,
    operation_id: u64,
) -> Option<OperationTerminalOutcome> {
    state
        .client_history()
        .reads
        .get(&operation_id)
        .and_then(|read| match read.outcome {
            ClientReadOutcome::Completed { .. } => Some(OperationTerminalOutcome::Completed),
            ClientReadOutcome::Rejected { .. } => Some(OperationTerminalOutcome::Rejected),
            ClientReadOutcome::Canceled { .. } => Some(OperationTerminalOutcome::Canceled),
            ClientReadOutcome::Pending | ClientReadOutcome::ProofGranted { .. } => None,
        })
}

#[cfg(test)]
mod tests {
    use rafter::{LogIndex, NodeId, ReadIndexCancelReason};

    use super::{liveness_read_outcome, ClientReadOutcome, ExplorationState};
    use crate::{
        model_check::{
            liveness::features::production_configs,
            scheduling::Operation,
            state::{apply_to_state, ClientReadProof},
        },
        Cluster, SimSeed,
    };

    const REQUEST_ID: u64 = 7;

    #[test]
    fn read_liveness_accepts_completion_rejection_and_cancellation() {
        let mut completed = fresh_state();
        let completed_id = record_read(&mut completed, REQUEST_ID);
        completed
            .record_client_read_completion_corruption(
                completed_id,
                ClientReadProof {
                    application_epoch: 0,
                    read_index: LogIndex::ZERO,
                    local_applied_index: LogIndex::ZERO,
                },
                None,
            )
            .expect("completion fixture should update its pending read");
        assert_eq!(
            liveness_read_outcome(&completed, completed_id),
            Some(super::OperationTerminalOutcome::Completed)
        );

        let mut rejected = fresh_state();
        apply_to_state(
            &mut rejected,
            Operation::ReadIndex {
                to: NodeId(1),
                request_id: REQUEST_ID,
            },
        );
        let rejected_id = *rejected
            .client_history()
            .reads
            .keys()
            .next_back()
            .expect("instrumented read has an operation identity");
        assert!(matches!(
            rejected
                .client_history()
                .reads
                .get(&rejected_id)
                .map(|read| &read.outcome),
            Some(ClientReadOutcome::Rejected { .. })
        ));
        assert_eq!(
            liveness_read_outcome(&rejected, rejected_id),
            Some(super::OperationTerminalOutcome::Rejected)
        );

        let mut canceled = fresh_state();
        let canceled_id = record_read(&mut canceled, REQUEST_ID);
        canceled.inject_read_terminal_output(crate::ReadTerminalOutput::Canceled {
            node_id: NodeId(1),
            operation_id: Some(canceled_id),
            request_id: REQUEST_ID,
            reason: ReadIndexCancelReason::LeadershipLost,
        });
        canceled.refresh_client_history();
        assert!(matches!(
            canceled
                .client_history()
                .reads
                .get(&canceled_id)
                .map(|read| &read.outcome),
            Some(ClientReadOutcome::Canceled { .. })
        ));
        assert_eq!(
            liveness_read_outcome(&canceled, canceled_id),
            Some(super::OperationTerminalOutcome::Canceled)
        );
    }

    #[test]
    fn read_liveness_rejects_a_nonterminal_pending_outcome() {
        let mut state = fresh_state();
        let operation_id = record_read(&mut state, REQUEST_ID);

        assert_eq!(liveness_read_outcome(&state, operation_id), None);
    }

    #[test]
    fn stale_terminal_output_cannot_terminate_a_reused_read_id() {
        let mut state = fresh_state();
        let old_operation_id = record_read(&mut state, REQUEST_ID);
        state.inject_read_terminal_output(crate::ReadTerminalOutput::Canceled {
            node_id: NodeId(1),
            operation_id: Some(old_operation_id),
            request_id: REQUEST_ID,
            reason: ReadIndexCancelReason::LeadershipLost,
        });
        state.refresh_client_history();
        let reused_operation_id = old_operation_id + 1;
        state.record_client_read(&crate::ReadRegistered {
            node_id: NodeId(1),
            operation_id: reused_operation_id,
            request_id: REQUEST_ID,
            committed_floor: LogIndex::ZERO,
        });
        state.refresh_client_history();

        assert!(matches!(
            state
                .client_history()
                .reads
                .get(&old_operation_id)
                .map(|read| &read.outcome),
            Some(ClientReadOutcome::Canceled { .. })
        ));
        assert!(matches!(
            state
                .client_history()
                .reads
                .get(&reused_operation_id)
                .map(|read| &read.outcome),
            Some(ClientReadOutcome::Pending)
        ));
        assert_eq!(liveness_read_outcome(&state, reused_operation_id), None);
    }

    fn record_read(state: &mut ExplorationState, request_id: u64) -> u64 {
        let operation_id = state.client_history().reads.len() as u64;
        state.record_client_read(&crate::ReadRegistered {
            node_id: NodeId(1),
            operation_id,
            request_id,
            committed_floor: LogIndex::ZERO,
        });
        operation_id
    }

    fn fresh_state() -> ExplorationState {
        ExplorationState::new(fresh_cluster())
    }

    fn fresh_cluster() -> Cluster {
        Cluster::new_with_seed(
            production_configs().expect("production liveness configuration should be valid"),
            SimSeed(0x51_7e),
        )
    }
}
