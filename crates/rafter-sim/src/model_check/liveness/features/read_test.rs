//! What counts as a terminal outcome for the LV-03 read-barrier detector.
//!
//! Completion, rejection, and cancellation all terminate a read and a still
//! pending outcome does not, and a stale terminal output must not terminate a
//! reused read identity — otherwise a replayed history would satisfy liveness
//! for a read that never finished.

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
