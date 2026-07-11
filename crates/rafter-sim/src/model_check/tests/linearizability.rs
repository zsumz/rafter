use rafter::{LogIndex, NodeId, SharedPayload};

use super::super::linearizability::check_client_history_linearizable;
use super::super::state::{
    ClientHistory, ClientRead, ClientReadOutcome, ClientReadProof, ClientWrite, ClientWriteStatus,
};
use super::super::ProposalId;

#[test]
fn linearizer_rejects_read_that_misses_completed_write() {
    let mut history = ClientHistory::default();
    insert_completed_write(&mut history, ProposalId(1), 0, 1, LogIndex(1), b"one");
    insert_completed_read(&mut history, 1, 2, 3, LogIndex(1), None);

    let error = check_client_history_linearizable(&history)
        .expect_err("read after completed write must observe the register value");

    assert!(error.contains("not linearizable"));
    assert!(error.contains("write 1"));
    assert!(error.contains("read 1"));
}

#[test]
fn linearizer_accepts_read_ordered_before_overlapping_write() {
    let mut history = ClientHistory::default();
    insert_completed_write(&mut history, ProposalId(1), 0, 4, LogIndex(1), b"one");
    insert_completed_read(&mut history, 1, 1, 2, LogIndex(0), None);

    check_client_history_linearizable(&history)
        .expect("overlapping read may linearize before the write");
}

#[test]
fn linearizer_accepts_read_ordered_after_overlapping_write() {
    let mut history = ClientHistory::default();
    let value = payload(b"one");
    insert_completed_write(&mut history, ProposalId(1), 0, 4, LogIndex(1), b"one");
    insert_completed_read(&mut history, 1, 1, 2, LogIndex(1), Some(value));

    check_client_history_linearizable(&history)
        .expect("overlapping read may linearize after the write");
}

#[test]
fn linearizer_accepts_read_of_initial_register_value() {
    let mut history = ClientHistory {
        initial_value: Some(payload(b"seed")),
        ..ClientHistory::default()
    };
    insert_completed_read(&mut history, 1, 0, 1, LogIndex(1), Some(payload(b"seed")));

    check_client_history_linearizable(&history)
        .expect("reads may return the value established before history recording");
}

fn insert_completed_write(
    history: &mut ClientHistory,
    proposal_id: ProposalId,
    started_at: u64,
    completed_at: u64,
    index: LogIndex,
    value: &[u8],
) {
    history.writes.insert(
        proposal_id,
        ClientWrite {
            proposal_id,
            node_id: NodeId(1),
            payload: payload(value),
            started_at,
            status: ClientWriteStatus::Completed {
                node_id: NodeId(1),
                index,
                completed_at,
            },
        },
    );
}

fn insert_completed_read(
    history: &mut ClientHistory,
    request_id: u64,
    started_at: u64,
    completed_at: u64,
    read_index: LogIndex,
    result: Option<SharedPayload>,
) {
    history.reads.insert(
        request_id,
        ClientRead {
            node_id: NodeId(1),
            request_id,
            committed_floor: read_index,
            started_at,
            outcome: ClientReadOutcome::Completed {
                proof: ClientReadProof {
                    application_epoch: 0,
                    read_index,
                    local_applied_index: read_index,
                },
                result,
                completed_at,
            },
        },
    );
}

fn payload(value: &[u8]) -> SharedPayload {
    value.into()
}
