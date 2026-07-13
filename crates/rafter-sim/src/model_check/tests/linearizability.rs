use rafter::{LogIndex, NodeId, SharedPayload};

use super::super::linearizability::check_client_history_linearizable;
use super::super::state::{
    ClientHistory, ClientRead, ClientReadOutcome, ClientReadProof, ClientWrite, ClientWriteStatus,
    ClientWriteUnknownReason,
};
use super::super::ProposalId;
use super::super::{
    helpers::{deliver_all_in_state, elect_node_one_in_state, three_node_configs},
    scheduling::Operation,
    state::ExplorationState,
};
use crate::Cluster;

#[test]
fn linearizer_rejects_read_that_misses_completed_write() {
    let mut state = ExplorationState::new(Cluster::new(three_node_configs()));
    elect_node_one_in_state(&mut state);
    super::super::state::apply_to_state(
        &mut state,
        Operation::Propose {
            to: NodeId(1),
            proposal_id: ProposalId(1),
            stale_leader: false,
        },
    );
    deliver_all_in_state(&mut state);
    assert!(matches!(
        state.client_history().writes[&ProposalId(1)].status,
        ClientWriteStatus::Completed { .. }
    ));

    let read_index = state.cluster().local_applied_index(NodeId(1));
    state.record_client_read(NodeId(1), 1, read_index);
    state
        .record_client_read_completion_corruption(
            1,
            ClientReadProof {
                application_epoch: state.cluster().application_epoch(NodeId(1)),
                read_index,
                local_applied_index: read_index,
            },
            None,
        )
        .expect("registered read is available to the recorder corruption fixture");

    let error = check_client_history_linearizable(state.client_history())
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

#[test]
fn linearizer_can_include_unknown_write_to_explain_later_read() {
    let mut history = ClientHistory::default();
    insert_unknown_write(&mut history, ProposalId(1), 0, b"maybe");
    insert_completed_read(&mut history, 1, 1, 2, LogIndex(1), Some(payload(b"maybe")));

    check_client_history_linearizable(&history)
        .expect("an unknown write may explain a later observed value");
}

#[test]
fn linearizer_rejects_history_that_requires_and_then_forgets_unknown_write() {
    let mut history = ClientHistory::default();
    insert_unknown_write(&mut history, ProposalId(1), 0, b"maybe");
    insert_completed_read(&mut history, 1, 1, 2, LogIndex(1), Some(payload(b"maybe")));
    insert_completed_read(&mut history, 2, 3, 4, LogIndex(1), None);

    let error = check_client_history_linearizable(&history)
        .expect_err("an included unknown write cannot later disappear");
    assert!(error.contains("not linearizable"));
    assert!(error.contains("optional write 1"));
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

fn insert_unknown_write(
    history: &mut ClientHistory,
    proposal_id: ProposalId,
    started_at: u64,
    value: &[u8],
) {
    history.writes.insert(
        proposal_id,
        ClientWrite {
            proposal_id,
            node_id: NodeId(1),
            payload: payload(value),
            started_at,
            status: ClientWriteStatus::Unknown {
                reason: ClientWriteUnknownReason::StaleLeader,
            },
        },
    );
}

fn payload(value: &[u8]) -> SharedPayload {
    value.into()
}
