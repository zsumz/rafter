//! Fixture builders shared by the snapshot-application negative controls.
//!
//! Drives a transfer to a partial staged prefix, replays one mutated chunk
//! against it, installs the expected snapshot on the follower, and swaps in a
//! self-consistent but wrong source snapshot for the binding controls.

use super::super::*;
use crate::model_check::{scheduling::Operation, state::apply_to_restart_snapshot_state};

pub(super) fn partial_snapshot_transfer_state() -> RestartSnapshotState {
    let mut state = RestartSnapshotState::snapshot_transfer();
    for _ in 0..256 {
        if state
            .state
            .cluster()
            .node(NodeId(2))
            .pending_snapshot_transfer()
            .is_some_and(|pending| pending.received_bytes() > 0)
        {
            return state;
        }
        apply_to_restart_snapshot_state(&mut state, Operation::DeliverReadyAt(0), &[])
            .expect("snapshot transfer setup remains safe");
    }
    panic!("snapshot transfer did not reach a partial staged prefix");
}

pub(super) fn partial_request(
    pending: &PendingSnapshotTransfer,
    offset: u64,
    crc32: u32,
) -> rafter::InstallSnapshotChunk {
    rafter::InstallSnapshotChunk {
        term: Term(2),
        leader_id: pending.leader_id,
        transfer_id: pending.transfer_id,
        metadata: pending.metadata.clone(),
        total_payload_len: pending.total_payload_len,
        application_payload_crc32: crc32,
        offset,
        chunk: vec![0xA5],
        done: false,
    }
}

pub(super) fn record_mutated_partial_chunk(
    request: rafter::InstallSnapshotChunk,
) -> ExplorationState {
    let transfer = partial_snapshot_transfer_state();
    let before = transfer.state.cluster().clone();
    let mut after = before.clone();
    let mut advanced = after
        .node(NodeId(2))
        .pending_snapshot_transfer()
        .expect("fixture starts with a partial transfer");
    advanced.received_len += request.chunk.len() as u64;
    after
        .snapshot_staging
        .get_mut(&NodeId(2))
        .expect("partial transfer has staged bytes")
        .bytes
        .extend_from_slice(&request.chunk);
    after
        .node_mut(NodeId(2))
        .resume_pending_snapshot_transfer(advanced)
        .expect("mutated post-state remains individually valid");
    let delivered = Envelope {
        from: request.leader_id,
        to: NodeId(2),
        message: Message::InstallSnapshotChunk(request),
    };
    let mut state = ExplorationState::new(after);
    state.record_snapshot_transition(&before, Some(&delivered));
    state
}

pub(super) fn install_expected_snapshot_on_follower(state: &mut RestartSnapshotState) {
    let expected_index = state
        .expected_snapshot
        .as_ref()
        .expect("fixture has an expected snapshot")
        .snapshot
        .metadata
        .last_included_index;
    for _ in 0..256 {
        if state.state.cluster().node(NodeId(2)).snapshot_index() >= expected_index {
            return;
        }
        apply_to_restart_snapshot_state(state, Operation::DeliverReadyAt(0), &[])
            .expect("snapshot installation remains safe");
    }
    panic!("snapshot transfer did not install on the follower");
}

pub(super) fn install_mutated_source_snapshot(
    state: &mut RestartSnapshotState,
    snapshot: rafter::RaftSnapshot,
    payload: Vec<u8>,
) {
    state
        .state
        .inject_snapshot_payload(NodeId(1), &snapshot, payload);
    state
        .state
        .inject_bootstrap_state(NodeId(1), bootstrap_with_snapshot(Term(2), snapshot, &[]))
        .expect("mutated source snapshot remains structurally valid");
    state.state.refresh_snapshot_history();
}
