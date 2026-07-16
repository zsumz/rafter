//! Snapshot transfer, metadata reconstruction, and unsupported-form scenarios.

use rafter::{
    InstallSnapshot, InstallSnapshotChunk, InstallSnapshotResponse, LogIndex, Message, NodeId,
    SnapshotTransferId, Term,
};

use super::support::{round_trip, test_snapshot};
use crate::{encode_message, EncodePeerMessageError};

#[test]
fn install_snapshot_chunk_round_trips_with_opaque_chunk_payload() {
    round_trip(Message::InstallSnapshotChunk(InstallSnapshotChunk {
        term: Term(9),
        leader_id: NodeId(1),
        transfer_id: SnapshotTransferId(123_456),
        metadata: test_snapshot().metadata,
        total_payload_len: 99,
        application_payload_crc32: 0x1234_abcd,
        offset: 64,
        chunk: vec![0, 1, 2, 250, 255],
        done: false,
    }));
}

#[test]
fn install_snapshot_response_round_trips_with_and_without_transfer_id() {
    for transfer_id in [None, Some(SnapshotTransferId(123_456))] {
        round_trip(Message::InstallSnapshotResponse(InstallSnapshotResponse {
            term: Term(9),
            follower_id: NodeId(2),
            success: true,
            last_included_index: LogIndex(42),
            transfer_id,
            next_offset: 17,
        }));
    }
}

#[test]
fn encode_rejects_whole_install_snapshot_peer_frame() {
    let message = Message::InstallSnapshot(InstallSnapshot {
        term: Term(9),
        leader_id: NodeId(1),
        metadata: test_snapshot().metadata,
        application_payload: b"snapshot bytes".to_vec(),
    });

    assert_eq!(
        encode_message(&message),
        Err(EncodePeerMessageError::UnsupportedMessage {
            message: "InstallSnapshot",
            reason: "use InstallSnapshotChunk for peer transport",
        })
    );
}
