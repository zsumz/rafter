//! Chunk identity, offset, checksum, retransmission, and final installation.

use super::*;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};

/// The chunked path follows the same sender rule as the whole-message one: the
/// boundary a leader relays is not a statement about who may relay it.
#[test]
fn newly_added_leader_relays_an_older_boundary_snapshot_chunk() {
    let mut follower = node(2, &[1, 3, 4]);
    let payload = b"dynamic chunk snapshot".to_vec();
    // Node 4 is a voter in this follower's current membership; the boundary
    // membership it relays predates node 4.
    let snapshot = test_snapshot_with_committed_voters(3, 4, 5, &payload, &[1, 2, 3]);

    let outputs = follower.step(Input::Message {
        from: NodeId(4),
        message: Message::InstallSnapshotChunk(InstallSnapshotChunk {
            term: Term(5),
            leader_id: NodeId(4),
            transfer_id: snapshot.transfer_id(),
            metadata: snapshot.metadata,
            total_payload_len: snapshot.application_payload_len,
            application_payload_crc32: snapshot.application_payload_crc32,
            offset: 0,
            chunk: payload,
            done: true,
        }),
    });

    assert_eq!(follower.current_term(), Term(5));
    assert_eq!(follower.leader_hint(), Some(NodeId(4)));
    assert_eq!(follower.snapshot_index(), LogIndex(3));
    assert!(outputs
        .iter()
        .any(|output| matches!(output, Output::ApplySnapshot { .. })));
    let response = install_snapshot_response_from_outputs(&outputs);
    assert!(response.success);
    assert_eq!(response.last_included_index, LogIndex(3));
    assert_eq!(
        follower
            .snapshot_transfer_status()
            .rejected_chunks
            .metadata_mismatch,
        0
    );
}

/// The sender check that survives: a node this follower cannot place in any
/// membership it can currently see is still refused, and the refusal is still
/// accounted as a metadata-mismatch rejection.
#[test]
fn sender_outside_every_visible_membership_is_rejected() {
    let mut follower = node(2, &[1, 3]);
    let payload = b"stranger chunk snapshot".to_vec();
    let snapshot = test_snapshot_with_committed_voters(3, 4, 5, &payload, &[1, 2, 3]);

    let outputs = follower.step(Input::Message {
        from: NodeId(9),
        message: Message::InstallSnapshotChunk(InstallSnapshotChunk {
            term: Term(5),
            leader_id: NodeId(9),
            transfer_id: snapshot.transfer_id(),
            metadata: snapshot.metadata,
            total_payload_len: snapshot.application_payload_len,
            application_payload_crc32: snapshot.application_payload_crc32,
            offset: 0,
            chunk: payload,
            done: true,
        }),
    });

    assert_eq!(follower.current_term(), Term(5));
    assert_eq!(follower.leader_hint(), None);
    assert_eq!(follower.snapshot_index(), LogIndex::ZERO);
    assert!(follower.pending_snapshot_transfer().is_none());
    assert!(outputs.iter().all(|output| !matches!(
        output,
        Output::StageSnapshotChunk { .. } | Output::ApplySnapshot { .. }
    )));
    let response = install_snapshot_response_from_outputs(&outputs);
    assert!(!response.success);
    assert_eq!(response.last_included_index, LogIndex::ZERO);
    assert_eq!(response.next_offset, 0);
    assert_eq!(
        follower
            .snapshot_transfer_status()
            .rejected_chunks
            .metadata_mismatch,
        1
    );
}
#[test]
fn chunked_install_snapshot_applies_only_after_final_chunk() {
    let payload = large_snapshot_payload();
    let payload_len = payload.len() as u64;
    let (mut leader, source) = leader_with_snapshot_payload(payload.clone());
    let mut follower = node(2, &[1, 3]);
    leader
        .try_follower_progress_mut(NodeId(2))
        .expect("active follower")
        .next_index = LogIndex(3);

    let first_outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: false,
            match_index: LogIndex::ZERO,
        }),
    });
    let first_chunk = install_snapshot_chunk_from_output(&first_outputs[0], &source);
    assert_eq!(first_chunk.offset, 0);
    assert!(!first_chunk.done);
    assert!(first_chunk.chunk.len() < payload.len());

    let first_ack = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::InstallSnapshotChunk(first_chunk.clone()),
    });
    assert_eq!(follower.snapshot_index(), LogIndex::ZERO);
    assert!(matches!(
        first_ack.as_slice(),
        [
            Output::StageSnapshotChunk { chunk },
            Output::Send {
                message: Message::InstallSnapshotResponse(_),
                ..
            },
        ] if chunk.transfer_id == first_chunk.transfer_id
            && chunk.offset == 0
            && chunk.bytes == first_chunk.chunk
            && !chunk.done
    ));
    let response = install_snapshot_response_from_outputs(&first_ack);
    assert!(response.success);
    assert_eq!(response.last_included_index, LogIndex::ZERO);
    assert_eq!(response.next_offset, first_chunk.chunk.len() as u64);

    let second_outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::InstallSnapshotResponse(response),
    });
    let second_chunk = install_snapshot_chunk_from_output(&second_outputs[0], &source);
    assert_eq!(second_chunk.offset, first_chunk.chunk.len() as u64);
    assert!(second_chunk.done);

    let final_ack = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::InstallSnapshotChunk(second_chunk),
    });
    assert_eq!(follower.snapshot_index(), LogIndex(3));
    let installed = follower.snapshot().expect("snapshot installed");
    assert_eq!(installed.metadata.last_included_index, LogIndex(3));
    assert_eq!(installed.application_payload_len, payload_len);
    assert_eq!(installed.transfer_id(), first_chunk.transfer_id);
    assert!(matches!(
        final_ack.as_slice(),
        [
            Output::StageSnapshotChunk { chunk },
            Output::ApplySnapshot { .. },
            Output::Send {
                message: Message::InstallSnapshotResponse(response),
                ..
            }
        ] if chunk.done
            && chunk.offset == first_chunk.chunk.len() as u64
            && response.success
            && response.last_included_index == LogIndex(3)
            && response.next_offset == payload_len
    ));
    assert_eq!(
        [
            staged_snapshot_bytes(&first_ack),
            staged_snapshot_bytes(&final_ack)
        ]
        .concat(),
        payload
    );
}
#[test]
fn duplicate_snapshot_chunk_is_acknowledged_without_advancing_twice() {
    let payload = large_snapshot_payload();
    let (mut leader, source) = leader_with_snapshot_payload(payload.clone());
    let mut follower = node(2, &[1, 3]);
    leader
        .try_follower_progress_mut(NodeId(2))
        .expect("active follower")
        .next_index = LogIndex(3);
    let first_chunk = install_snapshot_chunk_from_output(
        &leader.step(Input::Message {
            from: NodeId(2),
            message: Message::AppendEntriesResponse(AppendEntriesResponse {
                sequence: 0,
                term: leader.current_term(),
                follower_id: NodeId(2),
                success: false,
                match_index: LogIndex::ZERO,
            }),
        })[0],
        &source,
    );

    let first_ack = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::InstallSnapshotChunk(first_chunk.clone()),
    });
    let duplicate_ack = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::InstallSnapshotChunk(first_chunk.clone()),
    });

    let first_response = install_snapshot_response_from_outputs(&first_ack);
    let duplicate_response = install_snapshot_response_from_outputs(&duplicate_ack);
    assert!(first_response.success);
    assert!(duplicate_response.success);
    assert_eq!(duplicate_response.next_offset, first_response.next_offset);
    assert!(
        !duplicate_ack
            .iter()
            .any(|output| matches!(output, Output::StageSnapshotChunk { .. })),
        "an already-staged prefix must be acknowledged without restaging"
    );
    assert_eq!(follower.snapshot_index(), LogIndex::ZERO);
}
#[test]
fn out_of_order_snapshot_chunk_requests_expected_offset() {
    let payload = large_snapshot_payload();
    let (mut leader, source) = leader_with_snapshot_payload(payload);
    let mut follower = node(2, &[1, 3]);
    let progress = leader
        .try_follower_progress_mut(NodeId(2))
        .expect("active follower");
    progress.next_index = LogIndex(3);
    progress.mode = ProgressMode::Snapshot {
        next_offset: 65_536,
    };
    let out_of_order = install_snapshot_chunk_from_output(
        &leader.step(Input::Message {
            from: NodeId(2),
            message: Message::AppendEntriesResponse(AppendEntriesResponse {
                sequence: 0,
                term: leader.current_term(),
                follower_id: NodeId(2),
                success: false,
                match_index: LogIndex::ZERO,
            }),
        })[0],
        &source,
    );

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::InstallSnapshotChunk(out_of_order),
    });
    let response = install_snapshot_response_from_outputs(&outputs);
    oracle_assert!(!response.success);
    oracle_assert_eq!(response.next_offset, 0);
    oracle_assert_eq!(follower.snapshot_index(), LogIndex::ZERO);
}
#[test]
fn mixed_snapshot_transfer_id_is_rejected_deterministically() {
    let payload = large_snapshot_payload();
    let (mut leader, source) = leader_with_snapshot_payload(payload);
    let mut follower = node(2, &[1, 3]);
    leader
        .try_follower_progress_mut(NodeId(2))
        .expect("active follower")
        .next_index = LogIndex(3);
    let mut chunk = install_snapshot_chunk_from_output(
        &leader.step(Input::Message {
            from: NodeId(2),
            message: Message::AppendEntriesResponse(AppendEntriesResponse {
                sequence: 0,
                term: leader.current_term(),
                follower_id: NodeId(2),
                success: false,
                match_index: LogIndex::ZERO,
            }),
        })[0],
        &source,
    );
    chunk.transfer_id = crate::SnapshotTransferId(chunk.transfer_id.0.wrapping_add(1));

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::InstallSnapshotChunk(chunk),
    });
    let response = install_snapshot_response_from_outputs(&outputs);
    assert!(!response.success);
    assert_eq!(response.next_offset, 0);
    assert_eq!(follower.snapshot_index(), LogIndex::ZERO);
}
#[test]
fn changed_snapshot_payload_checksum_is_rejected_mid_transfer() {
    let first_payload = large_snapshot_payload();
    let mut changed_payload = first_payload.clone();
    let last = changed_payload
        .last_mut()
        .expect("large snapshot payload is non-empty");
    *last ^= 0xff;
    assert_eq!(changed_payload.len(), first_payload.len());

    let (mut leader, source) = leader_with_snapshot_payload(first_payload);
    let mut follower = node(2, &[1, 3]);
    leader
        .try_follower_progress_mut(NodeId(2))
        .expect("active follower")
        .next_index = LogIndex(3);
    let first_chunk = install_snapshot_chunk_from_output(
        &leader.step(Input::Message {
            from: NodeId(2),
            message: Message::AppendEntriesResponse(AppendEntriesResponse {
                sequence: 0,
                term: leader.current_term(),
                follower_id: NodeId(2),
                success: false,
                match_index: LogIndex::ZERO,
            }),
        })[0],
        &source,
    );
    assert!(!first_chunk.done);

    let first_ack = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::InstallSnapshotChunk(first_chunk.clone()),
    });
    assert!(install_snapshot_response_from_outputs(&first_ack).success);

    let changed_snapshot =
        crate::RaftSnapshot::from_payload(first_chunk.metadata.clone(), &changed_payload);
    let offset = first_chunk.chunk.len();
    let changed_continuation = crate::InstallSnapshotChunk {
        term: first_chunk.term,
        leader_id: first_chunk.leader_id,
        transfer_id: changed_snapshot.transfer_id(),
        metadata: first_chunk.metadata.clone(),
        total_payload_len: changed_payload.len() as u64,
        application_payload_crc32: changed_snapshot.application_payload_crc32,
        offset: offset as u64,
        chunk: changed_payload[offset..].to_vec(),
        done: true,
    };

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::InstallSnapshotChunk(changed_continuation),
    });

    let response = install_snapshot_response_from_outputs(&outputs);
    assert!(!response.success);
    assert_eq!(response.next_offset, 0);
    assert_eq!(follower.snapshot_index(), LogIndex::ZERO);
    assert!(
        outputs
            .iter()
            .all(|output| !matches!(output, Output::StageSnapshotChunk { .. })),
        "a continuation with a different payload checksum must not be staged"
    );
    assert_eq!(
        follower
            .snapshot_transfer_status()
            .rejected_chunks
            .wrong_transfer,
        1
    );
}
