//! Leader and follower snapshot-transfer observability and rejection counters.

use super::*;

#[test]
fn snapshot_transfer_status_reports_follower_progress_and_rejections() {
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

    let _ = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::InstallSnapshotChunk(first_chunk.clone()),
    });

    let status = follower.snapshot_transfer_status();
    assert!(status.leader.is_empty());
    let follower_status = status.follower.expect("follower transfer is active");
    assert_eq!(follower_status.leader_id, NodeId(1));
    assert_eq!(follower_status.transfer_id, first_chunk.transfer_id);
    assert_eq!(follower_status.last_included_index, LogIndex(3));
    assert_eq!(follower_status.total_bytes, payload.len() as u64);
    assert_eq!(
        follower_status.received_bytes,
        first_chunk.chunk.len() as u64
    );
    assert!(status.rejected_chunks.is_empty());

    let mut out_of_order = first_chunk.clone();
    out_of_order.offset = first_chunk.chunk.len() as u64 + 1;
    out_of_order.chunk = vec![b'x'];
    out_of_order.done = false;
    let _ = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::InstallSnapshotChunk(out_of_order),
    });

    let mut stale = first_chunk.clone();
    stale.term = Term(4);
    let _ = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::InstallSnapshotChunk(stale),
    });

    let rejected = follower.snapshot_transfer_status().rejected_chunks;
    assert_eq!(rejected.out_of_order_offset, 1);
    assert_eq!(rejected.stale_term, 1);
}
#[test]
fn snapshot_transfer_status_reports_leader_progress() {
    let payload = large_snapshot_payload();
    let (mut leader, source) = leader_with_snapshot_payload(payload.clone());
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
    let first = install_snapshot_chunk_from_output(&first_outputs[0], &source);

    let _ = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::InstallSnapshotResponse(crate::InstallSnapshotResponse {
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            last_included_index: LogIndex::ZERO,
            transfer_id: Some(first.transfer_id),
            next_offset: first.chunk.len() as u64,
        }),
    });

    let status = leader.snapshot_transfer_status();
    assert!(status.follower.is_none());
    assert_eq!(status.leader.len(), 1);
    assert_eq!(status.leader[0].follower_id, NodeId(2));
    assert_eq!(status.leader[0].transfer_id, first.transfer_id);
    assert_eq!(status.leader[0].last_included_index, LogIndex(3));
    assert_eq!(status.leader[0].total_bytes, payload.len() as u64);
    assert_eq!(status.leader[0].next_offset, first.chunk.len() as u64);
}
