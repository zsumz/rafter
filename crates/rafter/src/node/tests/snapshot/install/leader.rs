//! Leader snapshot fallback and acknowledgement-driven return to log replication.

use super::*;

#[test]
fn leader_sends_install_snapshot_when_follower_is_behind_compacted_prefix() {
    let (mut leader, source) = leader_with_snapshot_payload(b"snapshot bytes".to_vec());
    leader
        .try_follower_progress_mut(NodeId(2))
        .expect("active follower")
        .next_index = LogIndex(4);

    let outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: false,
            match_index: LogIndex::ZERO,
        }),
    });

    assert_eq!(outputs.len(), 1);
    let Output::SendSnapshotChunk { to, chunk } = &outputs[0] else {
        panic!("expected send snapshot chunk directive");
    };
    assert_eq!(*to, NodeId(2));
    assert_eq!(chunk.leader_id, NodeId(1));
    assert_eq!(chunk.metadata.last_included_index, LogIndex(3));
    assert_eq!(chunk.offset, 0);
    assert_eq!(chunk.total_payload_len, b"snapshot bytes".len() as u64);
    assert!(chunk.done);
    let message = chunk.resolve(&source).expect("source serves the snapshot");
    assert_eq!(message.chunk, b"snapshot bytes".to_vec());
}
#[test]
fn leader_sends_log_suffix_after_successful_install_snapshot_response() {
    let mut leader = leader_with_snapshot_and_suffix();
    leader
        .try_follower_progress_mut(NodeId(2))
        .expect("active follower")
        .next_index = LogIndex(3);

    let outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::InstallSnapshotResponse(crate::InstallSnapshotResponse {
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            last_included_index: LogIndex(3),
            transfer_id: None,
            next_offset: b"snapshot bytes".len() as u64,
        }),
    });

    assert_eq!(outputs.len(), 1);
    let Output::Send { to, message } = &outputs[0] else {
        panic!("expected append entries send");
    };
    assert_eq!(*to, NodeId(2));
    let Message::AppendEntries(request) = message else {
        panic!("expected append entries after snapshot install");
    };
    assert_eq!(request.prev_log_index, LogIndex(3));
    assert_eq!(request.prev_log_term, Term(4));
    assert_eq!(
        request.entries,
        vec![
            LogEntry::application(Term(5), b"suffix-four".to_vec()),
            LogEntry::noop(Term(5)),
        ]
        .into()
    );
}
#[test]
fn stale_install_snapshot_response_does_not_regress_replication_state() {
    let mut leader = leader_with_snapshot_and_suffix();
    leader.volatile.commit_index = LogIndex(5);
    leader.volatile.applied_index = LogIndex(5);
    let progress = leader
        .try_follower_progress_mut(NodeId(2))
        .expect("active follower");
    progress.match_index = LogIndex(5);
    progress.next_index = LogIndex(6);
    progress.mode = ProgressMode::Replicate;

    let outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::InstallSnapshotResponse(crate::InstallSnapshotResponse {
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            last_included_index: LogIndex(3),
            transfer_id: None,
            next_offset: b"snapshot bytes".len() as u64,
        }),
    });

    let progress = leader
        .leader
        .progress
        .get(NodeId(2))
        .expect("active follower");
    assert_eq!(progress.match_index, LogIndex(5));
    assert_eq!(progress.next_index, LogIndex(6));
    assert!(outputs.is_empty());
}
#[test]
fn overstated_install_snapshot_response_is_clamped_to_leader_tail() {
    let mut leader = leader_with_snapshot_and_suffix();
    leader.volatile.commit_index = LogIndex(5);
    leader.volatile.applied_index = LogIndex(5);
    leader
        .try_follower_progress_mut(NodeId(2))
        .expect("active follower")
        .next_index = LogIndex(3);

    let outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::InstallSnapshotResponse(crate::InstallSnapshotResponse {
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            last_included_index: LogIndex(99),
            transfer_id: None,
            next_offset: b"snapshot bytes".len() as u64,
        }),
    });

    let progress = leader
        .leader
        .progress
        .get(NodeId(2))
        .expect("active follower");
    assert_eq!(progress.match_index, LogIndex(5));
    assert_eq!(progress.next_index, LogIndex(6));
    assert!(outputs.is_empty());
}
#[test]
fn delayed_duplicate_response_for_older_transfer_is_ignored() {
    let mut leader = leader_with_snapshot_and_suffix();
    leader.volatile.commit_index = LogIndex(4);
    leader.volatile.applied_index = LogIndex(4);
    let progress = leader
        .try_follower_progress_mut(NodeId(2))
        .expect("active follower");
    progress.match_index = LogIndex(4);
    progress.next_index = LogIndex(5);
    progress.mode = ProgressMode::Replicate;

    let outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::InstallSnapshotResponse(crate::InstallSnapshotResponse {
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            last_included_index: LogIndex(2),
            transfer_id: Some(SnapshotTransferId(0xdead_beef)),
            next_offset: 1,
        }),
    });

    assert!(
        outputs.is_empty(),
        "a delayed duplicate naming an obsolete transfer must not restream"
    );
    let progress = leader
        .leader
        .progress
        .get(NodeId(2))
        .expect("active follower");
    assert_eq!(
        progress.next_index,
        LogIndex(5),
        "replication progress must not regress on stale transfer responses"
    );
    assert_eq!(
        progress.mode,
        ProgressMode::Replicate,
        "a stale transfer response must not push the follower back into snapshot streaming"
    );
}
#[test]
fn duplicate_ack_within_current_transfer_does_not_regress_offset() {
    let mut leader = leader_with_snapshot_and_suffix();
    let progress = leader
        .try_follower_progress_mut(NodeId(2))
        .expect("active follower");
    progress.next_index = LogIndex(1);
    progress.mode = ProgressMode::Snapshot { next_offset: 10 };
    let current_transfer = leader.snapshot_transfer_status().leader[0].transfer_id;

    let _ = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::InstallSnapshotResponse(crate::InstallSnapshotResponse {
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            last_included_index: LogIndex::ZERO,
            transfer_id: Some(current_transfer),
            next_offset: 4,
        }),
    });

    assert_eq!(
        leader
            .leader
            .progress
            .get(NodeId(2))
            .expect("active follower")
            .mode,
        ProgressMode::Snapshot { next_offset: 10 },
        "an out-of-order ack for the current transfer must not rewind the send offset"
    );
}
