//! Snapshot-mode pause, installation recovery, and mixed-peer fan-out.

use super::support::*;
use super::*;

#[test]
fn snapshot_mode_pauses_pipelining_and_resumes_with_a_window_fill_after_installation() {
    let snapshot_payload = b"pipelining snapshot";
    let snapshot = test_snapshot(3, 4, 5, snapshot_payload);
    let mut leader = Node::from_bootstrap(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
            .expect("test Raft node config is valid")
            .with_max_append_entries_bytes(ONE_ENTRY_BATCH_BUDGET),
        BootstrapState {
            current_term: Term(5),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: Some(snapshot),
            log: vec![
                bootstrap_entry(4, 5, &payload(4)),
                bootstrap_entry(5, 5, &payload(5)),
                bootstrap_entry(6, 5, &payload(6)),
            ],
        },
    )
    .expect("leader hydrates from snapshot");
    leader.become_leader();
    // Follower 2's send position lies behind the compacted prefix.
    leader
        .try_follower_progress_mut(NodeId(2))
        .expect("active follower")
        .next_index = LogIndex(2);

    // The broadcast notices the follower needs the snapshot, not the log:
    // chunks flow and append pipelining pauses entirely.
    for _ in 0..2 {
        let outputs = leader.step(Input::Tick);
        assert!(
            appends_to(&outputs, NodeId(2)).is_empty(),
            "no AppendEntries while the snapshot streams"
        );
        let chunks = snapshot_chunks_to(&outputs, NodeId(2));
        assert_eq!(chunks.len(), 1);
        assert_eq!(
            chunks[0].offset, 0,
            "the cursor chunk is re-sent until acknowledged"
        );
        assert_eq!(
            replication_state(&leader, NodeId(2)),
            ReplicationState::Snapshotting { next_offset: 0 }
        );
    }
    assert_eq!(
        follower_progress(&leader, NodeId(2))
            .inflights
            .batch_count(),
        0
    );

    // The installation acknowledgement confirms the boundary: the mode
    // returns to Replicate and the suffix fills the window in the same step.
    let transfer_id = leader.snapshot().expect("snapshot is held").transfer_id();
    let outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::InstallSnapshotResponse(InstallSnapshotResponse {
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            last_included_index: LogIndex(3),
            transfer_id: Some(transfer_id),
            next_offset: snapshot_payload.len() as u64,
        }),
    });

    let filled = appends_to(&outputs, NodeId(2));
    assert_eq!(
        filled.len(),
        3,
        "the installed boundary confirms the position: the suffix window-fills at once"
    );
    assert_eq!(filled[0].prev_log_index, LogIndex(3));
    assert_eq!(filled[1].prev_log_index, LogIndex(4));
    assert_eq!(filled[2].prev_log_index, LogIndex(5));
    assert_eq!(
        replication_state(&leader, NodeId(2)),
        ReplicationState::Replicating
    );
    let progress = follower_progress(&leader, NodeId(2));
    assert_eq!(progress.match_index, LogIndex(3));
    assert_eq!(progress.next_index, LogIndex(8));
    assert_eq!(progress.inflights.batch_count(), 3);
}
#[test]
fn snapshot_peer_does_not_break_shared_append_fanout_to_log_peers() {
    let snapshot = test_snapshot(3, 4, 5, b"mixed-mode snapshot");
    let mut leader = Node::from_bootstrap(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3), NodeId(4)], 3)
            .expect("test Raft node config is valid"),
        BootstrapState {
            current_term: Term(5),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: Some(snapshot),
            log: vec![bootstrap_entry(4, 5, &payload(4))],
        },
    )
    .expect("leader hydrates from snapshot");
    leader.become_leader();
    seed_replicating(&mut leader, NodeId(2), LogIndex(3));
    seed_replicating(&mut leader, NodeId(3), LogIndex(3));
    leader
        .try_follower_progress_mut(NodeId(4))
        .expect("active follower")
        .next_index = LogIndex(2);

    let outputs = leader.step(Input::Tick);

    assert!(
        appends_to(&outputs, NodeId(4)).is_empty(),
        "a compacted follower receives snapshot chunks, not cached log batches"
    );
    assert_eq!(snapshot_chunks_to(&outputs, NodeId(4)).len(), 1);
    let follower_two_entries = appends_to(&outputs, NodeId(2))
        .first()
        .expect("follower 2 receives the retained suffix")
        .entries
        .clone();
    let follower_three_entries = appends_to(&outputs, NodeId(3))
        .first()
        .expect("follower 3 receives the retained suffix")
        .entries
        .clone();
    assert!(!follower_two_entries.is_empty());
    assert!(
        follower_two_entries.shares_allocation(&follower_three_entries),
        "snapshot-mode peers must not prevent log peers from sharing one suffix batch"
    );
}
