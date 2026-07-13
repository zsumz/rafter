//! Follower progress projection, matched-prefix heartbeats, and response identity.

use super::*;

#[test]
fn leader_replication_progress_projects_follower_match_and_next_indexes() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);
    let follower = node(2, &[1, 3]);

    assert!(follower.leader_replication_progress().is_empty());

    let _ = leader.step(Input::ClientProposal {
        payload: b"alert-opened".to_vec(),
    });
    let _ = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(2),
        }),
    });

    let progress = leader.leader_replication_progress();
    assert_eq!(progress.len(), 2);
    assert_eq!(progress[0].follower_id, NodeId(2));
    assert_eq!(progress[0].match_index, LogIndex(2));
    assert_eq!(progress[0].next_index, LogIndex(3));
    assert_eq!(progress[1].follower_id, NodeId(3));
    assert_eq!(progress[1].match_index, LogIndex::ZERO);
    assert_eq!(progress[1].next_index, LogIndex(1));
}
#[test]
fn heartbeat_with_divergent_follower_tail_reports_matched_prefix_only() {
    let mut leader = node(1, &[2, 3, 4, 5]);
    elect_five_node_leader(&mut leader);

    let _ = leader.step(Input::ClientProposal {
        payload: b"prefix".to_vec(),
    });
    acknowledge_append(&mut leader, NodeId(2), LogIndex(2));
    acknowledge_append(&mut leader, NodeId(3), LogIndex(2));
    assert_eq!(leader.commit_index(), LogIndex(2));

    let _ = leader.step(Input::ClientProposal {
        payload: b"leader-only".to_vec(),
    });
    assert_eq!(leader.last_log_index(), LogIndex(3));
    assert_eq!(leader.commit_index(), LogIndex(2));
    acknowledge_append(&mut leader, NodeId(3), LogIndex(3));
    assert_eq!(leader.commit_index(), LogIndex(2));

    let mut follower = node(2, &[1, 3]);
    follower
        .persistent
        .log
        .push(LogEntry::noop(leader.current_term()));
    push_log_entry(&mut follower, leader.current_term(), b"prefix");
    push_log_entry(&mut follower, Term(99), b"divergent-tail");

    let heartbeat_outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: leader.current_term(),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(2),
            prev_log_term: leader.current_term(),
            entries: Vec::new().into(),
            leader_commit: LogIndex(2),
        }),
    });

    let response = append_entries_response(&heartbeat_outputs);
    assert!(response.success);
    assert_eq!(
        response.match_index,
        LogIndex(2),
        "empty heartbeat confirms only prev_log_index, not the follower tail"
    );

    let commit_outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(response),
    });

    assert_eq!(
        leader.commit_index(),
        LogIndex(2),
        "leader must not commit an entry the follower did not acknowledge from this leader"
    );
    assert!(commit_outputs.is_empty());
}
#[test]
fn leader_rejects_append_response_when_sender_disagrees_with_follower_id() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);

    let _ = leader.step(Input::ClientProposal {
        payload: b"leader-only".to_vec(),
    });

    let outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id: NodeId(3),
            success: true,
            match_index: LogIndex(1),
        }),
    });

    assert!(outputs.is_empty());
    assert_eq!(leader.commit_index(), LogIndex::ZERO);
}
