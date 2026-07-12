//! Read authority, current-term commitment, and cancellation boundaries.

use super::support::*;

#[test]
fn read_rejected_without_current_term_commit() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);

    let outputs = leader.step(read_index(7));

    assert!(matches!(
        outputs.as_slice(),
        [Output::ReadIndexRejected {
            read_id: ReadId(7),
            reason: ReadIndexRejection::NoCommitInCurrentTerm,
        }]
    ));
}

#[test]
fn read_rejected_on_follower() {
    let mut follower = node(2, &[1, 3]);
    let outputs = follower.step(read_index(3));
    assert!(matches!(
        outputs.as_slice(),
        [Output::ReadIndexRejected {
            read_id: ReadId(3),
            reason: ReadIndexRejection::NotLeader { .. },
        }]
    ));
}

#[test]
fn leader_noop_unlocks_read_index_without_client_write() {
    let mut leader = node(1, &[2, 3]);
    let _ = elect_leader(&mut leader);
    assert_eq!(
        leader.log_entries_from(LogIndex(1)),
        vec![LogEntry::noop(leader.current_term())]
    );

    let outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(1),
            sequence: 0,
        }),
    });
    assert_eq!(leader.commit_index(), LogIndex(1));
    assert!(outputs
        .iter()
        .all(|output| !matches!(output, Output::Apply { .. })));

    let heartbeats = leader.step(read_index(77));
    let round = heartbeat_round(&heartbeats);
    let outputs = ack(&mut leader, 2, round);

    assert_eq!(granted(&outputs), vec![(ReadId(77), LogIndex(1))]);
}

#[test]
fn isolated_ex_leader_never_grants_a_read() {
    // The scenario needs the isolated leader to keep believing in itself for
    // the whole run, which is exactly what check-quorum forecloses.
    let mut leader = commit_first_entry(Node::new(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
            .expect("valid config")
            .with_check_quorum(false),
    ));

    // Partitioned: the read is registered, heartbeats go nowhere, no
    // acknowledgement ever arrives.
    let outputs = leader.step(read_index(5));
    assert!(granted(&outputs).is_empty());
    for _ in 0..20 {
        let outputs = leader.step(Input::Tick);
        assert!(
            granted(&outputs).is_empty(),
            "an unconfirmed leader must never grant a read barrier"
        );
    }
    assert_eq!(leader.pending_read_count(), 1);
}

#[test]
fn reads_rejected_during_leadership_transfer() {
    let mut leader = leader_with_current_term_commit();
    let _ = leader.step(Input::TransferLeadership { target: NodeId(2) });

    let outputs = leader.step(read_index(4));
    assert!(matches!(
        outputs.as_slice(),
        [Output::ReadIndexRejected {
            read_id: ReadId(4),
            reason: ReadIndexRejection::LeadershipTransferInProgress { target: NodeId(2) },
        }]
    ));
}

#[test]
fn pending_reads_are_cleared_on_step_down() {
    let mut leader = leader_with_current_term_commit();
    let _ = leader.step(read_index(6));
    let _ = leader.step(read_index(7));
    assert_eq!(leader.pending_read_count(), 2);

    let outputs = leader.step(Input::Message {
        from: NodeId(3),
        message: Message::AppendEntries(AppendEntries {
            term: leader.current_term().next(),
            leader_id: NodeId(3),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            entries: Vec::new().into(),
            leader_commit: LogIndex::ZERO,
            sequence: 1,
        }),
    });

    assert_eq!(leader.role(), Role::Follower);
    assert_eq!(leader.pending_read_count(), 0);
    assert_eq!(
        canceled(&outputs),
        vec![
            (ReadId(6), ReadIndexCancelReason::LeadershipLost),
            (ReadId(7), ReadIndexCancelReason::LeadershipLost),
        ]
    );
}
