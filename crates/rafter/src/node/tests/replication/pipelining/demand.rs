//! Proposal traffic must not manufacture empty contact rounds or delay recovery.
use super::support::*;
use super::*;

fn full_windows() -> Node {
    let mut leader = pipelining_leader(4, |config| {
        config
            .with_max_inflight_appends(1)
            .with_heartbeat_interval_ticks(2)
    });
    for follower in [NodeId(2), NodeId(3)] {
        seed_replicating(&mut leader, follower, LogIndex::ZERO);
    }
    let _ = leader.broadcast_append_entries();
    leader.leader.heartbeat_elapsed = 1;
    leader
}

#[test]
fn busy_proposals_do_not_send_empty_appends_or_postpone_a_full_window_heartbeat() {
    let mut leader = full_windows();
    let sequence = leader.leader.heartbeat_sequence;
    for _ in 0..20 {
        let outputs = leader.step(Input::ClientProposal {
            payload: payload(10),
        });
        assert!(appends_to(&outputs, NodeId(2)).is_empty());
        assert!(appends_to(&outputs, NodeId(3)).is_empty());
        assert_eq!(leader.leader.heartbeat_elapsed, 1);
        assert_eq!(leader.leader.heartbeat_sequence, sequence);
    }
    let outputs = leader.step(Input::Tick);
    for follower in [NodeId(2), NodeId(3)] {
        let sent = appends_to(&outputs, follower);
        assert_eq!(sent.len(), 1);
        assert!(sent[0].entries.is_empty());
        assert_eq!(sent[0].sequence, sequence + 1);
    }
    assert_eq!(leader.leader.heartbeat_elapsed, 0);
}

#[test]
fn useful_data_to_one_follower_does_not_postpone_contact_with_the_other() {
    let mut leader = full_windows();
    let last = leader.last_log_index();
    seed_replicating(&mut leader, NodeId(3), last);
    let outputs = leader.step(Input::ClientProposal {
        payload: payload(10),
    });
    assert!(appends_to(&outputs, NodeId(2)).is_empty());
    let sent = appends_to(&outputs, NodeId(3));
    assert_eq!(sent.len(), 1);
    assert!(!sent[0].entries.is_empty());
    assert_eq!(leader.leader.heartbeat_elapsed, 1);
    let outputs = leader.step(Input::Tick);
    assert_eq!(appends_to(&outputs, NodeId(2)).len(), 1);
}

#[test]
fn awaiting_probe_is_silent_for_proposals_but_contact_and_rejection_still_recover() {
    let mut leader = full_windows();
    *leader.reconcile_follower_progress_mut(NodeId(2)).unwrap() = Progress::probing(LogIndex(1));
    let first = leader.step(Input::ClientProposal {
        payload: payload(10),
    });
    assert_eq!(appends_to(&first, NodeId(2)).len(), 1);
    let next = leader.step(Input::ClientProposal {
        payload: payload(11),
    });
    assert!(appends_to(&next, NodeId(2)).is_empty());
    let tick = leader.step(Input::Tick);
    assert!(appends_to(&tick, NodeId(2))[0].entries.is_empty());
    let rejected = deliver_append_response(&mut leader, NodeId(2), false, LogIndex::ZERO);
    assert!(!appends_to(&rejected, NodeId(2)).is_empty());
}

#[test]
fn read_confirmation_still_contacts_full_windows_and_requires_its_own_sequence() {
    let mut leader = full_windows();
    let tail = leader.last_log_index();
    let _ = deliver_append_response(&mut leader, NodeId(2), true, tail);
    for follower in [NodeId(2), NodeId(3)] {
        seed_replicating(&mut leader, follower, tail);
    }
    let _ = leader.step(Input::ClientProposal {
        payload: payload(10),
    });
    let old_sequence = leader.leader.heartbeat_sequence;
    let outputs = leader.step(Input::ReadIndex {
        read_id: crate::ReadId(7),
    });
    let sent = appends_to(&outputs, NodeId(2));
    assert_eq!(sent.len(), 1);
    assert!(sent[0].entries.is_empty());
    let sequence = sent[0].sequence;
    assert!(sequence > old_sequence);
    let response = |sequence| Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence,
            term: Term(2),
            follower_id: NodeId(2),
            success: true,
            match_index: tail,
        }),
    };
    let stale = leader.step(response(old_sequence));
    assert!(!stale
        .iter()
        .any(|output| matches!(output, Output::ReadIndexGranted { .. })));
    let confirmed = leader.step(response(sequence));
    assert!(confirmed.iter().any(|output| matches!(
        output,
        Output::ReadIndexGranted {
            read_id: crate::ReadId(7),
            ..
        }
    )));
}
