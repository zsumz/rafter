//! Admission preserves boundaries, identity, sequence and finite queue budgets.
use super::*;
use rafter::{AppendEntries, AppendEntriesResponse, LogEntry};

fn gate(role: Role) -> PeerBatchGate {
    PeerBatchGate {
        role,
        term: Term(2),
        leader: Some(NodeId(1)),
        tail: LogIndex(3),
        tail_term: Term(2),
        remaining_events: 32,
        remaining_bytes: 256 * 1024,
        closed: false,
    }
}
fn response(term: u64, index: u64, sequence: u64) -> Message {
    Message::AppendEntriesResponse(AppendEntriesResponse {
        term: Term(term),
        follower_id: NodeId(2),
        success: true,
        match_index: LogIndex(index),
        sequence,
    })
}
fn append(prev: u64, entries: Vec<LogEntry>) -> Message {
    Message::AppendEntries(AppendEntries {
        term: Term(2),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(prev),
        prev_log_term: Term(2),
        sequence: 7,
        entries: entries.into(),
        leader_commit: LogIndex(prev),
    })
}

#[test]
fn leader_keeps_duplicate_out_of_order_and_read_sequences() {
    let mut gate = gate(Role::Leader);
    for message in [response(2, 3, 5), response(2, 2, 1), response(2, 3, 9)] {
        assert!(gate.admit(NodeId(2), &message));
    }
    assert!(!gate.admit(NodeId(2), &response(3, 3, 10)));
    assert!(!gate.admit(NodeId(2), &response(2, 4, 11)));
}

#[test]
fn follower_accepts_only_contiguous_application_tail_extensions() {
    let mut gate = gate(Role::Follower);
    assert!(gate.admit(
        NodeId(1),
        &append(3, vec![LogEntry::application(Term(2), vec![1])])
    ));
    assert!(gate.admit(
        NodeId(1),
        &append(4, vec![LogEntry::application(Term(2), vec![2])])
    ));
    assert!(!gate.admit(
        NodeId(1),
        &append(3, vec![LogEntry::application(Term(2), vec![3])])
    ));
    assert!(!gate.admit(
        NodeId(1),
        &append(5, vec![LogEntry::application(Term(2), vec![4])])
    ));
}

#[test]
fn empty_noop_wrong_identity_and_configuration_end_a_batch() {
    let config = rafter::ConfigurationEntry::Stable {
        config_id: rafter::ConfigurationId(1),
        membership: rafter::MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], vec![])
            .unwrap(),
    };
    for entries in [
        vec![],
        vec![LogEntry::noop(Term(2))],
        vec![LogEntry::configuration(Term(2), config)],
    ] {
        assert!(!gate(Role::Follower).admit(NodeId(1), &append(3, entries)));
    }
    assert!(!gate(Role::Leader).admit(NodeId(3), &response(2, 3, 1)));
    assert!(!gate(Role::Follower).admit(
        NodeId(3),
        &append(3, vec![LogEntry::application(Term(2), vec![1])])
    ));
}

#[test]
fn event_and_decoded_byte_caps_close_the_gate() {
    let mut events = gate(Role::Leader);
    events.remaining_events = 1;
    assert!(events.admit(NodeId(2), &response(2, 3, 1)));
    assert!(!events.admit(NodeId(2), &response(2, 4, 2)));
    let mut bytes = gate(Role::Follower);
    bytes.remaining_bytes = 128;
    assert!(!bytes.admit(
        NodeId(1),
        &append(3, vec![LogEntry::application(Term(2), vec![0; 129])])
    ));
}
