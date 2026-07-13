//! Hydration of durable term, vote, log, and follower state.

use super::support::*;
use super::*;

#[test]
fn node_hydrates_from_persistent_state_as_follower() {
    let node = Node::from_bootstrap(
        config(),
        BootstrapState {
            current_term: Term(7),
            voted_for: Some(NodeId(2)),
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: None,
            log: vec![
                bootstrap_entry(1, 6, b"create"),
                bootstrap_entry(2, 7, b"append"),
            ],
        },
    )
    .expect("bootstrap state is valid");

    assert_eq!(node.role(), Role::Follower);
    assert_eq!(node.current_term(), Term(7));
    assert_eq!(node.voted_for(), Some(NodeId(2)));
    assert_eq!(node.last_log_index(), LogIndex(2));
    assert_eq!(node.commit_index(), LogIndex::ZERO);
    assert_eq!(
        node.entry_at(LogIndex(2)).map(|entry| (
            entry.term,
            entry.application_payload().expect("application entry")
        )),
        Some((Term(7), &b"append"[..]))
    );
}
#[test]
fn log_entries_from_returns_opaque_suffixes() {
    let node = Node::from_bootstrap(
        config(),
        BootstrapState {
            current_term: Term(7),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: None,
            log: vec![
                bootstrap_entry(1, 6, b"create"),
                bootstrap_entry(2, 7, b"append"),
            ],
        },
    )
    .expect("bootstrap state is valid");

    assert_eq!(node.log_entries_from(LogIndex::ZERO), Vec::new());
    assert!(node.log_entries_slice_from(LogIndex::ZERO).is_empty());
    assert_eq!(
        node.log_entries_from(LogIndex(2)),
        vec![LogEntry::application(Term(7), b"append".to_vec())]
    );
    assert_eq!(
        node.log_entries_slice_from(LogIndex(2)),
        &[LogEntry::application(Term(7), b"append".to_vec())]
    );
    assert_eq!(node.log_entries_from(LogIndex(3)), Vec::new());
    assert!(node.log_entries_slice_from(LogIndex(3)).is_empty());
}
#[test]
fn hydrated_node_rejects_second_vote_in_persisted_term() {
    let mut node = Node::from_bootstrap(
        config(),
        BootstrapState {
            current_term: Term(7),
            voted_for: Some(NodeId(2)),
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: None,
            log: vec![bootstrap_entry(1, 7, b"append")],
        },
    )
    .expect("bootstrap state is valid");

    let outputs = node.step(Input::Message {
        from: NodeId(3),
        message: Message::RequestVote(RequestVote {
            term: Term(7),
            candidate_id: NodeId(3),
            last_log_index: LogIndex(1),
            last_log_term: Term(7),
        }),
    });

    assert_vote_response(&outputs, NodeId(3), false);
}
#[test]
fn bootstrap_preserves_vote_for_candidate_outside_static_voters() {
    let node = Node::from_bootstrap(
        config(),
        BootstrapState {
            current_term: Term(7),
            voted_for: Some(NodeId(9)),
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: None,
            log: Vec::new(),
        },
    )
    .expect("persisted vote is a valid one-vote-per-term record");

    assert_eq!(node.voted_for(), Some(NodeId(9)));
}
