//! `AppendEntries` matching immediately around the snapshot boundary.

use super::super::support::*;
use super::*;

#[test]
fn append_entries_matches_immediately_after_snapshot_boundary() {
    let mut node = Node::from_bootstrap(
        config(),
        BootstrapState {
            current_term: Term(8),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: Some(snapshot_descriptor(5, 6, 8)),
            log: Vec::new(),
        },
    )
    .expect("snapshot bootstrap state is valid");

    let outputs = node.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(8),
            leader_id: NodeId(2),
            prev_log_index: LogIndex(5),
            prev_log_term: Term(6),
            entries: vec![LogEntry::application(
                Term(8),
                b"first-suffix-entry".to_vec(),
            )]
            .into(),
            leader_commit: LogIndex(6),
        }),
    });

    assert_eq!(node.last_log_index(), LogIndex(6));
    assert_eq!(node.commit_index(), LogIndex(6));
    assert_eq!(
        node.entry_at(LogIndex(6)).map(|entry| (
            entry.term,
            entry.application_payload().expect("application entry")
        )),
        Some((Term(8), &b"first-suffix-entry"[..]))
    );
    assert_eq!(outputs.len(), 2);
    assert_eq!(
        outputs[0],
        Output::Apply {
            index: LogIndex(6),
            term: Term(8),
            payload: b"first-suffix-entry".to_vec().into(),
            local_proposal_id: None,
        }
    );
    assert_append_entries_response(&outputs[1..], NodeId(2), true, LogIndex(6));
}
#[test]
fn append_entries_rejects_previous_log_before_snapshot_boundary() {
    let mut node = Node::from_bootstrap(
        config(),
        BootstrapState {
            current_term: Term(8),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: Some(snapshot_descriptor(5, 6, 8)),
            log: Vec::new(),
        },
    )
    .expect("snapshot bootstrap state is valid");

    let outputs = node.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(8),
            leader_id: NodeId(2),
            prev_log_index: LogIndex(4),
            prev_log_term: Term(6),
            entries: Vec::new().into(),
            leader_commit: LogIndex(4),
        }),
    });

    assert_eq!(node.last_log_index(), LogIndex(5));
    assert_eq!(node.commit_index(), LogIndex(5));
    assert_append_entries_response(&outputs, NodeId(2), false, LogIndex::ZERO);
}
