//! Rejected malformed durable images and index geometry.

use super::support::*;

#[test]
fn bootstrap_rejects_vote_in_zero_term() {
    assert_eq!(
        Node::from_bootstrap(
            config(),
            BootstrapState {
                current_term: Term::default(),
                voted_for: Some(NodeId(2)),
                commit_index: LogIndex::ZERO,
                committed_configuration: None,
                snapshot: None,
                log: Vec::new(),
            },
        ),
        Err(BootstrapValidationError::VoteInZeroTerm {
            voted_for: NodeId(2),
        })
    );
}
#[test]
fn bootstrap_rejects_non_contiguous_log_entries() {
    assert_eq!(
        Node::from_bootstrap(
            config(),
            BootstrapState {
                current_term: Term(7),
                voted_for: None,
                commit_index: LogIndex::ZERO,
                committed_configuration: None,
                snapshot: None,
                log: vec![bootstrap_entry(2, 7, b"append")],
            },
        ),
        Err(BootstrapValidationError::NonContiguousLog {
            expected: LogIndex(1),
            actual: LogIndex(2),
        })
    );
}
#[test]
fn bootstrap_rejects_zero_term_log_entry() {
    assert_eq!(
        Node::from_bootstrap(
            config(),
            BootstrapState {
                current_term: Term(7),
                voted_for: None,
                commit_index: LogIndex::ZERO,
                committed_configuration: None,
                snapshot: None,
                log: vec![bootstrap_entry(1, 0, b"append")],
            },
        ),
        Err(BootstrapValidationError::ZeroTermLogEntry { index: LogIndex(1) })
    );
}
#[test]
fn bootstrap_rejects_entry_term_ahead_of_current_term() {
    assert_eq!(
        Node::from_bootstrap(
            config(),
            BootstrapState {
                current_term: Term(6),
                voted_for: None,
                commit_index: LogIndex::ZERO,
                committed_configuration: None,
                snapshot: None,
                log: vec![bootstrap_entry(1, 7, b"append")],
            },
        ),
        Err(BootstrapValidationError::EntryTermAheadOfCurrentTerm {
            index: LogIndex(1),
            entry_term: Term(7),
            current_term: Term(6),
        })
    );
}
#[test]
fn bootstrap_rejects_index_arithmetic_at_the_representable_maximum() {
    assert_eq!(
        RaftSnapshotMetadata::new(
            SnapshotGroupId::new("group").expect("valid group id"),
            NodeId(1),
            LogIndex(u64::MAX),
            Term(1),
            Term(1),
            ApplicationSnapshotMetadata::new(
                ApplicationSnapshotKind::new("kind").expect("valid kind"),
                ApplicationSnapshotVersion::new(1).expect("valid version"),
            ),
        )
        .expect_err("a boundary with no successor is invalid"),
        SnapshotMetadataError::LastIncludedIndexAtMaximum,
    );

    let config = NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
        .expect("test Raft node config is valid");
    let bootstrap = BootstrapState {
        current_term: Term(1),
        voted_for: None,
        commit_index: LogIndex::ZERO,
        committed_configuration: None,
        snapshot: None,
        log: vec![
            bootstrap_entry(u64::MAX - 1, 1, b"tail"),
            bootstrap_entry(u64::MAX, 1, b"end"),
        ],
    };
    assert!(matches!(
        Node::from_bootstrap(config, bootstrap),
        Err(BootstrapValidationError::NonContiguousLog { .. }
            | BootstrapValidationError::LogIndexAtMaximum { .. })
    ));
}
