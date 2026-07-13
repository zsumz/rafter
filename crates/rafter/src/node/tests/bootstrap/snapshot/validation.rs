//! Snapshot-boundary sentinel and compacted-log validation.

use super::super::support::*;
use super::*;

#[test]
fn bootstrap_accepts_matching_snapshot_boundary_entry_as_validation_sentinel() {
    let node = Node::from_bootstrap(
        config(),
        BootstrapState {
            current_term: Term(8),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: Some(snapshot_descriptor(5, 6, 8)),
            log: vec![
                bootstrap_entry(5, 6, b"already-compacted"),
                bootstrap_entry(6, 8, b"visible"),
            ],
        },
    )
    .expect("matching boundary sentinel is valid");

    assert_eq!(node.commit_index(), LogIndex(5));
    assert_eq!(node.last_log_index(), LogIndex(6));
    assert_eq!(
        node.log_entries_from(LogIndex(1)),
        vec![LogEntry::application(Term(8), b"visible".to_vec())]
    );
}
#[test]
fn bootstrap_rejects_log_entries_before_snapshot_boundary() {
    assert_eq!(
        Node::from_bootstrap(
            config(),
            BootstrapState {
                current_term: Term(8),
                voted_for: None,
                commit_index: LogIndex::ZERO,
                committed_configuration: None,
                snapshot: Some(snapshot_descriptor(5, 6, 8)),
                log: vec![bootstrap_entry(4, 6, b"compacted")],
            },
        ),
        Err(BootstrapValidationError::CompactedLogEntry {
            snapshot_index: LogIndex(5),
            entry_index: LogIndex(4),
        })
    );
}
#[test]
fn bootstrap_rejects_mismatched_snapshot_boundary_sentinel() {
    assert_eq!(
        Node::from_bootstrap(
            config(),
            BootstrapState {
                current_term: Term(8),
                voted_for: None,
                commit_index: LogIndex::ZERO,
                committed_configuration: None,
                snapshot: Some(snapshot_descriptor(5, 6, 8)),
                log: vec![bootstrap_entry(5, 7, b"wrong-term")],
            },
        ),
        Err(BootstrapValidationError::SnapshotBoundaryTermMismatch {
            index: LogIndex(5),
            snapshot_term: Term(6),
            entry_term: Term(7),
        })
    );
}
