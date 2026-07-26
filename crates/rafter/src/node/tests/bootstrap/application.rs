//! Applied-floor recovery and local proposal reincarnation boundaries.

use super::support::snapshot_descriptor;
use super::*;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};

#[test]
fn applied_floor_suppresses_reapply_below_it() {
    let mut node = Node::from_bootstrap_applied_through(
        NodeConfig::new(NodeId(2), vec![NodeId(1), NodeId(3)], 3).expect("valid config"),
        BootstrapState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex(2),
            committed_configuration: None,
            snapshot: None,
            log: vec![
                bootstrap_entry(1, 1, b"one"),
                bootstrap_entry(2, 1, b"two"),
                bootstrap_entry(3, 1, b"three"),
            ],
        },
        LogIndex(2),
    )
    .expect("floor within log bootstraps");

    // The leader advances the commit index over the whole log; only the
    // entry above the declared floor replays.
    let outputs = node.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(crate::AppendEntries {
            term: Term(1),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(3),
            prev_log_term: Term(1),
            entries: Vec::new().into(),
            leader_commit: LogIndex(3),
            sequence: 1,
        }),
    });

    let applied: Vec<LogIndex> = outputs
        .iter()
        .filter_map(|output| match output {
            Output::Apply { index, .. } => Some(*index),
            _ => None,
        })
        .collect();
    oracle_assert_eq!(
        applied,
        vec![LogIndex(3)],
        "entries at or below the floor stay silent"
    );
}
#[test]
fn committed_entries_above_applied_floor_drain_immediately_after_bootstrap() {
    let mut node = Node::from_bootstrap_applied_through(
        NodeConfig::new(NodeId(2), vec![NodeId(1), NodeId(3)], 3).expect("valid config"),
        BootstrapState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex(3),
            committed_configuration: None,
            snapshot: None,
            log: vec![
                bootstrap_entry(1, 1, b"one"),
                bootstrap_entry(2, 1, b"two"),
                bootstrap_entry(3, 1, b"three"),
            ],
        },
        LogIndex(1),
    )
    .expect("floor within committed log bootstraps");

    oracle_assert_eq!(
        node.drain_committed_outputs(),
        vec![
            Output::Apply {
                index: LogIndex(2),
                term: Term(1),
                payload: b"two".to_vec().into(),
                local_proposal_id: None,
            },
            Output::Apply {
                index: LogIndex(3),
                term: Term(1),
                payload: b"three".to_vec().into(),
                local_proposal_id: None,
            },
        ],
        "committed entries above the applied floor replay without another Raft message"
    );
    oracle_assert!(
        node.drain_committed_outputs().is_empty(),
        "draining advances the volatile applied index"
    );
}
#[test]
fn bootstrap_does_not_restore_local_proposal_tracking() {
    let mut node = Node::from_bootstrap_applied_through(
        NodeConfig::new(NodeId(2), vec![NodeId(1), NodeId(3)], 3).expect("valid config"),
        BootstrapState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex(1),
            committed_configuration: None,
            snapshot: None,
            log: vec![bootstrap_entry(1, 1, b"committed-before-restart")],
        },
        LogIndex::ZERO,
    )
    .expect("floor within committed log bootstraps");

    assert!(
        node.volatile.local_proposals.is_empty(),
        "local proposal tracking is volatile and not hydrated from durable state"
    );
    assert_eq!(
        node.drain_committed_outputs(),
        vec![Output::Apply {
            index: LogIndex(1),
            term: Term(1),
            payload: b"committed-before-restart".to_vec().into(),
            local_proposal_id: None,
        }]
    );
}
#[test]
fn applied_floor_beyond_log_is_rejected() {
    let error = Node::from_bootstrap_applied_through(
        NodeConfig::new(NodeId(2), vec![NodeId(1), NodeId(3)], 3).expect("valid config"),
        BootstrapState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: None,
            log: vec![bootstrap_entry(1, 1, b"only")],
        },
        LogIndex(5),
    )
    .expect_err("floor beyond the log must fail");
    assert!(matches!(
        error,
        BootstrapValidationError::AppliedFloorBeyondLog {
            applied_through: LogIndex(5),
            last_log_index: LogIndex(1),
        }
    ));
}
#[test]
fn applied_floor_beyond_commit_is_rejected() {
    let error = Node::from_bootstrap_applied_through(
        NodeConfig::new(NodeId(2), vec![NodeId(1), NodeId(3)], 3).expect("valid config"),
        BootstrapState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex(1),
            committed_configuration: None,
            snapshot: None,
            log: vec![
                bootstrap_entry(1, 1, b"committed"),
                bootstrap_entry(2, 1, b"uncommitted"),
            ],
        },
        LogIndex(2),
    )
    .expect_err("floor beyond the committed prefix must fail");

    assert!(matches!(
        error,
        BootstrapValidationError::AppliedFloorBeyondCommit {
            applied_through: LogIndex(2),
            commit_index: LogIndex(1),
        }
    ));
}

fn compacted_bootstrap() -> BootstrapState {
    BootstrapState {
        current_term: Term(1),
        voted_for: None,
        commit_index: LogIndex(4),
        committed_configuration: None,
        snapshot: Some(snapshot_descriptor(3, 1, 1)),
        log: vec![bootstrap_entry(4, 1, b"above-the-boundary")],
    }
}

/// A declared floor below the snapshot boundary is raised to it, and the gap
/// is never emitted in any form. This is the kernel's documented behavior
/// rather than a defect it can repair — it retains nothing at or below the
/// boundary and holds a descriptor rather than payload bytes — but it is also
/// the reason the composition above must check, because nothing here reports
/// that entries were skipped.
///
/// Both indexes the caller needs for that check are readable afterwards, and
/// this test is what makes that a supported way to detect the raise rather
/// than an incidental one.
#[test]
fn applied_floor_below_the_snapshot_boundary_is_raised_and_the_gap_is_never_emitted() {
    for declared in [LogIndex::ZERO, LogIndex(2)] {
        let mut node = Node::from_bootstrap_applied_through(
            NodeConfig::new(NodeId(2), vec![NodeId(1), NodeId(3)], 3).expect("valid config"),
            compacted_bootstrap(),
            declared,
        )
        .expect("the kernel cannot refuse a floor a correct recovery reaches");

        oracle_assert_eq!(node.applied_index(), LogIndex(3));
        oracle_assert!(
            node.applied_index() > declared,
            "the declaration {declared} was raised, and only these two indexes say so"
        );
        oracle_assert_eq!(node.snapshot_index(), LogIndex(3));
        oracle_assert_eq!(
            node.drain_committed_outputs(),
            vec![Output::Apply {
                index: LogIndex(4),
                term: Term(1),
                payload: b"above-the-boundary".to_vec().into(),
                local_proposal_id: None,
            }],
            "nothing between {declared} and the boundary is replayed"
        );
    }
}

/// The boundary itself is the lowest floor the retained log can serve, and a
/// declaration at it is honored exactly.
#[test]
fn applied_floor_at_the_snapshot_boundary_is_honored_exactly() {
    let node = Node::from_bootstrap_applied_through(
        NodeConfig::new(NodeId(2), vec![NodeId(1), NodeId(3)], 3).expect("valid config"),
        compacted_bootstrap(),
        LogIndex(3),
    )
    .expect("a floor at the boundary bootstraps");

    oracle_assert_eq!(node.applied_index(), LogIndex(3));
    oracle_assert_eq!(node.applied_index(), node.snapshot_index());
}
