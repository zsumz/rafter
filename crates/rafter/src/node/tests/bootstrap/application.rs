//! Applied-floor recovery and local proposal reincarnation boundaries.

use super::*;

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
    assert_eq!(
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

    assert_eq!(
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
    assert!(
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
