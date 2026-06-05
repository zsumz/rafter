use super::super::*;
use super::helpers::{assert_append_entries_response, assert_vote_response, bootstrap_entry};
use crate::{
    AppendEntries, ApplicationSnapshotKind, ApplicationSnapshotMetadata,
    ApplicationSnapshotVersion, CommittedConfiguration, ConfigurationEntry, ConfigurationId,
    JointMembership, LogEntry, MembershipConfig, MembershipSet, RaftSnapshot, RaftSnapshotMetadata,
    RequestVote, SnapshotGroupId, SnapshotMetadataError,
};

mod snapshot;

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
    assert_eq!(
        node.log_entries_from(LogIndex(2)),
        vec![LogEntry::application(Term(7), b"append".to_vec())]
    );
    assert_eq!(node.log_entries_from(LogIndex(3)), Vec::new());
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

fn config() -> NodeConfig {
    NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
        .expect("test Raft node config is valid")
}

fn snapshot_metadata(index: u64, term: u64, hard_state_term: u64) -> RaftSnapshotMetadata {
    RaftSnapshotMetadata::new(
        SnapshotGroupId::new("metadata").expect("snapshot group id is valid"),
        NodeId(1),
        LogIndex(index),
        Term(term),
        Term(hard_state_term),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("metadata_catalog").expect("snapshot kind is valid"),
            ApplicationSnapshotVersion::new(1).expect("snapshot version is valid"),
        ),
    )
    .expect("snapshot metadata is valid")
}

fn snapshot_descriptor(index: u64, term: u64, hard_state_term: u64) -> RaftSnapshot {
    RaftSnapshot::from_payload(snapshot_metadata(index, term, hard_state_term), b"")
}

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
            entries: Vec::new(),
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
