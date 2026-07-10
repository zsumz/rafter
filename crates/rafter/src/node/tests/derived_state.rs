use super::super::*;
use super::helpers::node;
use crate::{
    AppendEntries, ApplicationSnapshotKind, ApplicationSnapshotMetadata,
    ApplicationSnapshotVersion, BootstrapLogEntry, ConfigurationEntry, ConfigurationId, LogEntry,
    MembershipSet, RaftSnapshot, RaftSnapshotMetadata, SnapshotGroupId,
};

#[test]
fn derived_state_is_valid_after_bootstrap() {
    let node = Node::from_bootstrap(
        config(),
        BootstrapState {
            current_term: Term(2),
            voted_for: None,
            commit_index: LogIndex(2),
            committed_configuration: Some(CommittedConfiguration {
                index: LogIndex(2),
                config_id: ConfigurationId(10),
            }),
            snapshot: None,
            log: vec![
                BootstrapLogEntry::application(LogIndex(1), Term(1), b"one".to_vec()),
                BootstrapLogEntry::configuration(LogIndex(2), Term(2), stable_configuration(10)),
                BootstrapLogEntry::application(LogIndex(3), Term(2), b"three".to_vec()),
                BootstrapLogEntry::configuration(LogIndex(4), Term(2), stable_configuration(11)),
            ],
        },
    )
    .expect("bootstrap state is valid");

    node.validate_derived_state()
        .expect("bootstrap rebuilds configuration offsets");
}

#[test]
fn derived_state_is_valid_after_append() {
    let mut follower = node(2, &[1, 3]);
    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(2),
            leader_id: NodeId(1),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            entries: vec![
                LogEntry::application(Term(2), b"one".to_vec()),
                LogEntry::configuration(Term(2), stable_configuration(20)),
            ]
            .into(),
            leader_commit: LogIndex::ZERO,
        }),
    });

    assert!(outputs
        .iter()
        .any(|output| matches!(output, Output::Send { .. })));
    follower
        .validate_derived_state()
        .expect("append updates configuration offsets");
}

#[test]
fn derived_state_is_valid_after_truncate() {
    let mut follower = node(2, &[1, 3]);
    let _ = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(2),
            leader_id: NodeId(1),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            entries: vec![
                LogEntry::configuration(Term(2), stable_configuration(30)),
                LogEntry::application(Term(2), b"old-tail".to_vec()),
            ]
            .into(),
            leader_commit: LogIndex::ZERO,
        }),
    });

    let _ = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 1,
            term: Term(3),
            leader_id: NodeId(1),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            entries: vec![LogEntry::application(Term(3), b"new-tail".to_vec())].into(),
            leader_commit: LogIndex::ZERO,
        }),
    });

    follower
        .validate_derived_state()
        .expect("truncate retains only live configuration offsets");
}

#[test]
fn derived_state_is_valid_after_replace_log() {
    let mut follower = node(2, &[1, 3]);
    let _ = follower.replace_log(
        vec![
            LogEntry::application(Term(3), b"one".to_vec()),
            LogEntry::configuration(Term(3), stable_configuration(40)),
            LogEntry::application(Term(3), b"three".to_vec()),
        ],
        LocalProposalDropReason::LeadershipLost,
    );

    follower
        .validate_derived_state()
        .expect("replace_log rebuilds configuration offsets");
}

#[test]
fn derived_state_is_valid_after_snapshot_install() {
    let mut follower = Node::from_bootstrap(
        config(),
        BootstrapState {
            current_term: Term(3),
            voted_for: None,
            commit_index: LogIndex(3),
            committed_configuration: None,
            snapshot: None,
            log: vec![
                BootstrapLogEntry::configuration(LogIndex(1), Term(1), stable_configuration(50)),
                BootstrapLogEntry::application(LogIndex(2), Term(2), b"covered".to_vec()),
                BootstrapLogEntry::configuration(LogIndex(3), Term(3), stable_configuration(51)),
            ],
        },
    )
    .expect("bootstrap state is valid");

    let _ = follower.install_local_snapshot(snapshot_descriptor(2, 2, 3));

    follower
        .validate_derived_state()
        .expect("snapshot compaction rebuilds retained configuration offsets");
}

fn config() -> NodeConfig {
    NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
        .expect("test Raft node config is valid")
}

fn stable_configuration(config_id: u64) -> ConfigurationEntry {
    let membership = MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
        .expect("test membership is valid");
    ConfigurationEntry::stable(ConfigurationId(config_id), membership)
}

fn snapshot_descriptor(index: u64, term: u64, hard_state_term: u64) -> RaftSnapshot {
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("derived-state").expect("snapshot group id is valid"),
        NodeId(1),
        LogIndex(index),
        Term(term),
        Term(hard_state_term),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("derived_state").expect("snapshot kind is valid"),
            ApplicationSnapshotVersion::new(1).expect("snapshot version is valid"),
        ),
    )
    .expect("snapshot metadata is valid");
    RaftSnapshot::from_payload(metadata, b"")
}
