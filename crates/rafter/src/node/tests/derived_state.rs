//! Derived-state synchronization after bootstrap and log mutation.

use super::super::state::{AcknowledgementSet, PendingReadRound};
use super::helpers::node;
use super::*;
use crate::{
    AppendEntries, ApplicationSnapshotKind, ApplicationSnapshotMetadata,
    ApplicationSnapshotVersion, BootstrapLogEntry, ConfigurationEntry, ConfigurationId, LogEntry,
    MembershipSet, RaftSnapshot, RaftSnapshotMetadata, ReadId, SnapshotGroupId,
};
use rafter_invariant_test::{oracle_assert, oracle_expect_err};

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

    let validation = node.validate_derived_state();
    oracle_assert!(
        validation.is_ok(),
        "bootstrap must rebuild configuration offsets: {validation:?}"
    );
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

#[rafter_invariant_test::detector_test]
fn derived_state_rejects_log_geometry_overflow() {
    let mut node = node(1, &[2, 3]);
    node.persistent.snapshot = Some(snapshot_descriptor(u64::MAX - 1, 1, 1));
    node.persistent.log.extend([
        LogEntry::application(Term(1), b"overflow-1".to_vec()),
        LogEntry::application(Term(1), b"overflow-2".to_vec()),
    ]);

    let error = oracle_expect_err!(
        detect_derived_state_violation(&node),
        "overflowing logical log geometry must be rejected"
    );
    oracle_assert!(error.contains("overflows LogIndex"), "{error}");
}

#[rafter_invariant_test::detector_test]
fn derived_state_rejects_commit_beyond_log() {
    let mut node = node(1, &[2, 3]);
    node.volatile.commit_index = LogIndex(1);

    let error = oracle_expect_err!(
        detect_derived_state_violation(&node),
        "commit beyond logical log must be rejected"
    );
    oracle_assert!(error.contains("commit index 1 exceeds logical last index 0"));
}

#[rafter_invariant_test::detector_test]
fn derived_state_rejects_apply_beyond_commit() {
    let mut node = node(1, &[2, 3]);
    node.append_log_entry(LogEntry::application(Term(1), b"entry".to_vec()));
    node.volatile.applied_index = LogIndex(1);

    let error = oracle_expect_err!(
        detect_derived_state_violation(&node),
        "apply beyond commit must be rejected"
    );
    oracle_assert!(error.contains("applied index 1 exceeds commit index 0"));
}

#[rafter_invariant_test::detector_test]
fn derived_state_rejects_non_leader_pending_read_round() {
    let mut node = node(1, &[2, 3]);
    let membership = node.effective_membership();
    node.leader.heartbeat_sequence = 1;
    node.leader.pending_reads.push(PendingReadRound {
        read_ids: vec![ReadId(7)],
        read_index: LogIndex::ZERO,
        registered_sequence: 1,
        acks: AcknowledgementSet::new(&membership, node.id()),
    });

    let error = oracle_expect_err!(
        detect_derived_state_violation(&node),
        "a follower cannot retain pending reads"
    );
    oracle_assert!(error.contains("non-leader retains pending read-index rounds"));
}

#[rafter_invariant_test::detector_test]
fn derived_state_rejects_stale_configuration_offsets() {
    let mut node = node(1, &[2, 3]);
    node.derived.push_configuration_offset_for_test(0);

    let error = oracle_expect_err!(
        detect_derived_state_violation(&node),
        "stale configuration offsets must be rejected"
    );
    oracle_assert!(error.contains("configuration_offsets mismatch"));
}

fn config() -> NodeConfig {
    NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
        .expect("test Raft node config is valid")
}

fn detect_derived_state_violation(node: &Node) -> Result<(), String> {
    node.validate_derived_state()
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
