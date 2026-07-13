//! Snapshot-boundary hydration, membership recovery, and retained suffixes.

use super::super::support::*;
use super::*;

#[test]
fn node_hydrates_from_snapshot_boundary_and_log_suffix() {
    let node = Node::from_bootstrap(
        config(),
        BootstrapState {
            current_term: Term(8),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: Some(snapshot_descriptor(5, 6, 8)),
            log: vec![
                bootstrap_entry(6, 7, b"after-snapshot"),
                bootstrap_entry(7, 8, b"tail"),
            ],
        },
    )
    .expect("snapshot bootstrap state is valid");

    assert_eq!(node.role(), Role::Follower);
    assert_eq!(node.current_term(), Term(8));
    assert_eq!(node.commit_index(), LogIndex(5));
    assert_eq!(node.last_log_index(), LogIndex(7));
    assert_eq!(node.term_at(LogIndex(4)), None);
    assert_eq!(node.term_at(LogIndex(5)), Some(Term(6)));
    assert_eq!(node.entry_at(LogIndex(5)), None);
    assert_eq!(
        node.entry_at(LogIndex(6)).map(|entry| (
            entry.term,
            entry.application_payload().expect("application entry")
        )),
        Some((Term(7), &b"after-snapshot"[..]))
    );
    assert_eq!(
        node.log_entries_from(LogIndex(5)),
        vec![
            LogEntry::application(Term(7), b"after-snapshot".to_vec()),
            LogEntry::application(Term(8), b"tail".to_vec()),
        ]
    );
    assert_eq!(
        node.log_entries_slice_from(LogIndex(5)),
        &[
            LogEntry::application(Term(7), b"after-snapshot".to_vec()),
            LogEntry::application(Term(8), b"tail".to_vec()),
        ]
    );
}
#[test]
fn node_hydrates_committed_membership_from_snapshot() {
    let membership = MembershipConfig::stable(
        MembershipSet::new(vec![NodeId(1), NodeId(3), NodeId(4)], Vec::new())
            .expect("membership is valid"),
    );
    let node = Node::from_bootstrap(
        config(),
        BootstrapState {
            current_term: Term(8),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: Some(RaftSnapshot::from_payload(
                snapshot_metadata(5, 6, 8).with_committed_membership(membership.clone()),
                b"",
            )),
            log: Vec::new(),
        },
    )
    .expect("snapshot bootstrap state is valid");

    assert_eq!(node.effective_membership(), membership);
    assert!(node.is_effective_voter(NodeId(4)));
    assert!(!node.is_effective_voter(NodeId(2)));
}
#[test]
fn log_suffix_configuration_overrides_snapshot_membership_after_restart() {
    let snapshot_membership = MembershipConfig::stable(
        MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
            .expect("snapshot membership is valid"),
    );
    let old = MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
        .expect("old membership is valid");
    let new = MembershipSet::new(vec![NodeId(1), NodeId(3), NodeId(4)], Vec::new())
        .expect("new membership is valid");
    let joint = ConfigurationEntry::joint(
        ConfigurationId(9),
        JointMembership::new(old.clone(), new.clone()),
    );

    let node = Node::from_bootstrap(
        config(),
        BootstrapState {
            current_term: Term(8),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: Some(RaftSnapshot::from_payload(
                snapshot_metadata(5, 6, 8).with_committed_membership(snapshot_membership),
                b"",
            )),
            log: vec![BootstrapLogEntry::configuration(
                LogIndex(6),
                Term(8),
                joint,
            )],
        },
    )
    .expect("snapshot bootstrap state is valid");

    assert_eq!(
        node.effective_membership(),
        MembershipConfig::joint(old, new)
    );
}
#[test]
fn bootstrap_recovers_committed_joint_and_stable_configurations_after_snapshot() {
    let snapshot_membership = MembershipConfig::stable(
        MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
            .expect("snapshot membership is valid"),
    );
    let old = MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
        .expect("old membership is valid");
    let new = MembershipSet::new(vec![NodeId(1), NodeId(3), NodeId(4)], Vec::new())
        .expect("new membership is valid");
    let joint = ConfigurationEntry::joint(
        ConfigurationId(10),
        JointMembership::new(old.clone(), new.clone()),
    );
    let stable = ConfigurationEntry::stable(ConfigurationId(11), new.clone());
    let mut log = (6..15)
        .map(|index| bootstrap_entry(index, 8, b"normal"))
        .collect::<Vec<_>>();
    log.push(BootstrapLogEntry::configuration(
        LogIndex(15),
        Term(8),
        joint.clone(),
    ));
    log.push(BootstrapLogEntry::configuration(
        LogIndex(16),
        Term(8),
        stable.clone(),
    ));

    let node = Node::from_bootstrap(
        config(),
        BootstrapState {
            current_term: Term(8),
            voted_for: None,
            commit_index: LogIndex(16),
            committed_configuration: Some(CommittedConfiguration {
                index: LogIndex(16),
                config_id: ConfigurationId(11),
            }),
            snapshot: Some(RaftSnapshot::from_payload(
                snapshot_metadata(5, 6, 8).with_committed_membership(snapshot_membership),
                b"",
            )),
            log,
        },
    )
    .expect("committed post-snapshot configuration suffix is valid");

    assert_eq!(node.commit_index(), LogIndex(16));
    assert_eq!(node.committed_configuration_entry(), Some(stable.clone()));
    assert_eq!(node.effective_configuration_entry(), Some(stable));
    assert_eq!(node.committed_membership(), MembershipConfig::stable(new));
    assert_eq!(
        node.committed_configuration_state(),
        Some(CommittedConfiguration {
            index: LogIndex(16),
            config_id: ConfigurationId(11),
        })
    );
}
#[test]
fn bootstrap_accepts_persisted_vote_for_dynamic_voter_after_snapshot() {
    let snapshot_membership = MembershipConfig::stable(
        MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
            .expect("snapshot membership is valid"),
    );
    let new = MembershipSet::new(vec![NodeId(1), NodeId(3), NodeId(4)], Vec::new())
        .expect("new membership is valid");
    let stable = ConfigurationEntry::stable(ConfigurationId(11), new.clone());

    let node = Node::from_bootstrap(
        config(),
        BootstrapState {
            current_term: Term(9),
            voted_for: Some(NodeId(4)),
            commit_index: LogIndex(6),
            committed_configuration: Some(CommittedConfiguration {
                index: LogIndex(6),
                config_id: ConfigurationId(11),
            }),
            snapshot: Some(RaftSnapshot::from_payload(
                snapshot_metadata(5, 6, 8).with_committed_membership(snapshot_membership),
                b"",
            )),
            log: vec![BootstrapLogEntry::configuration(
                LogIndex(6),
                Term(8),
                stable,
            )],
        },
    )
    .expect("vote for a committed dynamic voter bootstraps");

    assert_eq!(node.voted_for(), Some(NodeId(4)));
    assert_eq!(node.committed_membership(), MembershipConfig::stable(new));
}
#[test]
fn follower_rejects_second_uncommitted_configuration_after_snapshot_membership() {
    let membership = MembershipConfig::stable(
        MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
            .expect("membership is valid"),
    );
    let old = MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
        .expect("old membership is valid");
    let new = MembershipSet::new(vec![NodeId(1), NodeId(3), NodeId(4)], Vec::new())
        .expect("new membership is valid");
    let mut node = Node::from_bootstrap(
        config(),
        BootstrapState {
            current_term: Term(8),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: Some(RaftSnapshot::from_payload(
                snapshot_metadata(5, 6, 8).with_committed_membership(membership),
                b"",
            )),
            log: vec![BootstrapLogEntry::configuration(
                LogIndex(6),
                Term(8),
                ConfigurationEntry::joint(
                    ConfigurationId(9),
                    JointMembership::new(old.clone(), new.clone()),
                ),
            )],
        },
    )
    .expect("snapshot bootstrap state is valid");

    let outputs = node.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(8),
            leader_id: NodeId(2),
            prev_log_index: LogIndex(6),
            prev_log_term: Term(8),
            entries: vec![LogEntry::configuration(
                Term(8),
                ConfigurationEntry::stable(ConfigurationId(10), new),
            )]
            .into(),
            leader_commit: LogIndex(5),
        }),
    });

    assert_eq!(node.last_log_index(), LogIndex(6));
    assert_append_entries_response(&outputs, NodeId(2), false, LogIndex::ZERO);
}
#[test]
fn bootstrap_hydrates_snapshot_descriptor() {
    let descriptor = RaftSnapshot::from_payload(snapshot_metadata(5, 6, 8), b"catalog snapshot");
    let node = Node::from_bootstrap(
        config(),
        BootstrapState {
            current_term: Term(8),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: Some(descriptor.clone()),
            log: Vec::new(),
        },
    )
    .expect("node hydrates from snapshot descriptor");

    let snapshot = node.snapshot().expect("snapshot descriptor is hydrated");
    assert_eq!(snapshot.metadata, descriptor.metadata);
    assert_eq!(
        snapshot.application_payload_len,
        b"catalog snapshot".len() as u64
    );
    assert_eq!(snapshot.transfer_id(), descriptor.transfer_id());
}
