//! Normalize justified identities and reject contradictory durable sources.

use super::support::*;

fn state(index: u64, id: u64) -> CommittedConfiguration {
    CommittedConfiguration {
        index: LogIndex(index),
        config_id: ConfigurationId(id),
    }
}

fn membership(id: u64) -> MembershipConfig {
    MembershipConfig::Stable(
        MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], vec![NodeId(id + 3)]).unwrap(),
    )
}

fn entry(index: u64, id: u64) -> BootstrapLogEntry {
    let MembershipConfig::Stable(membership) = membership(id) else {
        unreachable!()
    };
    BootstrapLogEntry::configuration(
        LogIndex(index),
        Term(2),
        ConfigurationEntry::stable(ConfigurationId(id), membership),
    )
}

fn snapshot(index: u64, id: u64) -> RaftSnapshot {
    RaftSnapshot::from_payload(
        snapshot_metadata(2, 2, 3).with_committed_configuration(
            crate::SnapshotCommittedConfiguration::new(Some(state(index, id)), membership(id)),
        ),
        b"state",
    )
}

fn bootstrap() -> BootstrapState {
    BootstrapState {
        current_term: Term(3),
        voted_for: None,
        commit_index: LogIndex(1),
        committed_configuration: Some(state(1, 1)),
        snapshot: Some(snapshot(2, 2)),
        log: Vec::new(),
    }
}

#[test]
fn snapshot_identity_normalizes_persistent_and_exported_state() {
    let node = Node::from_bootstrap(config(), bootstrap()).unwrap();
    assert_eq!(node.commit_index(), LogIndex(2));
    assert_eq!(node.committed_membership(), membership(2));
    assert_eq!(node.committed_configuration_state(), Some(state(2, 2)));
    assert_eq!(node.persistent.committed_configuration, Some(state(2, 2)));
}

#[test]
fn newer_retained_configuration_wins_without_committing_its_successor() {
    let mut image = bootstrap();
    image.commit_index = LogIndex(3);
    image.log = vec![entry(3, 3), entry(4, 4)];
    for stored in [None, Some(state(1, 1)), Some(state(3, 3))] {
        image.committed_configuration = stored;
        let node = Node::from_bootstrap(config(), image.clone()).unwrap();
        assert_eq!(node.commit_index(), LogIndex(3));
        assert_eq!(node.committed_configuration_state(), Some(state(3, 3)));
        assert_eq!(node.persistent.committed_configuration, Some(state(3, 3)));
        assert_eq!(node.committed_membership(), membership(3));
        assert_eq!(node.effective_membership(), membership(4));
        assert_eq!(
            node.committed_configuration_state_at(LogIndex(2)),
            Some(state(2, 2))
        );
    }
}

#[test]
fn older_retained_hard_state_identity_is_validated_before_normalizing() {
    let mut image = bootstrap();
    image.snapshot = None;
    image.commit_index = LogIndex(3);
    image.log = vec![entry(1, 1), entry(2, 2), entry(3, 3)];
    let node = Node::from_bootstrap(config(), image.clone()).unwrap();
    assert_eq!(node.persistent.committed_configuration, Some(state(3, 3)));
    image.log[0] = entry(1, 9);
    assert_eq!(
        Node::from_bootstrap(config(), image),
        Err(BootstrapValidationError::CommittedConfigurationIdMismatch {
            index: LogIndex(1),
            expected: ConfigurationId(1),
            actual: ConfigurationId(9),
        })
    );
}

#[test]
fn equal_index_snapshot_and_hard_state_disagreement_is_rejected() {
    for retained in [false, true] {
        let mut image = bootstrap();
        image.committed_configuration = Some(state(2, 9));
        if retained {
            image.commit_index = LogIndex(3);
            image.log = vec![entry(3, 3)];
        }
        assert_eq!(
            Node::from_bootstrap(config(), image),
            Err(BootstrapValidationError::CommittedConfigurationIdMismatch {
                index: LogIndex(2),
                expected: ConfigurationId(9),
                actual: ConfigurationId(2),
            })
        );
    }
}

#[test]
fn snapshot_boundary_sentinel_cannot_hide_a_configuration_disagreement() {
    let mut image = bootstrap();
    image.log = vec![entry(2, 9)];
    assert_eq!(
        Node::from_bootstrap(config(), image),
        Err(BootstrapValidationError::CommittedConfigurationIdMismatch {
            index: LogIndex(2),
            expected: ConfigurationId(2),
            actual: ConfigurationId(9),
        })
    );
}

#[test]
fn snapshot_identity_cannot_claim_a_configuration_beyond_its_boundary() {
    let mut image = bootstrap();
    image.snapshot = Some(snapshot(3, 3));
    image.commit_index = LogIndex(3);
    image.log = vec![entry(3, 3)];
    assert_eq!(
        Node::from_bootstrap(config(), image),
        Err(
            BootstrapValidationError::CommittedConfigurationAheadOfCommit {
                committed_configuration_index: LogIndex(3),
                commit_index: LogIndex(2),
            }
        )
    );
}

#[test]
fn snapshot_with_older_explicit_identity_cannot_justify_newer_covered_hard_state() {
    let mut image = bootstrap();
    image.committed_configuration = Some(state(2, 2));
    image.snapshot = Some(snapshot(1, 1));
    assert_eq!(
        Node::from_bootstrap(config(), image),
        Err(BootstrapValidationError::CommittedConfigurationMissing {
            committed_configuration_index: LogIndex(2),
        })
    );
}
