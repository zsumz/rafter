//! Static, effective, committed, and snapshot membership views.

use super::support::*;

#[test]
fn node_without_configuration_entries_uses_static_membership() {
    let node = node(1, &[2, 3]);
    let expected = MembershipConfig::stable(
        MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
            .expect("static membership is valid"),
    );

    assert_eq!(node.effective_membership(), expected);
    assert!(node.is_effective_voter(NodeId(1)));
    assert!(node.is_effective_voter(NodeId(3)));
    assert!(!node.is_effective_voter(NodeId(4)));
}

#[test]
fn uncommitted_joint_configuration_becomes_effective_from_local_log() {
    let mut follower = node(2, &[1, 3, 4]);
    let joint = joint_configuration(ConfigurationId(9));

    let outputs = follower.step(Input::Message {
        from: NodeId(1),
        message: Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(2),
            leader_id: NodeId(1),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            entries: vec![LogEntry::configuration(Term(2), joint.clone())].into(),
            leader_commit: LogIndex::ZERO,
        }),
    });

    assert_eq!(follower.commit_index(), LogIndex::ZERO);
    assert_eq!(follower.last_log_index(), LogIndex(1));
    assert_eq!(follower.effective_membership(), joint.membership_config());
    assert!(follower
        .effective_membership()
        .has_quorum([NodeId(1), NodeId(3)]));
    assert!(!follower
        .effective_membership()
        .has_quorum([NodeId(1), NodeId(2)]));
    assert_append_entries_response(&outputs, NodeId(1), true, LogIndex(1));
}

#[test]
fn committed_membership_ignores_uncommitted_configuration_suffix() {
    let mut follower = node(2, &[1, 3, 4]);
    let joint = joint_configuration(ConfigurationId(9));
    let static_membership = follower.committed_membership();

    assert_append_entries_response(
        &follower.step(Input::Message {
            from: NodeId(1),
            message: Message::AppendEntries(AppendEntries {
                sequence: 0,
                term: Term(2),
                leader_id: NodeId(1),
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term::default(),
                entries: vec![LogEntry::configuration(Term(2), joint.clone())].into(),
                leader_commit: LogIndex::ZERO,
            }),
        }),
        NodeId(1),
        true,
        LogIndex(1),
    );

    assert_eq!(follower.effective_membership(), joint.membership_config());
    assert_eq!(follower.committed_membership(), static_membership);
    assert_eq!(follower.effective_configuration_entry(), Some(joint));
    assert_eq!(follower.committed_configuration_entry(), None);
}
