use crate::{MembershipConfig, MembershipSet, MembershipValidationError, NodeConfig, NodeId};

#[test]
fn stable_membership_sorts_voters_and_learners() {
    let membership = MembershipSet::new(
        vec![NodeId(3), NodeId(1), NodeId(2)],
        vec![NodeId(5), NodeId(4)],
    )
    .expect("membership is valid");

    assert_eq!(membership.voters(), &[NodeId(1), NodeId(2), NodeId(3)]);
    assert_eq!(membership.learners(), &[NodeId(4), NodeId(5)]);
    assert_eq!(membership.quorum_size(), 2);
}

#[test]
fn stable_membership_rejects_invalid_node_sets() {
    assert_eq!(
        MembershipSet::new(Vec::new(), Vec::new()),
        Err(MembershipValidationError::EmptyVoters)
    );
    assert_eq!(
        MembershipSet::new(vec![NodeId(1), NodeId(1)], Vec::new()),
        Err(MembershipValidationError::DuplicateVoter { node_id: NodeId(1) })
    );
    assert_eq!(
        MembershipSet::new(vec![NodeId(1)], vec![NodeId(2), NodeId(2)]),
        Err(MembershipValidationError::DuplicateLearner { node_id: NodeId(2) })
    );
    assert_eq!(
        MembershipSet::new(vec![NodeId(1), NodeId(2)], vec![NodeId(2)]),
        Err(MembershipValidationError::LearnerVoterOverlap { node_id: NodeId(2) })
    );
}

#[test]
fn stable_membership_quorum_counts_only_voters() {
    let membership = MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], vec![NodeId(4)])
        .expect("membership is valid");

    assert!(membership.has_quorum([NodeId(1), NodeId(2)]));
    assert!(membership.has_quorum([NodeId(1), NodeId(2), NodeId(4)]));
    assert!(!membership.has_quorum([NodeId(1), NodeId(4)]));
    assert!(!membership.has_quorum([NodeId(4)]));
}

#[test]
fn stable_membership_majority_handles_even_voter_sets() {
    let membership =
        MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3), NodeId(4)], Vec::new())
            .expect("membership is valid");

    assert_eq!(membership.quorum_size(), 3);
    assert!(membership.has_quorum([NodeId(1), NodeId(2), NodeId(3)]));
    assert!(!membership.has_quorum([NodeId(1), NodeId(2)]));
}

#[test]
fn joint_membership_requires_old_and_new_majorities() {
    let old = MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
        .expect("old membership is valid");
    let new = MembershipSet::new(vec![NodeId(2), NodeId(3), NodeId(4)], Vec::new())
        .expect("new membership is valid");
    let joint = MembershipConfig::joint(old, new);

    assert!(joint.has_quorum([NodeId(2), NodeId(3)]));
    assert!(joint.has_quorum([NodeId(1), NodeId(2), NodeId(4)]));
    assert!(!joint.has_quorum([NodeId(1), NodeId(2)]));
    assert!(!joint.has_quorum([NodeId(3), NodeId(4)]));
}

#[test]
fn joint_membership_ignores_learners_for_both_majorities() {
    let old = MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], vec![NodeId(5)])
        .expect("old membership is valid");
    let new = MembershipSet::new(vec![NodeId(3), NodeId(4), NodeId(5)], vec![NodeId(1)])
        .expect("new membership is valid");
    let joint = MembershipConfig::joint(old, new);

    assert!(joint.has_quorum([NodeId(2), NodeId(3), NodeId(4)]));
    assert!(!joint.has_quorum([NodeId(1), NodeId(3)]));
    assert!(!joint.has_quorum([NodeId(3), NodeId(5)]));
}

#[test]
fn node_config_static_quorum_behavior_is_unchanged() {
    let config =
        NodeConfig::new(NodeId(1), vec![NodeId(3), NodeId(2)], 5).expect("config is valid");

    assert_eq!(
        config.voters().collect::<Vec<_>>(),
        vec![NodeId(1), NodeId(3), NodeId(2)]
    );
    assert_eq!(config.quorum_size(), 2);
}
