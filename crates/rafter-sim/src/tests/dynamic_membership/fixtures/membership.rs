use super::super::super::*;
use rafter::{
    ConfigurationEntry, ConfigurationId, JointMembership, MembershipConfig, MembershipSet,
};

pub(crate) fn add_voter_joint_entry(config_id: ConfigurationId) -> ConfigurationEntry {
    ConfigurationEntry::joint(
        config_id,
        JointMembership::new(initial_learner_set(), stable_four_voter_set()),
    )
}

pub(crate) fn add_voter_final_entry(config_id: ConfigurationId) -> ConfigurationEntry {
    ConfigurationEntry::stable(config_id, stable_four_voter_set())
}

pub(crate) fn remove_node_two_joint_entry(config_id: ConfigurationId) -> ConfigurationEntry {
    ConfigurationEntry::joint(
        config_id,
        JointMembership::new(stable_four_voter_set(), remove_node_two_set()),
    )
}

pub(crate) fn remove_node_two_final_entry(config_id: ConfigurationId) -> ConfigurationEntry {
    ConfigurationEntry::stable(config_id, remove_node_two_set())
}

pub(crate) fn initial_learner_membership() -> MembershipConfig {
    MembershipConfig::stable(initial_learner_set())
}

pub(crate) fn stable_four_voter_membership() -> MembershipConfig {
    MembershipConfig::stable(stable_four_voter_set())
}

fn initial_learner_set() -> MembershipSet {
    membership_set(&[1, 2, 3], &[4])
}

fn stable_four_voter_set() -> MembershipSet {
    membership_set(&[1, 2, 3, 4], &[])
}

fn remove_node_two_set() -> MembershipSet {
    membership_set(&[1, 3, 4], &[])
}

fn membership_set(voters: &[u64], learners: &[u64]) -> MembershipSet {
    MembershipSet::new(
        voters.iter().copied().map(NodeId).collect(),
        learners.iter().copied().map(NodeId).collect(),
    )
    .expect("membership is valid")
}
