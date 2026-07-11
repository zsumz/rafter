use super::super::super::*;
use super::membership::{
    add_voter_final_entry, add_voter_joint_entry, remove_node_two_final_entry,
    remove_node_two_joint_entry,
};
use rafter::ConfigurationId;

pub(crate) fn commit_add_voter_transition(cluster: &mut Cluster, config_id: ConfigurationId) {
    propose_add_voter_joint(cluster, config_id);
    cluster.deliver_all();
    cluster.dangerous_raw_configuration_proposal(
        NodeId(1),
        add_voter_final_entry(config_id.next()),
        Vec::new(),
    );
    cluster.deliver_all();
}

pub(crate) fn propose_add_voter_joint(cluster: &mut Cluster, config_id: ConfigurationId) {
    let barrier = cluster
        .promotion_barrier(NodeId(1), NodeId(4))
        .expect("learner should have a promotion barrier");
    cluster.dangerous_raw_configuration_proposal(
        NodeId(1),
        add_voter_joint_entry(config_id),
        vec![barrier],
    );
}

pub(crate) fn commit_remove_node_two_transition(cluster: &mut Cluster, config_id: ConfigurationId) {
    cluster.dangerous_raw_configuration_proposal(
        NodeId(2),
        remove_node_two_joint_entry(config_id),
        Vec::new(),
    );
    cluster.deliver_all();
    cluster.dangerous_raw_configuration_proposal(
        NodeId(2),
        remove_node_two_final_entry(config_id.next()),
        Vec::new(),
    );
    cluster.deliver_all();
}

pub(crate) fn elect_node_two(cluster: &mut Cluster) {
    for _ in 0..3 {
        cluster.tick(NodeId(2));
    }
    cluster.deliver_all();
    assert_eq!(cluster.role(NodeId(2)), Role::Leader);
}

pub(crate) fn elect_node_one_after_removal(cluster: &mut Cluster) {
    for _ in 0..9 {
        cluster.tick(NodeId(1));
    }
    cluster.deliver_all();
    assert_eq!(cluster.role(NodeId(1)), Role::Leader);
}

pub(crate) fn flush_commit_notifications(cluster: &mut Cluster, leader_id: NodeId) {
    cluster.tick(leader_id);
    cluster.deliver_all();
}
