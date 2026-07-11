mod clusters;
mod membership;
mod snapshots;
mod transitions;

use super::super::*;

pub(super) use self::clusters::{
    direct_election_learner_four_node_cluster, direct_election_stable_four_voter_cluster,
    learner_four_node_cluster,
};
pub(super) use self::membership::{add_voter_final_entry, add_voter_joint_entry};
pub(super) use self::snapshots::restart_all_nodes_from_compacted_snapshots;
pub(super) use self::transitions::{
    commit_add_voter_transition, commit_remove_node_two_transition, elect_node_one_after_removal,
    elect_node_two, flush_commit_notifications, propose_add_voter_joint,
};

pub(super) fn applied_payloads(cluster: &Cluster, node_id: NodeId) -> Vec<rafter::SharedPayload> {
    cluster
        .applied()
        .iter()
        .filter_map(|applied| (applied.node_id == node_id).then_some(applied.payload.clone()))
        .collect()
}
