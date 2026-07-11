use super::super::helpers::config;
use super::super::*;

pub(super) const LEADER: NodeId = NodeId(1);

pub(super) fn production_cluster() -> Cluster {
    Cluster::new(vec![
        config(1, &[2, 3], 3),
        config(2, &[1, 3], 9),
        config(3, &[1, 2], 9),
    ])
}

pub(super) fn elect_node_one_with_pre_vote(cluster: &mut Cluster) {
    for _ in 0..6 {
        cluster.tick(LEADER);
        cluster.deliver_all();
        if cluster.role(LEADER) == Role::Leader {
            return;
        }
    }
    assert_eq!(cluster.role(LEADER), Role::Leader);
}

pub(super) fn commit_payload(cluster: &mut Cluster, payload: &[u8]) {
    cluster.propose(LEADER, payload.to_vec());
    cluster.deliver_all();
}

pub(super) fn applied_payloads(cluster: &Cluster, node_id: NodeId) -> Vec<Vec<u8>> {
    cluster
        .applied()
        .iter()
        .filter_map(|applied| {
            (applied.node_id == node_id).then_some(applied.payload.as_ref().to_vec())
        })
        .collect()
}
