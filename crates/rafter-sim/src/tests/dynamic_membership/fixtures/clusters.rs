use super::super::super::helpers::{config, direct_election_config};
use super::super::super::*;
use super::membership::{initial_learner_membership, stable_four_voter_membership};
use super::snapshots::snapshot_metadata;
use rafter::{BootstrapState, MembershipConfig, NodeConfig, RaftSnapshot};

pub(crate) fn learner_four_node_cluster(seed: SimSeed) -> Cluster {
    cluster_with_membership(seed, &initial_learner_membership(), NodeId(1), config)
}

/// The learner cluster in the minimal-protocol posture, for the partition
/// scenario whose successive candidacies pre-vote stickiness would deny.
pub(crate) fn direct_election_learner_four_node_cluster(seed: SimSeed) -> Cluster {
    cluster_with_membership(
        seed,
        &initial_learner_membership(),
        NodeId(1),
        direct_election_config,
    )
}

/// A four-voter cluster in the minimal-protocol posture: the removal test
/// elects a replacement leader whose peers still hold the removed leader's
/// hint, a direct election by construction.
pub(crate) fn direct_election_stable_four_voter_cluster(seed: SimSeed, leader: NodeId) -> Cluster {
    cluster_with_membership(
        seed,
        &stable_four_voter_membership(),
        leader,
        direct_election_config,
    )
}

fn cluster_with_membership(
    seed: SimSeed,
    membership: &MembershipConfig,
    fast_node: NodeId,
    node_config: fn(u64, &[u64], u64) -> NodeConfig,
) -> Cluster {
    let mut cluster = Cluster::new_with_seed(
        [NodeId(1), NodeId(2), NodeId(3), NodeId(4)]
            .into_iter()
            .map(|node_id| {
                let timeout = if node_id == fast_node { 3 } else { 9 };
                let peers = [1, 2, 3, 4]
                    .into_iter()
                    .filter(|id| *id != node_id.0)
                    .collect::<Vec<_>>();
                node_config(node_id.0, &peers, timeout)
            })
            .collect(),
        seed,
    );

    for node_id in [NodeId(1), NodeId(2), NodeId(3), NodeId(4)] {
        let snapshot = RaftSnapshot::from_payload(snapshot_metadata(membership.clone()), b"");
        cluster.seed_snapshot_payload(node_id, &snapshot, Vec::new());
        cluster
            .restart_node_from_bootstrap(
                node_id,
                BootstrapState {
                    current_term: Term(1),
                    voted_for: None,
                    commit_index: LogIndex::ZERO,
                    committed_configuration: None,
                    snapshot: Some(snapshot),
                    log: Vec::new(),
                },
            )
            .expect("membership bootstrap is valid");
    }

    cluster
}
