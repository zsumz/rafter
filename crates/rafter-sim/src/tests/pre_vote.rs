use super::helpers::three_node_cluster;
use super::*;
use rafter::Message;

fn elect_node_one_with_pre_vote(cluster: &mut Cluster) {
    cluster.tick(NodeId(1));
    cluster.tick(NodeId(1));
    cluster.tick(NodeId(1));
    cluster.deliver_all();
}

#[test]
fn pre_vote_cluster_elects_leader() {
    let mut cluster = three_node_cluster();

    elect_node_one_with_pre_vote(&mut cluster);

    assert_eq!(cluster.leaders(), vec![NodeId(1)]);
    assert_eq!(cluster.leaders_in_term(Term(1)), vec![NodeId(1)]);
    assert_eq!(cluster.current_term(NodeId(2)), Term(1));
    assert_eq!(cluster.current_term(NodeId(3)), Term(1));
}

#[test]
fn partitioned_node_rejoins_without_deposing_leader() {
    let mut cluster = three_node_cluster();
    elect_node_one_with_pre_vote(&mut cluster);
    assert_eq!(cluster.leaders(), vec![NodeId(1)]);

    // Node 3 is partitioned: it times out through two full pre-vote rounds
    // while its messages sit undelivered, and its term never moves.
    for _ in 0..18 {
        cluster.tick(NodeId(3));
    }
    assert_eq!(cluster.role(NodeId(3)), Role::PreCandidate);
    assert_eq!(cluster.current_term(NodeId(3)), Term(1));

    // The partition heals: the stranded pre-votes reach peers that heard from
    // the leader within their election timeout, so every one is denied and no
    // real election ever starts.
    let delivered = cluster.deliver_matching(|envelope| {
        envelope.from == NodeId(3) && matches!(envelope.message, Message::PreVote(_))
    });
    assert!(delivered > 0);
    cluster.deliver_all();
    assert_eq!(cluster.role(NodeId(3)), Role::PreCandidate);
    assert_eq!(cluster.leaders(), vec![NodeId(1)]);

    // The leader's next heartbeat returns the rejoined node to Follower with
    // no term change anywhere in the cluster.
    cluster.tick(NodeId(1));
    cluster.deliver_all();

    assert_eq!(cluster.leaders(), vec![NodeId(1)]);
    assert_eq!(cluster.role(NodeId(3)), Role::Follower);
    for node_id in [1, 2, 3] {
        assert_eq!(cluster.current_term(NodeId(node_id)), Term(1));
    }
}
