use super::helpers::{pre_vote, seeded_three_node_cluster, three_node_cluster};
use super::*;
use rafter::Message;

#[test]
fn simulator_can_reorder_delivery() {
    let mut cluster = three_node_cluster();

    cluster.tick(NodeId(1));
    cluster.tick(NodeId(1));
    cluster.tick(NodeId(1));

    assert!(cluster.deliver_one_matching(|envelope| envelope.to == NodeId(3)));
    assert!(cluster.deliver_one_matching(|envelope| envelope.to == NodeId(2)));
    cluster.deliver_all();

    assert_eq!(cluster.leaders(), vec![NodeId(1)]);
}

#[test]
fn simulator_can_drop_messages() {
    let mut cluster = three_node_cluster();

    cluster.tick(NodeId(1));
    cluster.tick(NodeId(1));
    cluster.tick(NodeId(1));

    assert_eq!(
        cluster.drop_matching(|envelope| matches!(envelope.message, Message::PreVote(_))),
        2
    );
    cluster.deliver_all();

    assert_eq!(cluster.leaders(), Vec::<NodeId>::new());
}

#[test]
fn simulator_can_delay_messages() {
    let mut cluster = three_node_cluster();

    cluster.tick(NodeId(1));
    cluster.tick(NodeId(1));
    cluster.tick(NodeId(1));

    assert_eq!(cluster.delay_matching(pre_vote(NodeId(1), NodeId(2)), 2), 1);
    assert!(!cluster.deliver_one_matching(pre_vote(NodeId(1), NodeId(2))));
    assert!(cluster.deliver_one_matching(pre_vote(NodeId(1), NodeId(3))));

    cluster.advance_clock();
    assert!(!cluster.deliver_one_matching(pre_vote(NodeId(1), NodeId(2))));

    cluster.advance_clock();
    assert!(cluster.deliver_one_matching(pre_vote(NodeId(1), NodeId(2))));
}

#[test]
fn fixed_seed_reproduces_random_delivery_order() {
    fn random_delivery_order(cluster: &mut Cluster) -> Vec<(NodeId, NodeId)> {
        cluster.tick(NodeId(1));
        cluster.tick(NodeId(1));
        cluster.tick(NodeId(1));

        let mut delivered = Vec::new();
        while let Some(envelope) = cluster.deliver_random_ready() {
            delivered.push((envelope.from, envelope.to));
        }
        delivered
    }

    let mut first = seeded_three_node_cluster(7);
    let mut second = seeded_three_node_cluster(7);

    assert_eq!(
        random_delivery_order(&mut first),
        random_delivery_order(&mut second)
    );
    assert_eq!(first.leaders(), second.leaders());
}
