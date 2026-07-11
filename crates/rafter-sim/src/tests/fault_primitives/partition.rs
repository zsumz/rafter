use super::super::helpers::direct_election_config;
use super::super::*;
use super::fixtures::{commit_payload, elect_node_one_with_pre_vote, LEADER};

#[test]
fn a_sustained_partition_holds_across_an_election_and_heals() {
    // Direct elections: the isolated pair must depose the old leader, and
    // pre-vote stickiness would (correctly) slow that scenario down.
    let mut cluster = Cluster::new(vec![
        direct_election_config(1, &[2, 3], 3),
        direct_election_config(2, &[1, 3], 5),
        direct_election_config(3, &[1, 2], 9),
    ]);
    elect_node_one_with_pre_vote(&mut cluster);
    commit_payload(&mut cluster, b"before-partition");

    // The partition drops in-flight traffic and everything after it — no
    // per-envelope scheduling can leak a message through.
    cluster.partition_isolate(LEADER);
    assert!(cluster.partitioned(LEADER, NodeId(2)));

    // The majority side elects a new leader THROUGH the sustained
    // partition: many ticks, an entire election, and the old leader's
    // heartbeats never arrive.
    for _ in 0..5 {
        cluster.tick(NodeId(2));
    }
    cluster.deliver_all();
    assert_eq!(cluster.role(NodeId(2)), Role::Leader);
    assert!(cluster.current_term(NodeId(2)) > cluster.current_term(LEADER));

    // The isolated ex-leader's check-quorum deposes it without any peer
    // traffic (acknowledgements recorded before the isolation satisfy the
    // first evaluation, so deposal takes up to two periods).
    for _ in 0..6 {
        cluster.tick(LEADER);
    }
    assert_ne!(cluster.role(LEADER), Role::Leader);

    // Healing restores the flow: the ex-leader rejoins the new term and
    // the pre-partition commit survives everywhere.
    cluster.heal_partitions();
    cluster.propose(NodeId(2), b"after-heal".to_vec());
    cluster.deliver_all();
    cluster.tick(NodeId(2));
    cluster.deliver_all();
    assert_eq!(
        cluster.current_term(LEADER),
        cluster.current_term(NodeId(2))
    );
    assert_eq!(
        cluster.log_entries_from(LEADER, LogIndex(1)),
        cluster.log_entries_from(NodeId(2), LogIndex(1)),
    );
}
