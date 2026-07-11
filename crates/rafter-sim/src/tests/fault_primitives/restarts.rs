use super::super::*;
use super::fixtures::{
    applied_payloads, commit_payload, elect_node_one_with_pre_vote, production_cluster, LEADER,
};

#[test]
fn a_lossy_restart_preserves_durable_hard_state_and_the_local_committed_prefix() {
    let mut cluster = production_cluster();
    elect_node_one_with_pre_vote(&mut cluster);
    commit_payload(&mut cluster, b"committed");
    let committed_prefix = cluster.log_entries_from(LEADER, LogIndex(1));
    let committed_index = cluster.commit_index(LEADER);
    let committed_configuration = cluster.committed_configuration_state(LEADER);
    assert_eq!(committed_index, LogIndex(2));
    assert_eq!(cluster.delivered_ack_floor(LEADER), LogIndex::ZERO);

    cluster.restart_node_lossy(LEADER);
    cluster.restart_node_lossy(LEADER);
    assert_eq!(cluster.commit_index(LEADER), committed_index);
    assert_eq!(
        cluster.committed_configuration_state(LEADER),
        committed_configuration,
    );
    assert_eq!(
        cluster.log_entries_from(LEADER, LogIndex(1)),
        committed_prefix,
        "lossy restart must not erase a node's locally committed prefix"
    );
}

#[test]
fn a_lossy_restart_replays_only_the_required_application_suffix() {
    let mut cluster = production_cluster();
    elect_node_one_with_pre_vote(&mut cluster);
    commit_payload(&mut cluster, b"before-restart");
    let restarted = NodeId(2);
    cluster.tick(LEADER);
    cluster.deliver_all();
    assert_eq!(
        applied_payloads(&cluster, restarted),
        vec![b"before-restart".to_vec()],
    );

    cluster.restart_node_lossy(restarted);
    cluster.restart_node_lossy(restarted);
    commit_payload(&mut cluster, b"after-restart");
    cluster.tick(LEADER);
    cluster.deliver_all();

    assert_eq!(
        applied_payloads(&cluster, restarted),
        vec![b"before-restart".to_vec(), b"after-restart".to_vec()],
        "durable application state must suppress duplicate replay without skipping new commits",
    );
    assert_eq!(
        cluster.local_applied_index(restarted),
        cluster.commit_index(restarted),
    );
}

#[test]
fn a_lossy_restart_confined_to_the_unacknowledged_tail_recovers_cleanly() {
    let mut cluster = production_cluster();
    elect_node_one_with_pre_vote(&mut cluster);
    commit_payload(&mut cluster, b"acknowledged");
    let acknowledged_floor = cluster.delivered_ack_floor(NodeId(2));
    assert!(acknowledged_floor >= LogIndex(1));

    // Two more entries reach follower 2, but its acknowledgements are
    // dropped: no leader ever counted them. The follower may still learn
    // that part of that suffix committed through node 3, so legal loss stops
    // at the max of the delivered ack floor and the local commit floor.
    for payload in [b"unsynced-1".as_slice(), b"unsynced-2".as_slice()] {
        cluster.propose(LEADER, payload.to_vec());
        // Explicit wave delivery: entries reach both followers, only node
        // 3's acknowledgement reaches the leader (committing the entry),
        // and everything else — node 2's acknowledgements and the commit
        // broadcast's follow-ups — is dropped before it can flow.
        let _ = cluster.deliver_matching(|envelope| envelope.to == NodeId(2));
        let _ = cluster.deliver_matching(|envelope| envelope.to == NodeId(3));
        let _ = cluster
            .deliver_matching(|envelope| envelope.to == LEADER && envelope.from == NodeId(3));
        let _ = cluster.drop_matching(|_| true);
    }
    assert_eq!(cluster.last_log_index(NodeId(2)), LogIndex(4));
    assert_eq!(cluster.delivered_ack_floor(NodeId(2)), acknowledged_floor);
    let legal_loss_floor = acknowledged_floor.max(cluster.commit_index(NodeId(2)));

    cluster.restart_node_lossy(NodeId(2));
    assert_eq!(
        cluster.last_log_index(NodeId(2)),
        legal_loss_floor,
        "only the unacknowledged and locally uncommitted tail is lost"
    );

    // The leader's match floor for node 2 sits at the acknowledged index,
    // so ordinary replication repairs the loss without friction.
    cluster.tick(LEADER);
    cluster.deliver_all();
    cluster.tick(LEADER);
    cluster.deliver_all();
    assert_eq!(cluster.last_log_index(NodeId(2)), LogIndex(4));
    assert_eq!(
        cluster.log_entries_from(NodeId(2), LogIndex(1)),
        cluster.log_entries_from(LEADER, LogIndex(1)),
    );
}

#[test]
fn a_marked_restart_that_loses_acknowledged_entries_pins_the_documented_amnesia() {
    let mut cluster = production_cluster();
    elect_node_one_with_pre_vote(&mut cluster);
    commit_payload(&mut cluster, b"first");
    cluster.mark_synced(NodeId(2));

    // Everything after the mark is fully acknowledged: the leader has
    // counted these entries, so losing them violates the durability
    // assumption the protocol runs on.
    commit_payload(&mut cluster, b"second");
    commit_payload(&mut cluster, b"third");
    assert_eq!(cluster.delivered_ack_floor(NodeId(2)), LogIndex(4));

    cluster.restart_node_from_mark(NodeId(2));
    assert_eq!(cluster.last_log_index(NodeId(2)), LogIndex(2));

    // Documented behavior: the leader's match floor never walks below an
    // acknowledgement, so the amnesiac follower's rejections are treated as
    // stale noise and it can never catch up until a future rejection hint can
    // prove its lower durable tail. The cluster itself stays safe and live
    // through the intact quorum.
    //
    // Delivery here is bounded rounds, never `deliver_all`: an amnesiac
    // follower's reject -> immediate re-probe exchange is an infinite
    // ping-pong under quiescence-driven delivery (each cycle costs a
    // round trip on a real network; synchronous delivery has no such
    // pacing).
    for _ in 0..6 {
        cluster.tick(LEADER);
        let _ = cluster.deliver_matching(|envelope| envelope.to == NodeId(2));
        let _ = cluster.deliver_matching(|envelope| envelope.to == LEADER);
        let _ = cluster.deliver_matching(|envelope| envelope.to == NodeId(3));
        let _ = cluster.deliver_matching(|envelope| envelope.to == LEADER);
        let _ = cluster.drop_matching(|_| true);
    }
    assert_eq!(
        cluster.last_log_index(NodeId(2)),
        LogIndex(2),
        "the amnesiac follower stays behind, by design, until rejection hints land"
    );
    let progress = cluster.leader_replication_progress(LEADER);
    let amnesiac = progress
        .iter()
        .find(|entry| entry.follower_id == NodeId(2))
        .expect("leader tracks the follower");
    assert!(amnesiac.next_index >= LogIndex(5));

    cluster.propose(LEADER, b"still-live".to_vec());
    let _ = cluster.deliver_matching(|envelope| envelope.to == NodeId(3));
    let _ = cluster.deliver_matching(|envelope| envelope.to == LEADER);
    let _ = cluster.drop_matching(|_| true);
    assert_eq!(cluster.commit_index(LEADER), LogIndex(5));
}
