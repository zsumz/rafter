//! Scenario coverage for the A2 fault primitives: sustained partitions that
//! hold across elections, lossy restarts in both their legal and
//! assumption-violating shapes, and typed wire-corruption injection.

use super::helpers::{config, direct_election_config};
use super::*;

const LEADER: NodeId = NodeId(1);

fn production_cluster() -> Cluster {
    Cluster::new(vec![
        config(1, &[2, 3], 3),
        config(2, &[1, 3], 9),
        config(3, &[1, 2], 9),
    ])
}

fn elect_node_one_with_pre_vote(cluster: &mut Cluster) {
    for _ in 0..6 {
        cluster.tick(LEADER);
        cluster.deliver_all();
        if cluster.role(LEADER) == Role::Leader {
            return;
        }
    }
    assert_eq!(cluster.role(LEADER), Role::Leader);
}

fn commit_payload(cluster: &mut Cluster, payload: &[u8]) {
    cluster.propose(LEADER, payload.to_vec());
    cluster.deliver_all();
}

fn applied_payloads(cluster: &Cluster, node_id: NodeId) -> Vec<Vec<u8>> {
    cluster
        .applied()
        .iter()
        .filter_map(|applied| {
            (applied.node_id == node_id).then_some(applied.payload.as_ref().to_vec())
        })
        .collect()
}

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

#[test]
fn corrupted_frames_never_panic_and_the_cluster_reconverges() {
    let mut cluster = production_cluster();
    elect_node_one_with_pre_vote(&mut cluster);
    commit_payload(&mut cluster, b"stable");

    // Queue a replication wave, then corrupt the in-flight appends with
    // field-level damage: inflated terms, scrambled prev coordinates, and
    // truncated batches. Codec frame checksums reject byte-level damage at
    // decode; this direct in-memory path still verifies the kernel absorbs
    // malformed fields without panicking, even though byzantine field values
    // can locally disrupt (a huge term forces step-downs: disruption, not
    // unsafety).
    cluster.propose(LEADER, b"during-corruption".to_vec());
    let corrupted = cluster.corrupt_queued_matching(
        |envelope| envelope.to == NodeId(2),
        |message| {
            if let rafter::Message::AppendEntries(request) = message {
                request.prev_log_index = LogIndex(7);
                request.prev_log_term = Term(9);
                request.entries = rafter::SharedEntries::default();
                request.leader_commit = LogIndex(42);
            }
        },
    );
    assert!(corrupted >= 1);
    cluster.deliver_all();

    // A corrupted vote-shaped frame with an absurd term: absorbed, no
    // panic; the legitimate leader re-establishes the cluster afterward.
    cluster.tick(LEADER);
    let _ = cluster.corrupt_queued_matching(
        |envelope| envelope.to == NodeId(3),
        |message| {
            if let rafter::Message::AppendEntries(request) = message {
                request.term = Term(request.term.0 + 1_000);
            }
        },
    );
    cluster.deliver_all();

    // Reconvergence: whatever local disruption the damage caused, the
    // cluster elects, commits, and agrees again. Tick two candidates so
    // stickiness hints lapse and someone can win.
    let mut rounds = 0;
    while cluster.leaders().is_empty() {
        rounds += 1;
        assert!(rounds <= 40, "the cluster re-elects within bounded rounds");
        cluster.tick(LEADER);
        cluster.tick(NodeId(2));
        cluster.deliver_all();
    }
    let leaders = cluster.leaders();
    let leader = *leaders.last().expect("a leader re-establishes");
    cluster.propose(leader, b"after-corruption".to_vec());
    cluster.deliver_all();
    let reference = cluster.log_entries_from(leader, LogIndex(1));
    assert!(reference
        .iter()
        .any(|entry| entry.application_payload() == Some(b"after-corruption")));
}
