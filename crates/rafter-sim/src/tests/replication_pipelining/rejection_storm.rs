use super::super::helpers::{
    config, deliver_append_entries, deliver_append_entries_response, pre_vote, pre_vote_response,
    request_vote,
};
use super::super::*;
use super::fixtures::{
    bootstrap_state, deliver_ready_generation, follower_progress, ready_message_count,
    vote_response, BEHIND_FOLLOWER, CAUGHT_UP_FOLLOWER, LEADER,
};
use rafter::{Message, ReplicationState};

#[test]
fn duplicated_rejection_storm_collapses_to_probe_and_converges_bounded() {
    let mut cluster = storm_cluster_with_divergent_follower();
    elect_leader_against_divergent_follower(&mut cluster);
    let leader_last = cluster.last_log_index(LEADER);

    let mut rounds = 0;
    let mut appends_delivered = 0;
    let mut stale_rejection_armed = false;
    let mut probing_after_match = false;
    duplicate_storm_traffic(&mut cluster);
    loop {
        rounds += 1;
        assert!(rounds <= 40, "rejection storm must converge, not livelock");
        if ready_message_count(&cluster) == 0 {
            cluster.tick(LEADER);
        } else {
            appends_delivered += ready_appends_to_behind_follower(&cluster);
            deliver_ready_generation(&mut cluster);
        }
        duplicate_storm_traffic(&mut cluster);
        if !stale_rejection_armed {
            stale_rejection_armed = delay_one_rejection_into_staleness(&mut cluster);
        }

        let progress = follower_progress(&cluster, BEHIND_FOLLOWER);
        assert!(
            progress.next_index >= progress.match_index.next(),
            "round {rounds}: next_index {} rewound below match_index {} + 1",
            progress.next_index,
            progress.match_index
        );
        if progress.match_index == leader_last {
            probing_after_match |= progress.state == ReplicationState::Probing;
            if cluster.pending().count() == 0 {
                break;
            }
        }
    }

    // The follower converged on the leader's log in a pinned, bounded
    // number of rounds despite every append and rejection arriving twice.
    assert_eq!(rounds, 15);
    assert_eq!(
        cluster.log_entries_from(BEHIND_FOLLOWER, LogIndex(1)),
        cluster.log_entries_from(LEADER, LogIndex(1))
    );

    // Duplication doubles traffic per hop but the match floor caps the
    // walk-back, so total leader→follower messages stay far from an
    // exponential blowup. The exact count is deterministic.
    assert_eq!(appends_delivered, 44);
    assert!(
        appends_delivered <= 64,
        "storm delivered {appends_delivered} leader appends, above the generous bound"
    );

    // The stale duplicated rejection landed after the follower's position
    // was confirmed: the leader collapsed back to Probing without rewinding
    // next_index below match + 1, then resumed Replicating.
    assert!(stale_rejection_armed);
    assert!(probing_after_match);
    let progress = follower_progress(&cluster, BEHIND_FOLLOWER);
    assert_eq!(progress.state, ReplicationState::Replicating);
    assert_eq!(progress.match_index, leader_last);
    assert_eq!(progress.next_index, leader_last.next());
}

/// Nodes 1 and 3 share a committed-quality log; node 2 diverges after the
/// shared prefix with an uncommitted suffix from an older term.
fn storm_cluster_with_divergent_follower() -> Cluster {
    let mut cluster = Cluster::new(vec![
        config(1, &[2, 3], 3).with_max_inflight_appends(8),
        config(2, &[1, 3], 9).with_max_inflight_appends(8),
        config(3, &[1, 2], 9).with_max_inflight_appends(8),
    ]);
    let leader_log: [(u64, Term, &[u8]); 6] = [
        (1, Term(1), b"shared-prefix"),
        (2, Term(3), b"leader-suffix-2"),
        (3, Term(3), b"leader-suffix-3"),
        (4, Term(3), b"leader-suffix-4"),
        (5, Term(3), b"leader-suffix-5"),
        (6, Term(3), b"leader-suffix-6"),
    ];
    let divergent_log: [(u64, Term, &[u8]); 4] = [
        (1, Term(1), b"shared-prefix"),
        (2, Term(2), b"divergent-2"),
        (3, Term(2), b"divergent-3"),
        (4, Term(2), b"divergent-4"),
    ];
    cluster
        .restart_node_from_bootstrap(LEADER, bootstrap_state(Term(3), &leader_log))
        .expect("leader bootstrap is valid");
    cluster
        .restart_node_from_bootstrap(BEHIND_FOLLOWER, bootstrap_state(Term(3), &divergent_log))
        .expect("divergent follower bootstrap is valid");
    cluster
        .restart_node_from_bootstrap(CAUGHT_UP_FOLLOWER, bootstrap_state(Term(3), &leader_log))
        .expect("voter bootstrap is valid");
    cluster
}

/// Elects node 1 with node 3's vote and settles their replication; the only
/// message left in flight is the probe append to the divergent follower.
fn elect_leader_against_divergent_follower(cluster: &mut Cluster) {
    for _ in 0..3 {
        cluster.tick(LEADER);
    }
    assert_eq!(cluster.drop_matching(pre_vote(LEADER, BEHIND_FOLLOWER)), 1);
    assert_eq!(
        cluster.deliver_matching(pre_vote(LEADER, CAUGHT_UP_FOLLOWER)),
        1
    );
    assert_eq!(
        cluster.deliver_matching(pre_vote_response(CAUGHT_UP_FOLLOWER, LEADER)),
        1
    );
    assert_eq!(
        cluster.drop_matching(request_vote(LEADER, BEHIND_FOLLOWER)),
        1
    );
    assert_eq!(
        cluster.deliver_matching(request_vote(LEADER, CAUGHT_UP_FOLLOWER)),
        1
    );
    assert_eq!(
        cluster.deliver_matching(vote_response(CAUGHT_UP_FOLLOWER, LEADER)),
        1
    );
    assert_eq!(cluster.role(LEADER), Role::Leader);
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries(LEADER, CAUGHT_UP_FOLLOWER)),
        1
    );
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries_response(CAUGHT_UP_FOLLOWER, LEADER)),
        1
    );
    assert_eq!(cluster.pending().count(), 1);
}

fn leader_append_to_behind_follower(envelope: &Envelope) -> bool {
    envelope.from == LEADER
        && envelope.to == BEHIND_FOLLOWER
        && matches!(envelope.message, Message::AppendEntries(_))
}

fn rejection_from_behind_follower(envelope: &Envelope) -> bool {
    envelope.from == BEHIND_FOLLOWER
        && envelope.to == LEADER
        && matches!(
            &envelope.message,
            Message::AppendEntriesResponse(response) if !response.success
        )
}

/// Duplicates every deliverable leader→follower append and follower→leader
/// rejection currently on the wire: the at-least-once storm.
fn duplicate_storm_traffic(cluster: &mut Cluster) {
    cluster.duplicate_matching(leader_append_to_behind_follower);
    cluster.duplicate_matching(rejection_from_behind_follower);
}

/// Holds back one queued rejection by two ticks so it arrives stale — after
/// the follower's log position has already been confirmed.
fn delay_one_rejection_into_staleness(cluster: &mut Cluster) -> bool {
    let mut delayed = false;
    cluster.delay_matching(
        |envelope| {
            if delayed || !rejection_from_behind_follower(envelope) {
                return false;
            }
            delayed = true;
            true
        },
        2,
    );
    delayed
}

fn ready_appends_to_behind_follower(cluster: &Cluster) -> usize {
    let now = cluster.clock().now();
    cluster
        .network
        .iter()
        .filter(|queued| {
            queued.ready_at <= now && leader_append_to_behind_follower(&queued.envelope)
        })
        .count()
}
