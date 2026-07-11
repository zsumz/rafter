use super::super::helpers::{config, pre_vote, pre_vote_response, request_vote};
use super::super::*;
use super::fixtures::{
    deliver_ready_generation, follower_progress, ready_message_count, vote_response,
    BEHIND_FOLLOWER, CAUGHT_UP_FOLLOWER, LEADER,
};
use rafter::{NodeConfig, ReplicationState};

/// Append batches in the committed suffix the behind follower must fetch:
/// the batch budget below admits exactly one catch-up entry per append.
const SUFFIX_BATCHES: u64 = 12;
const SUFFIX_LOG_ENTRIES: u64 = SUFFIX_BATCHES + 1;
const CATCH_UP_PAYLOAD_LEN: usize = 100;

#[test]
fn windowed_catch_up_streams_committed_suffix_in_constant_rounds() {
    // A window comfortably above the batch count: the whole suffix fits in
    // one in-flight burst.
    let mut cluster = catch_up_cluster(16);
    elect_and_commit_suffix_away_from_behind_follower(&mut cluster);

    let outcome = drive_catch_up_rounds(&mut cluster);

    // Deterministic pipelined accounting: the eager leadership no-op probe,
    // its acknowledgement releasing the whole windowed burst, the burst hop,
    // and the acknowledgement hop confirming every batch.
    assert_eq!(outcome.rounds, 5);
    assert!(
        outcome.rounds <= serialized_round_floor().div_ceil(2),
        "windowed catch-up took {} rounds, not a fraction of the serialized floor {}",
        outcome.rounds,
        serialized_round_floor()
    );

    // Probing flipped to Replicating as the catch-up burst was sent, with
    // the whole suffix already in flight while nothing was confirmed:
    // next_index runs ahead of match_index by the full window.
    assert_eq!(outcome.replicating_since_round, 3);
    assert_eq!(outcome.match_index_at_transition, LogIndex::ZERO);
    assert_eq!(
        outcome.next_index_at_transition,
        LogIndex(SUFFIX_LOG_ENTRIES + 1)
    );

    assert_catch_up_converged(&cluster);
}

#[test]
fn inflight_window_of_one_reproduces_serialized_ack_paced_catch_up() {
    let mut cluster = catch_up_cluster(1);
    elect_and_commit_suffix_away_from_behind_follower(&mut cluster);

    let outcome = drive_catch_up_rounds(&mut cluster);

    // The serialized baseline is real: with one unacknowledged batch
    // allowed, every batch costs a full acknowledgement round trip.
    assert!(
        outcome.rounds >= serialized_round_floor(),
        "window=1 catch-up took {} rounds, below the serialized floor {}",
        outcome.rounds,
        serialized_round_floor()
    );
    // Exact accounting: one leader tick plus the probe round trip, then two
    // rounds (batch hop, acknowledgement hop) per suffix batch.
    assert_eq!(outcome.rounds, 3 + 2 * serialized_round_floor());

    // Replication still becomes visible as the first catch-up batch is sent,
    // but the window admits a single batch: next_index leads match_index by
    // exactly one batch instead of the whole suffix.
    assert_eq!(outcome.replicating_since_round, 3);
    assert_eq!(outcome.match_index_at_transition, LogIndex::ZERO);
    assert_eq!(outcome.next_index_at_transition, LogIndex(2));

    assert_catch_up_converged(&cluster);
}

struct CatchUpOutcome {
    rounds: usize,
    replicating_since_round: usize,
    match_index_at_transition: LogIndex,
    next_index_at_transition: LogIndex,
}

/// The analytic serialized baseline: confirming one batch per
/// acknowledgement round trip can never finish `SUFFIX_LOG_ENTRIES` batches
/// in fewer than `SUFFIX_LOG_ENTRIES` rounds, before counting the probe hops.
fn serialized_round_floor() -> usize {
    usize::try_from(SUFFIX_LOG_ENTRIES).expect("suffix batch count fits in usize")
}

fn catch_up_payload(index: u64) -> Vec<u8> {
    let mut payload = format!("catch-up-{index:02}").into_bytes();
    payload.resize(CATCH_UP_PAYLOAD_LEN, b'.');
    payload
}

/// A batch budget sized to exactly one catch-up entry, so the committed
/// suffix spans a deterministic `SUFFIX_BATCHES` single-entry batches.
fn single_entry_batch_budget() -> usize {
    LogEntry::application(Term(1), catch_up_payload(1)).replication_bytes()
}

fn catch_up_cluster(max_inflight_appends: usize) -> Cluster {
    Cluster::new(vec![
        catch_up_config(1, &[2, 3], 3, max_inflight_appends),
        catch_up_config(2, &[1, 3], 9, max_inflight_appends),
        catch_up_config(3, &[1, 2], 9, max_inflight_appends),
    ])
}

/// The byte window admits the whole suffix, so the batch-count window is
/// the binding flow-control limit under test.
fn catch_up_config(
    id: u64,
    peers: &[u64],
    election_timeout_ticks: u64,
    max_inflight_appends: usize,
) -> NodeConfig {
    let budget = single_entry_batch_budget();
    config(id, peers, election_timeout_ticks)
        .with_max_append_entries_bytes(budget)
        .with_max_inflight_appends(max_inflight_appends)
        .with_max_inflight_bytes(budget * serialized_round_floor())
}

/// Elects node 1 while the behind follower is unreachable, commits
/// `SUFFIX_BATCHES` entries through node 3, then restarts the behind
/// follower as the fresh, empty process it still is. Ends with the network
/// idle and the leader probing the behind follower at `next_index` 1.
fn elect_and_commit_suffix_away_from_behind_follower(cluster: &mut Cluster) {
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

    for index in 1..=SUFFIX_BATCHES {
        cluster.propose(LEADER, catch_up_payload(index));
    }
    loop {
        let dropped = cluster.drop_matching(|envelope| envelope.to == BEHIND_FOLLOWER);
        let delivered = cluster.deliver_matching(|envelope| envelope.to != BEHIND_FOLLOWER);
        if dropped == 0 && delivered == 0 {
            break;
        }
    }
    assert_eq!(cluster.commit_index(LEADER), LogIndex(SUFFIX_LOG_ENTRIES));
    assert_eq!(cluster.last_log_index(LEADER), LogIndex(SUFFIX_LOG_ENTRIES));
    assert_eq!(cluster.pending().count(), 0);

    // Nothing ever reached the behind follower, so its restart hydrates the
    // empty state of a fresh process.
    let bootstrap = cluster.bootstrap_state(BEHIND_FOLLOWER);
    assert!(bootstrap.log.is_empty());
    cluster
        .restart_node_from_bootstrap(BEHIND_FOLLOWER, bootstrap)
        .expect("fresh follower bootstrap is valid");

    let probing = follower_progress(cluster, BEHIND_FOLLOWER);
    assert_eq!(probing.state, ReplicationState::Probing);
    assert_eq!(probing.match_index, LogIndex::ZERO);
    assert_eq!(probing.next_index, LogIndex(1));
}

/// Drives explicit rounds until the leader records the behind follower's
/// `match_index` at its own last index, returning the round count and the
/// progress observed when the leader first reported Replicating.
fn drive_catch_up_rounds(cluster: &mut Cluster) -> CatchUpOutcome {
    let target = cluster.last_log_index(LEADER);
    let mut rounds = 0;
    let mut transition = None;
    loop {
        rounds += 1;
        assert!(rounds <= 64, "catch-up must converge, not livelock");
        if ready_message_count(cluster) == 0 {
            cluster.tick(LEADER);
        } else {
            deliver_ready_generation(cluster);
        }

        let progress = follower_progress(cluster, BEHIND_FOLLOWER);
        if transition.is_none() && progress.state == ReplicationState::Replicating {
            transition = Some((rounds, progress.match_index, progress.next_index));
        }
        if progress.match_index == target {
            let (replicating_since_round, match_index_at_transition, next_index_at_transition) =
                transition.expect("catch-up must pass through Replicating");
            return CatchUpOutcome {
                rounds,
                replicating_since_round,
                match_index_at_transition,
                next_index_at_transition,
            };
        }
    }
}

fn assert_catch_up_converged(cluster: &Cluster) {
    assert_eq!(
        cluster.log_entries_from(BEHIND_FOLLOWER, LogIndex(1)),
        cluster.log_entries_from(LEADER, LogIndex(1))
    );
    assert_eq!(
        cluster.commit_index(BEHIND_FOLLOWER),
        LogIndex(SUFFIX_LOG_ENTRIES)
    );
    let progress = follower_progress(cluster, BEHIND_FOLLOWER);
    assert_eq!(progress.state, ReplicationState::Replicating);
    assert_eq!(progress.match_index, LogIndex(SUFFIX_LOG_ENTRIES));
}
