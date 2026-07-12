//! Replication-window count, byte-budget, acknowledgement, and rejection behavior.

use super::support::*;
use super::*;

#[test]
fn replicate_mode_fill_is_bounded_by_the_batch_window() {
    let mut leader = pipelining_leader(12, |config| config.with_max_inflight_appends(4));
    seed_replicating(&mut leader, NodeId(2), LogIndex::ZERO);

    let outputs = leader.step(Input::Tick);

    let batches = appends_to(&outputs, NodeId(2));
    assert_eq!(batches.len(), 4, "one step fills exactly the batch window");
    for (offset, batch) in batches.iter().enumerate() {
        let offset = offset as u64;
        assert_eq!(batch.prev_log_index, LogIndex(offset));
        assert_eq!(
            batch.entries.len(),
            1,
            "the byte budget bounds every batch to one entry"
        );
        assert_eq!(
            batch.entries[0].application_payload(),
            Some(payload(offset + 1).as_slice())
        );
    }
    let progress = follower_progress(&leader, NodeId(2));
    assert_eq!(
        progress.next_index,
        LogIndex(5),
        "next_index advances optimistically past every sent batch"
    );
    assert_eq!(progress.inflights.batch_count(), 4);
    assert_eq!(progress.inflights.byte_count(), 4 * one_entry_batch_bytes());

    let outputs = leader.step(Input::Tick);
    let heartbeats = appends_to(&outputs, NodeId(2));
    assert_eq!(
        heartbeats.len(),
        1,
        "a full window degrades a broadcast round to a single message"
    );
    assert!(
        heartbeats[0].entries.is_empty(),
        "a full window admits empty heartbeats only"
    );
    assert_eq!(
        follower_progress(&leader, NodeId(2))
            .inflights
            .batch_count(),
        4
    );
}
#[test]
fn replicate_mode_fill_stops_when_the_pending_suffix_runs_out() {
    let mut leader = pipelining_leader(3, |config| config.with_max_inflight_appends(8));
    seed_replicating(&mut leader, NodeId(2), LogIndex::ZERO);

    let outputs = leader.step(Input::Tick);

    let batches = appends_to(&outputs, NodeId(2));
    assert_eq!(
        batches.len(),
        3,
        "the fill sends the available batches, not the whole window"
    );
    let progress = follower_progress(&leader, NodeId(2));
    assert_eq!(progress.next_index, LogIndex(5));
    assert_eq!(progress.inflights.batch_count(), 3);
}
#[test]
fn window_fill_stops_at_the_inflight_byte_budget_before_the_batch_count() {
    let two_batches_of_bytes = 2 * one_entry_batch_bytes();
    let mut leader = pipelining_leader(12, move |config| {
        config
            .with_max_inflight_appends(8)
            .with_max_inflight_bytes(two_batches_of_bytes)
    });
    seed_replicating(&mut leader, NodeId(2), LogIndex::ZERO);

    let outputs = leader.step(Input::Tick);

    let batches = appends_to(&outputs, NodeId(2));
    assert_eq!(
        batches.len(),
        2,
        "the byte budget closes the window before the batch count does"
    );
    let progress = follower_progress(&leader, NodeId(2));
    assert_eq!(progress.next_index, LogIndex(3));
    assert_eq!(progress.inflights.batch_count(), 2);
    assert_eq!(progress.inflights.byte_count(), two_batches_of_bytes);
}
#[test]
fn one_batch_is_admissible_into_an_empty_window_regardless_of_the_byte_budget() {
    let mut leader = pipelining_leader(2, |config| config.with_max_inflight_bytes(1));
    seed_replicating(&mut leader, NodeId(2), LogIndex::ZERO);

    let outputs = leader.step(Input::Tick);

    let batches = appends_to(&outputs, NodeId(2));
    assert_eq!(
        batches.len(),
        1,
        "an empty window always admits one batch, or an oversized batch could never ship"
    );
    assert_eq!(batches[0].entries.len(), 1);
    assert_eq!(
        follower_progress(&leader, NodeId(2))
            .inflights
            .batch_count(),
        1
    );

    let outputs = leader.step(Input::Tick);
    let heartbeats = appends_to(&outputs, NodeId(2));
    assert_eq!(heartbeats.len(), 1);
    assert!(
        heartbeats[0].entries.is_empty(),
        "the admitted batch saturates the byte budget until it is acknowledged"
    );
}
#[test]
fn a_zero_batch_window_behaves_as_one() {
    let mut leader = pipelining_leader(3, |config| config.with_max_inflight_appends(0));
    seed_replicating(&mut leader, NodeId(2), LogIndex::ZERO);

    let outputs = leader.step(Input::Tick);

    let batches = appends_to(&outputs, NodeId(2));
    assert_eq!(
        batches.len(),
        1,
        "a zero window still pipelines one append, or replication would stall"
    );
    assert_eq!(
        follower_progress(&leader, NodeId(2))
            .inflights
            .batch_count(),
        1
    );
}
#[test]
fn a_success_ack_frees_the_head_slot_and_pulls_the_next_batch_in_the_same_step() {
    let mut leader = pipelining_leader(12, |config| config.with_max_inflight_appends(2));
    seed_replicating(&mut leader, NodeId(2), LogIndex::ZERO);
    // Fill the window with entries 1 and 2.
    let _ = leader.step(Input::Tick);
    assert_eq!(
        follower_progress(&leader, NodeId(2))
            .inflights
            .batch_count(),
        2
    );

    let outputs = deliver_append_response(&mut leader, NodeId(2), true, LogIndex(1));

    let pulled = appends_to(&outputs, NodeId(2));
    assert_eq!(
        pulled.len(),
        1,
        "the freed head slot pulls the next batch without waiting for a tick"
    );
    assert_eq!(pulled[0].prev_log_index, LogIndex(2));
    assert_eq!(
        pulled[0].entries[0].application_payload(),
        Some(payload(3).as_slice())
    );
    assert_eq!(
        outputs.len(),
        1,
        "catch-up is acknowledgement-paced: the pull is the step's only output"
    );
    let progress = follower_progress(&leader, NodeId(2));
    assert_eq!(progress.match_index, LogIndex(1));
    assert_eq!(progress.next_index, LogIndex(4));
    assert_eq!(
        progress.inflights.batch_count(),
        2,
        "the window is full again: entries 2 and 3 are in flight"
    );
}
#[test]
fn a_rejection_collapses_the_window_and_walks_back_no_further_than_the_match_floor() {
    let mut leader = pipelining_leader(12, |config| config.with_max_inflight_appends(4));
    seed_replicating(&mut leader, NodeId(2), LogIndex(4));
    // Fill the window with entries 5 through 8; next_index runs ahead to 9.
    let _ = leader.step(Input::Tick);
    assert_eq!(
        follower_progress(&leader, NodeId(2))
            .inflights
            .batch_count(),
        4
    );
    assert_eq!(
        follower_progress(&leader, NodeId(2)).next_index,
        LogIndex(9)
    );

    let outputs = deliver_append_response(&mut leader, NodeId(2), false, LogIndex::ZERO);

    let probes = appends_to(&outputs, NodeId(2));
    assert_eq!(
        probes.len(),
        1,
        "the collapse emits exactly one immediate re-probe"
    );
    assert_eq!(
        probes[0].prev_log_index,
        LogIndex(7),
        "next_index walked back one step from the optimistic tip"
    );
    assert_eq!(
        probes[0].entries.len(),
        1,
        "the re-probe carries one bounded batch"
    );
    let progress = follower_progress(&leader, NodeId(2));
    assert_eq!(
        progress.mode,
        ProgressMode::Probe {
            awaiting_response: true
        }
    );
    assert_eq!(
        progress.inflights.batch_count(),
        0,
        "the in-flight window is forfeited on rejection"
    );
    assert_eq!(progress.next_index, LogIndex(8));

    // Each further rejection walks next_index back one step...
    for expected_next in [7, 6, 5] {
        let _ = deliver_append_response(&mut leader, NodeId(2), false, LogIndex::ZERO);
        assert_eq!(
            follower_progress(&leader, NodeId(2)).next_index,
            LogIndex(expected_next)
        );
    }

    // ...but a stale rejection contradicting the earlier acknowledgement of
    // entry 4 cannot rewind below the match floor.
    let outputs = deliver_append_response(&mut leader, NodeId(2), false, LogIndex::ZERO);
    assert_eq!(
        follower_progress(&leader, NodeId(2)).next_index,
        LogIndex(5),
        "next_index holds at match_index + 1"
    );
    let probes = appends_to(&outputs, NodeId(2));
    assert_eq!(probes.len(), 1);
    assert_eq!(
        probes[0].prev_log_index,
        LogIndex(4),
        "the re-probe restarts just above the acknowledged match"
    );
}
