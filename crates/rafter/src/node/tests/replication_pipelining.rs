//! Kernel tests for the C2 replication rework: the Progress/Inflights send
//! discipline — probe once, fill the in-flight window, degrade to empty
//! heartbeats, collapse on rejection, pause for snapshots.

use super::super::state::{Inflights, Progress, ProgressMode};
use super::super::*;
use super::helpers::bootstrap_entry;
use super::replication_snapshot_support::test_snapshot;
use crate::{AppendEntries, AppendEntriesResponse, InstallSnapshotResponse, LogEntry};

/// Every pipelining test replicates 100-byte payloads under a 180-byte batch
/// budget: one application entry costs 164 replication bytes, so each batch
/// carries exactly one entry and window arithmetic is exact.
const PAYLOAD_BYTES: usize = 100;
const ONE_ENTRY_BATCH_BUDGET: usize = 180;

fn payload(index: u64) -> Vec<u8> {
    let byte = u8::try_from(index).expect("test entry indexes fit into a payload byte");
    vec![byte; PAYLOAD_BYTES]
}

fn one_entry_batch_bytes() -> usize {
    LogEntry::application(Term(1), payload(1)).replication_bytes()
}

/// A term-2 leader over voters {1, 2, 3} whose log carries `entry_count`
/// term-1 entries of one batch each. Prior-term entries keep commit
/// advancement quiescent (thesis 3.6.2), so every append in a step is
/// attributable to the send discipline under test, never to a
/// commit-advance broadcast.
fn pipelining_leader(entry_count: u64, configure: impl FnOnce(NodeConfig) -> NodeConfig) -> Node {
    let log = (1..=entry_count)
        .map(|index| bootstrap_entry(index, 1, &payload(index)))
        .collect();
    let mut leader = Node::from_bootstrap(
        configure(
            NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
                .expect("test Raft node config is valid")
                .with_max_append_entries_bytes(ONE_ENTRY_BATCH_BUDGET),
        ),
        BootstrapState {
            current_term: Term(2),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: None,
            log,
        },
    )
    .expect("leader bootstraps from a prior-term log");
    leader.become_leader();
    leader
}

/// Puts `follower` in confirmed Replicate mode with everything through
/// `match_index` acknowledged and a clean window — the state a successful
/// probe acknowledgement leaves behind.
fn seed_replicating(leader: &mut Node, follower: NodeId, match_index: LogIndex) {
    *leader
        .try_follower_progress_mut(follower)
        .expect("active follower") = Progress {
        match_index,
        next_index: match_index.next(),
        mode: ProgressMode::Replicate,
        inflights: Inflights::default(),
    };
}

fn follower_progress(leader: &Node, follower: NodeId) -> &Progress {
    leader
        .leader
        .progress
        .get(follower)
        .expect("active follower")
}

fn replication_state(leader: &Node, follower: NodeId) -> ReplicationState {
    leader
        .leader_replication_progress()
        .into_iter()
        .find(|progress| progress.follower_id == follower)
        .expect("the leader reports every follower")
        .state
}

fn appends_to(outputs: &[Output], to: NodeId) -> Vec<&AppendEntries> {
    outputs
        .iter()
        .filter_map(|output| {
            let Output::Send {
                to: actual_to,
                message: Message::AppendEntries(request),
            } = output
            else {
                return None;
            };
            (*actual_to == to).then_some(request)
        })
        .collect()
}

fn snapshot_chunks_to(outputs: &[Output], to: NodeId) -> Vec<&crate::SnapshotChunkSend> {
    outputs
        .iter()
        .filter_map(|output| {
            let Output::SendSnapshotChunk {
                to: actual_to,
                chunk,
            } = output
            else {
                return None;
            };
            (*actual_to == to).then_some(chunk)
        })
        .collect()
}

fn deliver_append_response(
    leader: &mut Node,
    follower: NodeId,
    success: bool,
    match_index: LogIndex,
) -> Vec<Output> {
    leader.step(Input::Message {
        from: follower,
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id: follower,
            success,
            match_index,
        }),
    })
}

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

#[test]
fn probe_mode_sends_one_bounded_probe_then_empty_heartbeats_until_the_ack() {
    let mut leader = pipelining_leader(3, |config| config);
    // Follower 2 collapsed to probing from the log start.
    *leader
        .try_follower_progress_mut(NodeId(2))
        .expect("active follower") = Progress::probing(LogIndex(1));

    let outputs = leader.step(Input::Tick);
    let probes = appends_to(&outputs, NodeId(2));
    assert_eq!(probes.len(), 1);
    assert_eq!(probes[0].prev_log_index, LogIndex::ZERO);
    assert_eq!(
        probes[0].entries.len(),
        1,
        "the probe carries one bounded batch, not the window"
    );
    assert_eq!(
        replication_state(&leader, NodeId(2)),
        ReplicationState::Probing
    );

    // Follower 3 stays responsive, so check-quorum keeps the leader in
    // place while follower 2's probe goes unanswered.
    let _ = deliver_append_response(&mut leader, NodeId(3), true, LogIndex(4));

    // Unanswered, further broadcasts send empty heartbeats only; the probe
    // is not repeated and next_index never moves.
    for _ in 0..2 {
        let outputs = leader.step(Input::Tick);
        let heartbeats = appends_to(&outputs, NodeId(2));
        assert_eq!(heartbeats.len(), 1);
        assert!(
            heartbeats[0].entries.is_empty(),
            "an awaiting probe degrades broadcasts to empty heartbeats"
        );
    }
    let progress = follower_progress(&leader, NodeId(2));
    assert_eq!(
        progress.next_index,
        LogIndex(1),
        "probing never advances next_index optimistically"
    );
    assert_eq!(progress.inflights.batch_count(), 0);

    // The probe's success acknowledgement flips the follower to Replicate
    // and fills the window with the remaining suffix in the same step.
    let outputs = deliver_append_response(&mut leader, NodeId(2), true, LogIndex(1));
    let filled = appends_to(&outputs, NodeId(2));
    assert_eq!(filled.len(), 2);
    assert_eq!(filled[0].prev_log_index, LogIndex(1));
    assert_eq!(filled[1].prev_log_index, LogIndex(2));
    assert_eq!(
        replication_state(&leader, NodeId(2)),
        ReplicationState::Replicating
    );
    let progress = follower_progress(&leader, NodeId(2));
    assert_eq!(progress.next_index, LogIndex(5));
    assert_eq!(progress.inflights.batch_count(), 2);
}

#[test]
fn snapshot_mode_pauses_pipelining_and_resumes_with_a_window_fill_after_installation() {
    let snapshot_payload = b"pipelining snapshot";
    let snapshot = test_snapshot(3, 4, 5, snapshot_payload);
    let mut leader = Node::from_bootstrap(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
            .expect("test Raft node config is valid")
            .with_max_append_entries_bytes(ONE_ENTRY_BATCH_BUDGET),
        BootstrapState {
            current_term: Term(5),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: Some(snapshot),
            log: vec![
                bootstrap_entry(4, 5, &payload(4)),
                bootstrap_entry(5, 5, &payload(5)),
                bootstrap_entry(6, 5, &payload(6)),
            ],
        },
    )
    .expect("leader hydrates from snapshot");
    leader.become_leader();
    // Follower 2's send position lies behind the compacted prefix.
    leader
        .try_follower_progress_mut(NodeId(2))
        .expect("active follower")
        .next_index = LogIndex(2);

    // The broadcast notices the follower needs the snapshot, not the log:
    // chunks flow and append pipelining pauses entirely.
    for _ in 0..2 {
        let outputs = leader.step(Input::Tick);
        assert!(
            appends_to(&outputs, NodeId(2)).is_empty(),
            "no AppendEntries while the snapshot streams"
        );
        let chunks = snapshot_chunks_to(&outputs, NodeId(2));
        assert_eq!(chunks.len(), 1);
        assert_eq!(
            chunks[0].offset, 0,
            "the cursor chunk is re-sent until acknowledged"
        );
        assert_eq!(
            replication_state(&leader, NodeId(2)),
            ReplicationState::Snapshotting { next_offset: 0 }
        );
    }
    assert_eq!(
        follower_progress(&leader, NodeId(2))
            .inflights
            .batch_count(),
        0
    );

    // The installation acknowledgement confirms the boundary: the mode
    // returns to Replicate and the suffix fills the window in the same step.
    let transfer_id = leader.snapshot().expect("snapshot is held").transfer_id();
    let outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::InstallSnapshotResponse(InstallSnapshotResponse {
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            last_included_index: LogIndex(3),
            transfer_id: Some(transfer_id),
            next_offset: snapshot_payload.len() as u64,
        }),
    });

    let filled = appends_to(&outputs, NodeId(2));
    assert_eq!(
        filled.len(),
        3,
        "the installed boundary confirms the position: the suffix window-fills at once"
    );
    assert_eq!(filled[0].prev_log_index, LogIndex(3));
    assert_eq!(filled[1].prev_log_index, LogIndex(4));
    assert_eq!(filled[2].prev_log_index, LogIndex(5));
    assert_eq!(
        replication_state(&leader, NodeId(2)),
        ReplicationState::Replicating
    );
    let progress = follower_progress(&leader, NodeId(2));
    assert_eq!(progress.match_index, LogIndex(3));
    assert_eq!(progress.next_index, LogIndex(8));
    assert_eq!(progress.inflights.batch_count(), 3);
}

#[test]
fn snapshot_peer_does_not_break_shared_append_fanout_to_log_peers() {
    let snapshot = test_snapshot(3, 4, 5, b"mixed-mode snapshot");
    let mut leader = Node::from_bootstrap(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3), NodeId(4)], 3)
            .expect("test Raft node config is valid"),
        BootstrapState {
            current_term: Term(5),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: Some(snapshot),
            log: vec![bootstrap_entry(4, 5, &payload(4))],
        },
    )
    .expect("leader hydrates from snapshot");
    leader.become_leader();
    seed_replicating(&mut leader, NodeId(2), LogIndex(3));
    seed_replicating(&mut leader, NodeId(3), LogIndex(3));
    leader
        .try_follower_progress_mut(NodeId(4))
        .expect("active follower")
        .next_index = LogIndex(2);

    let outputs = leader.step(Input::Tick);

    assert!(
        appends_to(&outputs, NodeId(4)).is_empty(),
        "a compacted follower receives snapshot chunks, not cached log batches"
    );
    assert_eq!(snapshot_chunks_to(&outputs, NodeId(4)).len(), 1);
    let follower_two_entries = appends_to(&outputs, NodeId(2))
        .first()
        .expect("follower 2 receives the retained suffix")
        .entries
        .clone();
    let follower_three_entries = appends_to(&outputs, NodeId(3))
        .first()
        .expect("follower 3 receives the retained suffix")
        .entries
        .clone();
    assert!(!follower_two_entries.is_empty());
    assert!(
        follower_two_entries.shares_allocation(&follower_three_entries),
        "snapshot-mode peers must not prevent log peers from sharing one suffix batch"
    );
}

#[test]
fn leader_replication_progress_reports_the_state_of_every_mode() {
    let snapshot = test_snapshot(3, 4, 5, b"observability snapshot");
    let mut leader = Node::from_bootstrap(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3), NodeId(4)], 3)
            .expect("test Raft node config is valid"),
        BootstrapState {
            current_term: Term(5),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: Some(snapshot),
            log: vec![bootstrap_entry(4, 5, &payload(4))],
        },
    )
    .expect("leader hydrates from snapshot");
    leader.become_leader();

    // Follower 2 keeps the fresh-leadership probe; follower 3 has confirmed
    // its position; follower 4 is mid-snapshot.
    seed_replicating(&mut leader, NodeId(3), LogIndex(4));
    let behind = leader
        .try_follower_progress_mut(NodeId(4))
        .expect("active follower");
    behind.next_index = LogIndex(3);
    behind.mode = ProgressMode::Snapshot { next_offset: 7 };

    assert_eq!(
        leader.leader_replication_progress(),
        vec![
            ReplicationProgress {
                follower_id: NodeId(2),
                match_index: LogIndex::ZERO,
                next_index: LogIndex(5),
                state: ReplicationState::Probing,
            },
            ReplicationProgress {
                follower_id: NodeId(3),
                match_index: LogIndex(4),
                next_index: LogIndex(5),
                state: ReplicationState::Replicating,
            },
            ReplicationProgress {
                follower_id: NodeId(4),
                match_index: LogIndex::ZERO,
                next_index: LogIndex(3),
                state: ReplicationState::Snapshotting { next_offset: 7 },
            },
        ]
    );
}

/// C3's zero-copy claim, asserted by allocation identity rather than
/// assumed: batching a suffix and fanning it out to every follower shares
/// the append-entry slice and each entry's payload allocation. The clone in
/// batch fan-out bumps a reference count; it never copies content.
#[test]
fn fan_out_shares_the_log_payload_allocation_across_followers() {
    let mut leader = pipelining_leader(1, |config| config);
    seed_replicating(&mut leader, NodeId(2), LogIndex::ZERO);
    seed_replicating(&mut leader, NodeId(3), LogIndex::ZERO);

    let outputs = leader.step(Input::Tick);
    let sent_entries: Vec<crate::SharedEntries> = outputs
        .iter()
        .filter_map(|output| match output {
            Output::Send {
                message: Message::AppendEntries(AppendEntries { entries, .. }),
                ..
            } if !entries.is_empty() => Some(entries.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(
        sent_entries.len(),
        2,
        "both followers receive the suffix entry"
    );
    assert!(
        sent_entries[0].shares_allocation(&sent_entries[1]),
        "both follower batches share one append-entry slice"
    );

    let sent_payloads: Vec<crate::SharedPayload> = sent_entries
        .iter()
        .filter_map(|entries| {
            entries.first().map(|entry| match &entry.kind {
                crate::LogEntryKind::Application(payload) => payload.clone(),
                crate::LogEntryKind::Configuration(_) => {
                    panic!("test log holds application entries")
                }
                crate::LogEntryKind::Noop => panic!("test batch should not start with a no-op"),
            })
        })
        .collect();

    let log_payload = match &leader.log_entries_from(LogIndex(1))[0].kind {
        crate::LogEntryKind::Application(payload) => payload.clone(),
        crate::LogEntryKind::Configuration(_) => unreachable!("test log holds application entries"),
        crate::LogEntryKind::Noop => unreachable!("first entry is an application fixture"),
    };
    assert!(
        sent_payloads[0].shares_allocation(&log_payload),
        "the first follower's batch shares the log allocation"
    );
    assert!(
        sent_payloads[1].shares_allocation(&log_payload),
        "the second follower's batch shares the log allocation"
    );

    // Prior-term entries never commit by counting (thesis 3.6.2), so the
    // apply-sharing leg proposes a current-term entry and commits it.
    let _ = leader.step(Input::ClientProposal {
        payload: payload(2),
    });
    let proposed_log_payload = match &leader
        .log_entries_from(LogIndex(1))
        .last()
        .expect("client proposal is appended")
        .kind
    {
        crate::LogEntryKind::Application(payload) => payload.clone(),
        crate::LogEntryKind::Configuration(_) | crate::LogEntryKind::Noop => {
            unreachable!("proposal is an application entry")
        }
    };
    let applied = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            match_index: LogIndex(3),
            sequence: 0,
        }),
    });
    let apply_payload = applied
        .iter()
        .filter_map(|output| match output {
            Output::Apply { index, payload, .. } if *index == LogIndex(3) => Some(payload.clone()),
            _ => None,
        })
        .next_back()
        .expect("quorum acknowledgement commits and applies the proposed entry");
    assert!(
        apply_payload.shares_allocation(&proposed_log_payload),
        "the applied payload shares the log allocation"
    );
}
