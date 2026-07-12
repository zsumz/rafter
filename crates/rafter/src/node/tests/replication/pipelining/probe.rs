//! Single-probe discipline and heartbeat behavior while awaiting acknowledgement.

use super::support::*;
use super::*;

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
