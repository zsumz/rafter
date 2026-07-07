#![allow(clippy::wildcard_imports)]

mod support;

use support::*;

#[test]
fn in_memory_driver_reports_unsupported_lease_reads_explicitly() {
    let driver = elected_driver();
    let handle = driver.handle();

    assert_eq!(
        block_on(handle.read("alpha".to_owned(), ReadConsistency::LeaseRead)),
        Err(ReadError::UnsupportedConsistency {
            consistency: ReadConsistency::LeaseRead,
        })
    );
}

#[test]
fn in_memory_driver_reports_read_id_exhaustion_after_max() {
    let mut adopted = scripted_read_group(ScriptedReadMode::Reject);
    adopted
        .begin_read_barrier_outcome(ReadBarrierRequest {
            group_id: (),
            read_id: ReadId(u64::MAX - 1),
            min_applied_index: None,
            context: Vec::new(),
        })
        .expect("manual read consumes the penultimate read id");
    let driver = ScriptedReadDriver::new(NodeId(1), vec![adopted])
        .expect("quiescent manually driven group is adoptable");
    let handle = driver.handle();

    assert!(matches!(
        block_on(handle.read("alpha".to_owned(), ReadConsistency::Linearizable)),
        Err(ReadError::Rejected {
            read_id: Some(ReadId(u64::MAX)),
            ..
        })
    ));
    assert_eq!(
        block_on(handle.read("alpha".to_owned(), ReadConsistency::Linearizable)),
        Err(ReadError::ReadIdExhausted)
    );
}

#[test]
fn in_memory_driver_local_reads_do_not_consume_read_ids() {
    let driver = scripted_read_driver(ScriptedReadMode::Reject);
    let handle = driver.handle();

    assert_eq!(
        block_on(handle.read("missing".to_owned(), ReadConsistency::Local))
            .expect("local read succeeds without read id")
            .result,
        None
    );
    assert_eq!(
        block_on(handle.read("missing".to_owned(), ReadConsistency::Local))
            .expect("repeated local read succeeds without read id")
            .result,
        None
    );
    assert_eq!(
        block_on(handle.read("missing".to_owned(), ReadConsistency::Linearizable)),
        Err(ReadError::Rejected {
            read_id: Some(ReadId(1)),
            reason: ReadIndexRejection::NotLeader {
                role: Role::Follower,
                term: Term(1),
            },
            leader_hint: Some(NodeId(1)),
        })
    );
}

#[test]
fn in_memory_driver_cancels_freshness_unavailable_linearizable_reads() {
    let driver = scripted_read_driver(ScriptedReadMode::Grant(LogIndex(5)));
    let handle = driver.handle();

    for read_id in [ReadId(1), ReadId(2)] {
        assert_eq!(
            block_on(handle.read("missing".to_owned(), ReadConsistency::Linearizable)),
            Err(ReadError::FreshnessUnavailable {
                read_id: Some(read_id),
                required_applied_index: LogIndex(5),
                local_applied_index: LogIndex::ZERO,
            })
        );
        assert_eq!(
            handle.metrics().expect("metrics").current().pending_reads,
            0,
            "abandoned freshness-unavailable read must not leak pending app state"
        );
    }
}

#[test]
fn in_memory_driver_cancels_stalled_linearizable_reads() {
    let driver = scripted_read_driver(ScriptedReadMode::Pending);
    let handle = driver.handle();

    assert_eq!(
        block_on(handle.read("missing".to_owned(), ReadConsistency::Linearizable)),
        Err(ReadError::Transport {
            message: "managed read stalled after 1024 steps".to_owned(),
        })
    );
    assert_eq!(
        handle.metrics().expect("metrics").current().pending_reads,
        0,
        "abandoned stalled read must not leak pending app state"
    );
}

#[test]
fn in_memory_driver_publishes_metrics_for_rejected_reads() {
    let driver = scripted_read_driver(ScriptedReadMode::Reject);
    let handle = driver.handle();

    assert_eq!(
        handle.metrics().expect("metrics").current().commit_index,
        LogIndex::ZERO
    );
    assert_eq!(
        block_on(handle.read("missing".to_owned(), ReadConsistency::Linearizable)),
        Err(ReadError::Rejected {
            read_id: Some(ReadId(1)),
            reason: ReadIndexRejection::NotLeader {
                role: Role::Follower,
                term: Term(1),
            },
            leader_hint: Some(NodeId(1)),
        })
    );
    assert_eq!(
        handle.metrics().expect("metrics").current().commit_index,
        LogIndex(1),
        "rejected read publishes the scripted metrics transition"
    );
}

#[test]
fn in_memory_driver_publishes_metrics_for_canceled_reads() {
    let driver = scripted_read_driver(ScriptedReadMode::Cancel);
    let handle = driver.handle();

    assert_eq!(
        handle.metrics().expect("metrics").current().commit_index,
        LogIndex::ZERO
    );
    assert_eq!(
        block_on(handle.read("missing".to_owned(), ReadConsistency::Linearizable)),
        Err(ReadError::Canceled {
            read_id: ReadId(1),
            reason: ReadIndexCancelReason::LeaderStateReset,
            leader_hint: Some(NodeId(1)),
        })
    );
    assert_eq!(
        handle.metrics().expect("metrics").current().commit_index,
        LogIndex(1),
        "canceled read publishes the scripted metrics transition"
    );
}
