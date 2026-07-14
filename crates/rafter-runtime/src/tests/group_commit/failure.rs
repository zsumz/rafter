use super::*;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};

#[test]
fn group_commit_failure_is_surfaced_without_releasing_outputs() {
    let mut runtime = elected_leader_with_log_segment(FailAfterLogSegment {
        inner: InMemoryRaftLogSegment::new(),
        allowed: 1,
    });

    let error = runtime
        .step_batch(proposals(3))
        .expect_err("the batch's single flush fails");
    oracle_assert_eq!(
        error,
        RaftRuntimeError::LogAppend(RaftLogSegmentAppendError::Io {
            operation: INJECTED_APPEND_OPERATION,
            message: INJECTED_APPEND_MESSAGE.to_owned(),
        })
    );
    oracle_assert_eq!(
        runtime.log_segment.inner.replay_entries().len(),
        1,
        "the failed batch returns no outputs and persists none of its entries"
    );
}

#[test]
fn group_commit_failure_poisons_runtime_and_rejects_further_writes() {
    let mut runtime = elected_leader_with_log_segment(FailAfterLogSegment {
        inner: InMemoryRaftLogSegment::new(),
        allowed: 1,
    });

    runtime
        .step_batch(proposals(3))
        .expect_err("the injected append failure is surfaced");

    let error = runtime
        .step_batch(proposals(1))
        .expect_err("a poisoned runtime refuses further writes");
    oracle_assert!(matches!(
        error,
        RaftRuntimeError::Poisoned {
            cause: RaftRuntimeFatalError::LogAppend(RaftLogSegmentAppendError::Io {
                operation: INJECTED_APPEND_OPERATION,
                ref message,
            }),
        } if message == INJECTED_APPEND_MESSAGE
    ));
}

#[test]
fn group_commit_failure_preserves_last_successful_state_across_crash_and_reopen() {
    let mut runtime = elected_leader_with_log_segment(FailAfterLogSegment {
        inner: InMemoryRaftLogSegment::new(),
        allowed: 2,
    });
    runtime
        .step_batch(proposals(1))
        .expect("the first proposal persists");
    let last_successful_index = runtime.last_log_index();

    let error = runtime
        .step_batch(proposals(1))
        .expect_err("the second proposal's flush fails");
    oracle_assert!(matches!(error, RaftRuntimeError::LogAppend(_)));

    let durable_log = runtime.log_segment.inner.replay_entries();
    let durable_hard_state = runtime.hard_state_store.current();
    oracle_assert_eq!(durable_log.len(), 2);
    oracle_assert_eq!(durable_log[0].index, rafter::LogIndex(1));
    oracle_assert_eq!(durable_log[1].index, last_successful_index);
    oracle_assert_eq!(runtime.last_log_index(), rafter::LogIndex(3));
    oracle_assert!(matches!(
        runtime
            .step_batch(proposals(1))
            .expect_err("stepped-ahead volatile state cannot execute"),
        RaftRuntimeError::Poisoned { .. }
    ));

    let hard_state = runtime.hard_state_store.clone();
    let segment = runtime.log_segment.inner.clone();
    drop(runtime);

    let mut restarted = DurableRaftNode::with_storage(raft_config(2, &[1, 3]), hard_state, segment)
        .expect("reopen from the durable stores");
    oracle_assert_eq!(restarted.last_log_index(), last_successful_index);
    oracle_assert_eq!(restarted.log_segment.replay_entries(), durable_log);
    oracle_assert_eq!(restarted.hard_state_store.current(), durable_hard_state);

    let outputs = restarted
        .step(RaftInput::Tick)
        .expect("the reopened node runs from durable state");
    oracle_assert!(!outputs.is_empty() || restarted.role() != RaftRole::Leader);
}
