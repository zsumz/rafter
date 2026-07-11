use super::*;

#[test]
fn a_failed_batch_releases_no_output_and_poisons_the_runtime() {
    let mut runtime = elected_leader_with_log_segment(FailAfterLogSegment {
        inner: InMemoryRaftLogSegment::new(),
        allowed: 1,
    });

    let error = runtime
        .step_batch(proposals(3))
        .expect_err("the batch's single flush fails");
    assert!(matches!(error, RaftRuntimeError::LogAppend(_)));

    let error = runtime
        .step_batch(vec![RaftInput::Tick])
        .expect_err("a poisoned runtime refuses further batches");
    assert!(matches!(error, RaftRuntimeError::Poisoned { .. }));
}

/// The prose contract behind the poisoned-accessor change, machine-checked:
/// a failed persist leaves durable state exactly where the last successful
/// persist put it, and a restart from those stores resumes from that state.
#[test]
fn durable_state_never_runs_ahead_of_a_failed_persist_and_restart_resumes_from_it() {
    let mut runtime = elected_leader_with_log_segment(FailAfterLogSegment {
        inner: InMemoryRaftLogSegment::new(),
        allowed: 2,
    });
    runtime
        .step_batch(proposals(1))
        .expect("the first proposal persists");

    let error = runtime
        .step_batch(proposals(1))
        .expect_err("the second proposal's flush fails");
    assert!(matches!(error, RaftRuntimeError::LogAppend(_)));

    // Durable contents: exactly the election no-op and first entry, nothing
    // from the failed batch — even though the poisoned runtime's accessors
    // stepped past it.
    let durable = runtime.log_segment.inner.replay_entries();
    assert_eq!(durable.len(), 2);
    assert_eq!(durable[0].index, rafter::LogIndex(1));
    assert_eq!(durable[1].index, rafter::LogIndex(2));
    assert_eq!(runtime.last_log_index(), rafter::LogIndex(3));

    // Restart from the durable stores: the node resumes at the persisted
    // state and accepts new work.
    let hard_state = runtime.hard_state_store.clone();
    let segment = runtime.log_segment.inner.clone();
    let mut restarted = DurableRaftNode::with_storage(raft_config(2, &[1, 3]), hard_state, segment)
        .expect("restart from the durable stores");
    assert_eq!(restarted.last_log_index(), rafter::LogIndex(2));
    let outputs = restarted
        .step(RaftInput::Tick)
        .expect("restarted node runs");
    assert!(!outputs.is_empty() || restarted.role() != RaftRole::Leader);
}
