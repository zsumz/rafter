//! Group commit: a batch of inputs persists with one durable flush per
//! store, and no output from any batched input escapes before that flush.

use std::{cell::Cell, rc::Rc};

use super::*;
use rafter_storage::{
    RaftLogSegmentAppendError, RaftLogSegmentCompactError, RaftLogSegmentTruncateError,
};

/// Counts every append and truncate call while delegating to an in-memory
/// segment: one `append_entries` call is one durable flush in a file-backed
/// segment, so the counter observes the amortization directly.
struct CountingLogSegment {
    inner: InMemoryRaftLogSegment,
    appends: Rc<Cell<u64>>,
    truncates: Rc<Cell<u64>>,
}

impl CountingLogSegment {
    fn new() -> (Self, Rc<Cell<u64>>, Rc<Cell<u64>>) {
        let appends = Rc::new(Cell::new(0));
        let truncates = Rc::new(Cell::new(0));
        (
            Self {
                inner: InMemoryRaftLogSegment::new(),
                appends: Rc::clone(&appends),
                truncates: Rc::clone(&truncates),
            },
            appends,
            truncates,
        )
    }
}

impl RaftLogSegment for CountingLogSegment {
    fn append_entries(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), RaftLogSegmentAppendError> {
        self.appends.set(self.appends.get() + 1);
        self.inner.append_entries(entries)
    }

    fn truncate_suffix(
        &mut self,
        from_index: rafter::LogIndex,
    ) -> Result<(), RaftLogSegmentTruncateError> {
        self.truncates.set(self.truncates.get() + 1);
        self.inner.truncate_suffix(from_index)
    }

    fn compact_prefix_through(
        &mut self,
        through_index: rafter::LogIndex,
    ) -> Result<(), RaftLogSegmentCompactError> {
        self.inner.compact_prefix_through(through_index)
    }

    fn compacted_through(&self) -> rafter::LogIndex {
        self.inner.compacted_through()
    }

    fn replay_entries(&self) -> Vec<PersistedRaftLogEntry> {
        self.inner.replay_entries()
    }

    fn next_index(&self) -> rafter::LogIndex {
        self.inner.next_index()
    }
}

fn proposals(count: u64) -> Vec<RaftInput> {
    (1..=count)
        .map(|index| RaftInput::ClientProposal {
            payload: vec![u8::try_from(index).expect("test batch fits a byte"); 8],
        })
        .collect()
}

#[test]
fn a_batched_step_appends_the_whole_suffix_in_one_durable_flush() {
    let (segment, appends, truncates) = CountingLogSegment::new();
    let mut runtime = elected_leader_with_log_segment(segment);
    let appends_after_election = appends.get();

    let outputs = runtime
        .step_batch(proposals(4))
        .expect("batched proposals persist");

    assert_eq!(
        appends.get() - appends_after_election,
        1,
        "four batched proposals land in one suffix append"
    );
    assert_eq!(truncates.get(), 0);
    assert_eq!(runtime.last_log_index(), rafter::LogIndex(5));
    let sent_appends = outputs
        .iter()
        .filter(|output| {
            matches!(
                output,
                RaftOutput::Send {
                    message: Message::AppendEntries(_),
                    ..
                }
            )
        })
        .count();
    assert!(
        sent_appends > 0,
        "the batch's replication traffic is released after the flush"
    );
}

#[test]
fn unbatched_steps_pay_one_flush_per_proposal() {
    let (segment, appends, _truncates) = CountingLogSegment::new();
    let mut runtime = elected_leader_with_log_segment(segment);
    let appends_after_election = appends.get();

    for input in proposals(4) {
        runtime.step(input).expect("proposal persists");
    }

    assert_eq!(
        appends.get() - appends_after_election,
        4,
        "one suffix append per unbatched proposal"
    );
}

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

#[test]
fn an_empty_batch_is_a_durable_no_op() {
    let (segment, appends, truncates) = CountingLogSegment::new();
    let mut runtime = elected_leader_with_log_segment(segment);
    let appends_after_election = appends.get();

    let outputs = runtime.step_batch(Vec::new()).expect("empty batch is fine");

    assert!(outputs.is_empty());
    assert_eq!(appends.get() - appends_after_election, 0);
    assert_eq!(truncates.get(), 0);
}

/// Elects node 2 leader of {1, 2, 3} over the supplied log segment by
/// scripting node 3's vote, mirroring the single-step election helpers.
fn elected_leader_with_log_segment<L: RaftLogSegment>(
    log_segment: L,
) -> DurableRaftNode<InMemoryRaftHardStateStore, L, InMemoryRaftSnapshotStore> {
    let mut runtime = DurableRaftNode::with_storage(
        raft_config(2, &[1, 3]),
        InMemoryRaftHardStateStore::new(),
        log_segment,
    )
    .expect("runtime hydrates");

    let outputs = runtime
        .step(RaftInput::Tick)
        .expect("election timeout fires");
    let poll_term = outputs
        .iter()
        .find_map(|output| match output {
            RaftOutput::Send {
                message: Message::PreVote(request),
                ..
            } => Some(request.term),
            _ => None,
        })
        .expect("timed-out node opens a pre-vote poll");
    let outputs = runtime
        .step(RaftInput::Message {
            from: RaftNodeId(3),
            message: Message::PreVoteResponse(rafter::PreVoteResponse {
                term: poll_term,
                voter_id: RaftNodeId(3),
                vote_granted: true,
            }),
        })
        .expect("granted poll starts the election");
    let vote_term = outputs
        .iter()
        .find_map(|output| match output {
            RaftOutput::Send {
                message: Message::RequestVote(request),
                ..
            } => Some(request.term),
            _ => None,
        })
        .expect("timed-out node starts an election");
    runtime
        .step(RaftInput::Message {
            from: RaftNodeId(3),
            message: Message::RequestVoteResponse(rafter::RequestVoteResponse {
                term: vote_term,
                voter_id: RaftNodeId(3),
                vote_granted: true,
            }),
        })
        .expect("granted vote elects the leader");
    assert_eq!(runtime.role(), RaftRole::Leader);
    runtime
}

/// Fails every append after the first `allowed` calls, exposing the inner
/// segment so tests can inspect exactly what became durable.
struct FailAfterLogSegment {
    inner: InMemoryRaftLogSegment,
    allowed: u32,
}

impl RaftLogSegment for FailAfterLogSegment {
    fn append_entries(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), RaftLogSegmentAppendError> {
        if entries.is_empty() {
            return self.inner.append_entries(entries);
        }
        if self.allowed == 0 {
            return Err(RaftLogSegmentAppendError::NonContiguous {
                expected: self.inner.next_index(),
                actual: entries[0].index,
            });
        }
        self.allowed -= 1;
        self.inner.append_entries(entries)
    }

    fn truncate_suffix(
        &mut self,
        from_index: rafter::LogIndex,
    ) -> Result<(), RaftLogSegmentTruncateError> {
        self.inner.truncate_suffix(from_index)
    }

    fn compact_prefix_through(
        &mut self,
        through_index: rafter::LogIndex,
    ) -> Result<(), RaftLogSegmentCompactError> {
        self.inner.compact_prefix_through(through_index)
    }

    fn compacted_through(&self) -> rafter::LogIndex {
        self.inner.compacted_through()
    }

    fn replay_entries(&self) -> Vec<PersistedRaftLogEntry> {
        self.inner.replay_entries()
    }

    fn next_index(&self) -> rafter::LogIndex {
        self.inner.next_index()
    }
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
