//! Group commit: a batch of inputs persists with one durable flush per
//! store, and no output from any batched input escapes before that flush.

use std::{cell::Cell, rc::Rc};

use super::*;
use rafter_storage::{
    RaftLogSegmentAppendError, RaftLogSegmentCompactError, RaftLogSegmentTruncateError,
};

mod batch;
mod failure;

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
