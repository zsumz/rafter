use super::*;
use rafter::{PendingSnapshotTransfer, StagedSnapshotChunk};

mod directives;
mod pending;
mod promotion;

/// Elects node 2 leader of a three-node cluster by scripting the vote from
/// node 3; node 4 stays silent so it can lag honestly.
fn elected_leader_with_snapshot_store<S: RaftSnapshotStore + SnapshotChunkSource>(
    snapshot_store: S,
) -> DurableRaftNode<InMemoryRaftHardStateStore, InMemoryRaftLogSegment, S> {
    let mut leader = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[3, 4]),
        InMemoryRaftHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
        snapshot_store,
    )
    .expect("leader hydrates");
    win_election_with_scripted_votes(&mut leader);
    leader
}

/// Drives `node` from follower to leader by scripting node 3's pre-vote and
/// vote grants, which is quorum in any of these three-node fixtures.
fn win_election_with_scripted_votes<H, L, S>(node: &mut DurableRaftNode<H, L, S>)
where
    H: RaftHardStateStore,
    L: RaftLogSegment,
    S: RaftSnapshotStore + SnapshotChunkSource,
{
    let outputs = node.step(RaftInput::Tick).expect("election timeout fires");
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
    let outputs = node
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
    node.step(RaftInput::Message {
        from: RaftNodeId(3),
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term: vote_term,
            voter_id: RaftNodeId(3),
            vote_granted: true,
        }),
    })
    .expect("granted vote elects the leader");
    assert_eq!(node.role(), RaftRole::Leader);
}

fn commit_with_follower_ack<S: RaftSnapshotStore + SnapshotChunkSource>(
    leader: &mut DurableRaftNode<InMemoryRaftHardStateStore, InMemoryRaftLogSegment, S>,
    payload: &[u8],
    index: u64,
) {
    let outputs = leader
        .step(RaftInput::ClientProposal {
            payload: payload.to_vec(),
        })
        .expect("proposal persists");
    let sequence = append_entries_sequence(&outputs);
    // Follower 3 provides the commit quorum; follower 4 stays genuinely
    // behind so a later rejection from it is honest, not stale noise the
    // match floor discards.
    let outputs = leader
        .step(RaftInput::Message {
            from: RaftNodeId(3),
            message: Message::AppendEntriesResponse(rafter::AppendEntriesResponse {
                term: leader.current_term(),
                follower_id: RaftNodeId(3),
                success: true,
                match_index: LogIndex(index),
                sequence,
            }),
        })
        .expect("follower ack advances the commit index");
    assert!(outputs.iter().any(
        |output| matches!(output, RaftOutput::Apply { index: applied, .. } if applied.0 == index)
    ));
}

fn append_entries_sequence(outputs: &[RaftOutput]) -> u64 {
    outputs
        .iter()
        .find_map(|output| match output {
            RaftOutput::Send {
                message: Message::AppendEntries(append),
                ..
            } => Some(append.sequence),
            _ => None,
        })
        .expect("leader replicates to its follower")
}

/// Reports the follower as lagging behind the compacted prefix: the failed
/// append probe decrements `next_index` to the snapshot boundary, so the
/// leader turns to snapshot streaming in the same step.
fn report_follower_lag<S: RaftSnapshotStore + SnapshotChunkSource>(
    leader: &mut DurableRaftNode<InMemoryRaftHardStateStore, InMemoryRaftLogSegment, S>,
) -> Vec<RaftOutput> {
    let outputs = leader.step(RaftInput::Tick).expect("leader heartbeats");
    let sequence = append_entries_sequence(&outputs);
    leader
        .step(RaftInput::Message {
            from: RaftNodeId(4),
            message: Message::AppendEntriesResponse(rafter::AppendEntriesResponse {
                term: leader.current_term(),
                follower_id: RaftNodeId(4),
                success: false,
                match_index: LogIndex::ZERO,
                sequence,
            }),
        })
        .expect("failed probe persists nothing but may emit a snapshot chunk")
}

/// A snapshot store whose whole state sits behind a lock, cloneable into
/// several handles onto one medium.
///
/// The runtime's staging repairs read the store through the trait; this store
/// can hand out nothing that outlives its guard and caches nothing, so a repair
/// that worked only against an owned-field store fails here.
#[derive(Clone, Debug, Default)]
struct GuardedSnapshotStore {
    medium: std::sync::Arc<std::sync::Mutex<InMemoryRaftSnapshotStore>>,
}

impl GuardedSnapshotStore {
    fn medium(&self) -> std::sync::MutexGuard<'_, InMemoryRaftSnapshotStore> {
        self.medium.lock().expect("guarded snapshot store is live")
    }
}

impl RaftSnapshotStore for GuardedSnapshotStore {
    fn write_snapshot(
        &mut self,
        snapshot: PersistedRaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.medium().write_snapshot(snapshot)
    }

    fn write_snapshot_from_source(
        &mut self,
        snapshot: &RaftSnapshot,
        source: &dyn SnapshotChunkSource,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.medium().write_snapshot_from_source(snapshot, source)
    }

    fn current_snapshot(&self) -> Option<RaftSnapshot> {
        self.medium().current_snapshot()
    }

    fn stage_snapshot_chunk(
        &mut self,
        chunk: &StagedSnapshotChunk,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.medium().stage_snapshot_chunk(chunk)
    }

    fn promote_staged_snapshot(
        &mut self,
        snapshot: &RaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.medium().promote_staged_snapshot(snapshot)
    }

    fn clear_pending_snapshot_transfer(&mut self) -> Result<(), RaftSnapshotStoreWriteError> {
        self.medium().clear_pending_snapshot_transfer()
    }

    fn current_pending_snapshot_transfer(&self) -> Option<PendingSnapshotTransfer> {
        self.medium().current_pending_snapshot_transfer()
    }
}

impl SnapshotChunkSource for GuardedSnapshotStore {
    fn snapshot_chunk(&self, request: rafter::SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        self.medium().snapshot_chunk(request)
    }
}

/// Delegates storage to an in-memory store but cannot serve any snapshot
/// chunk, forcing every leader chunk directive to be unresolvable.
#[derive(Clone, Debug, Eq, PartialEq)]
struct UnservableChunkSourceStore(InMemoryRaftSnapshotStore);

impl RaftSnapshotStore for UnservableChunkSourceStore {
    fn write_snapshot(
        &mut self,
        snapshot: PersistedRaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.0.write_snapshot(snapshot)
    }

    fn write_snapshot_from_source(
        &mut self,
        snapshot: &RaftSnapshot,
        source: &dyn SnapshotChunkSource,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.0.write_snapshot_from_source(snapshot, source)
    }

    fn current_snapshot(&self) -> Option<RaftSnapshot> {
        self.0.current_snapshot()
    }

    fn stage_snapshot_chunk(
        &mut self,
        chunk: &StagedSnapshotChunk,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.0.stage_snapshot_chunk(chunk)
    }

    fn promote_staged_snapshot(
        &mut self,
        snapshot: &RaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.0.promote_staged_snapshot(snapshot)
    }

    fn clear_pending_snapshot_transfer(&mut self) -> Result<(), RaftSnapshotStoreWriteError> {
        self.0.clear_pending_snapshot_transfer()
    }

    fn current_pending_snapshot_transfer(&self) -> Option<PendingSnapshotTransfer> {
        self.0.current_pending_snapshot_transfer()
    }
}

impl SnapshotChunkSource for UnservableChunkSourceStore {
    fn snapshot_chunk(&self, _request: rafter::SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        None
    }
}
