use super::*;

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

    let outputs = leader
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
    let outputs = leader
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
    leader
        .step(RaftInput::Message {
            from: RaftNodeId(3),
            message: Message::RequestVoteResponse(RequestVoteResponse {
                term: vote_term,
                voter_id: RaftNodeId(3),
                vote_granted: true,
            }),
        })
        .expect("granted vote elects the leader");
    assert_eq!(leader.role(), RaftRole::Leader);
    leader
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

    fn current_pending_snapshot_transfer(&self) -> Option<&PendingSnapshotTransfer> {
        self.0.current_pending_snapshot_transfer()
    }
}

impl SnapshotChunkSource for UnservableChunkSourceStore {
    fn snapshot_chunk(&self, _request: rafter::SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        None
    }
}

#[test]
fn runtime_leader_resolves_chunk_directives_into_install_snapshot_chunk_messages() {
    let mut leader = elected_leader_with_snapshot_store(InMemoryRaftSnapshotStore::new());
    commit_with_follower_ack(&mut leader, b"create stream payload", 2);
    commit_with_follower_ack(&mut leader, b"append stream payload", 3);
    let snapshot = raft_snapshot_for_writer(3, 1, 1, 2, b"opaque application snapshot");
    leader
        .compact_log_with_snapshot(snapshot.clone())
        .expect("leader compacts through its durable snapshot");

    let outputs = report_follower_lag(&mut leader);

    assert!(
        outputs
            .iter()
            .all(|output| !matches!(output, RaftOutput::SendSnapshotChunk { .. })),
        "callers must never see unresolved snapshot chunk directives"
    );
    let chunk = outputs
        .iter()
        .find_map(|output| match output {
            RaftOutput::Send {
                to,
                message: Message::InstallSnapshotChunk(chunk),
            } if *to == RaftNodeId(4) => Some(chunk),
            _ => None,
        })
        .expect("leader streams a resolved snapshot chunk to the lagging follower");
    let installed = leader
        .snapshot()
        .expect("leader installed a local snapshot");
    assert_eq!(chunk.metadata, installed.metadata);
    assert_eq!(chunk.transfer_id, installed.transfer_id());
    assert_eq!(
        chunk.application_payload_crc32,
        installed.application_payload_crc32
    );
    assert_eq!(
        chunk.total_payload_len,
        snapshot.application_payload.len() as u64
    );
    assert_eq!(chunk.offset, 0);
    assert_eq!(chunk.chunk, snapshot.application_payload);
    assert!(chunk.done);
}

#[test]
fn runtime_drops_snapshot_chunk_directives_the_store_cannot_serve() {
    let mut leader = elected_leader_with_snapshot_store(UnservableChunkSourceStore(
        InMemoryRaftSnapshotStore::new(),
    ));
    commit_with_follower_ack(&mut leader, b"create stream payload", 2);
    commit_with_follower_ack(&mut leader, b"append stream payload", 3);
    leader
        .compact_log_with_snapshot(raft_snapshot_for_writer(3, 1, 1, 2, b"unservable payload"))
        .expect("leader compacts through its durable snapshot");

    let outputs = report_follower_lag(&mut leader);

    assert!(
        outputs.iter().all(|output| !matches!(
            output,
            RaftOutput::SendSnapshotChunk { .. }
                | RaftOutput::Send {
                    message: Message::InstallSnapshotChunk(_),
                    ..
                }
        )),
        "an unresolvable directive is dropped like a lost message"
    );
}

#[test]
fn runtime_persists_pending_snapshot_chunk_and_resumes_after_restart() {
    let payload = b"abcdefghi".to_vec();
    let metadata = snapshot_metadata(3, 4, 5);
    let descriptor = RaftSnapshot::from_payload(metadata.clone(), &payload);
    let transfer_id = descriptor.transfer_id();
    let first_chunk = rafter::InstallSnapshotChunk {
        term: Term(5),
        leader_id: RaftNodeId(1),
        transfer_id,
        metadata: metadata.clone(),
        total_payload_len: payload.len() as u64,
        application_payload_crc32: descriptor.application_payload_crc32,
        offset: 0,
        chunk: payload[..4].to_vec(),
        done: false,
    };
    let mut runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store(5, None),
        InMemoryRaftLogSegment::new(),
        InMemoryRaftSnapshotStore::new(),
    )
    .expect("runtime hydrates");

    let first_outputs = runtime
        .step(RaftInput::Message {
            from: RaftNodeId(1),
            message: Message::InstallSnapshotChunk(first_chunk.clone()),
        })
        .expect("first snapshot chunk persists");

    assert!(matches!(
        first_outputs.as_slice(),
        [
            RaftOutput::StageSnapshotChunk { chunk },
            RaftOutput::Send {
                message: Message::InstallSnapshotResponse(response),
                ..
            }
        ] if !chunk.done
            && chunk.offset == 0
            && chunk.bytes.as_slice() == &payload[..4]
            && response.success
            && response.next_offset == 4
    ));
    assert_eq!(
        runtime
            .snapshot_store
            .current_pending_snapshot_transfer()
            .expect("pending transfer persisted")
            .received_len,
        4
    );

    let mut restarted = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        runtime.hard_state_store.clone(),
        runtime.log_segment.clone(),
        runtime.snapshot_store.clone(),
    )
    .expect("runtime resumes pending transfer");
    let follower_status = restarted
        .snapshot_transfer_status()
        .follower
        .expect("pending transfer is visible after restart");
    assert_eq!(follower_status.received_bytes, 4);

    let final_chunk = rafter::InstallSnapshotChunk {
        term: Term(5),
        leader_id: RaftNodeId(1),
        transfer_id,
        metadata: metadata.clone(),
        total_payload_len: payload.len() as u64,
        application_payload_crc32: descriptor.application_payload_crc32,
        offset: 4,
        chunk: payload[4..].to_vec(),
        done: true,
    };
    let final_outputs = restarted
        .step(RaftInput::Message {
            from: RaftNodeId(1),
            message: Message::InstallSnapshotChunk(final_chunk),
        })
        .expect("final snapshot chunk installs");

    assert!(final_outputs.iter().any(|output| matches!(
        output,
        RaftOutput::ApplySnapshot { snapshot }
            if snapshot.metadata == metadata
                && snapshot.application_payload_len == payload.len() as u64
    )));
    assert_eq!(restarted.snapshot_index(), LogIndex(3));
    let current_snapshot = restarted
        .snapshot_store
        .current()
        .expect("current snapshot persisted");
    assert_eq!(current_snapshot.metadata, metadata);
    assert_eq!(
        current_snapshot.application_payload.as_slice(),
        payload.as_slice()
    );
    assert_eq!(
        restarted.snapshot_store.current_pending_snapshot_transfer(),
        None
    );
}

#[test]
fn runtime_clears_stale_pending_snapshot_transfer_on_restart() {
    let mut snapshot_store = InMemoryRaftSnapshotStore::with_snapshot(PersistedRaftSnapshot {
        metadata: snapshot_metadata(5, 5, 5),
        application_payload: b"current".to_vec(),
    });
    let stale_metadata = snapshot_metadata(3, 4, 5);
    snapshot_store
        .stage_snapshot_chunk(&StagedSnapshotChunk {
            leader_id: RaftNodeId(1),
            transfer_id: snapshot_transfer_id(&stale_metadata, 10),
            metadata: stale_metadata,
            total_payload_len: 10,
            application_payload_crc32: 0,
            offset: 0,
            bytes: b"partial".to_vec(),
            done: false,
        })
        .expect("stale transfer stages");

    let runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store(5, None),
        InMemoryRaftLogSegment::new(),
        snapshot_store,
    )
    .expect("runtime hydrates and clears stale pending transfer");

    assert_eq!(runtime.snapshot_index(), LogIndex(5));
    assert_eq!(
        runtime.snapshot_store.current_pending_snapshot_transfer(),
        None
    );
}

/// Stages the whole `payload` for `metadata`'s transfer in two chunks and
/// stops there — the durable shape a crash leaves when the process dies
/// after staging the final chunk but before promoting the staging.
fn complete_staged_transfer_store(
    metadata: &RaftSnapshotMetadata,
    payload: &[u8],
) -> InMemoryRaftSnapshotStore {
    let descriptor = RaftSnapshot::from_payload(metadata.clone(), payload);
    let transfer_id = descriptor.transfer_id();
    let mut snapshot_store = InMemoryRaftSnapshotStore::new();
    snapshot_store
        .stage_snapshot_chunk(&StagedSnapshotChunk {
            leader_id: RaftNodeId(1),
            transfer_id,
            metadata: metadata.clone(),
            total_payload_len: payload.len() as u64,
            application_payload_crc32: descriptor.application_payload_crc32,
            offset: 0,
            bytes: payload[..4].to_vec(),
            done: false,
        })
        .expect("first chunk stages");
    snapshot_store
        .stage_snapshot_chunk(&StagedSnapshotChunk {
            leader_id: RaftNodeId(1),
            transfer_id,
            metadata: metadata.clone(),
            total_payload_len: payload.len() as u64,
            application_payload_crc32: descriptor.application_payload_crc32,
            offset: 4,
            bytes: payload[4..].to_vec(),
            done: true,
        })
        .expect("final chunk stages; the crash strikes before promotion");
    snapshot_store
}

#[test]
fn runtime_promotes_complete_staged_transfer_left_by_crash_before_promotion() {
    // Crash window: the final chunk of an inbound transfer was staged
    // durably but the process died before the promote. The kernel refuses
    // to resume complete transfers, so reopen must finish the interrupted
    // installation — promote the staging and compact through its boundary —
    // instead of failing on every boot.
    let payload = b"abcdefghi".to_vec();
    let metadata = snapshot_metadata(3, 4, 5);
    let snapshot_store = complete_staged_transfer_store(&metadata, &payload);

    let runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store(5, None),
        InMemoryRaftLogSegment::new(),
        snapshot_store,
    )
    .expect("reopen finishes the interrupted installation");

    assert_eq!(runtime.snapshot_index(), LogIndex(3));
    let current = runtime
        .snapshot_store
        .current()
        .expect("promoted snapshot is current");
    assert_eq!(current.metadata, metadata);
    assert_eq!(current.application_payload, payload);
    assert_eq!(
        runtime.snapshot_store.current_pending_snapshot_transfer(),
        None
    );
    assert_eq!(runtime.log_segment.compacted_through(), LogIndex(3));
    assert_eq!(runtime.log_segment.next_index(), LogIndex(4));
}

#[test]
fn runtime_promotes_complete_staged_transfer_and_compacts_covered_log() {
    // The same crash window on a follower that still holds log entries
    // below the transfer boundary: finishing the installation also
    // completes the compaction, so the segment's next appendable index is
    // the kernel's first appendable index.
    let payload = b"abcdefghi".to_vec();
    let metadata = snapshot_metadata(3, 4, 5);
    let snapshot_store = complete_staged_transfer_store(&metadata, &payload);
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[
            persisted_entry(1, 1, b"covered-one"),
            persisted_entry(2, 1, b"covered-two"),
        ])
        .expect("stale prefix persists");

    let runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        hard_state_store(5, None),
        log_segment,
        snapshot_store,
    )
    .expect("reopen finishes the interrupted installation and compaction");

    assert_eq!(runtime.snapshot_index(), LogIndex(3));
    assert_eq!(runtime.last_log_index(), LogIndex(3));
    assert_eq!(
        runtime.snapshot_store.current_pending_snapshot_transfer(),
        None
    );
    assert_eq!(runtime.log_segment.compacted_through(), LogIndex(3));
    assert_eq!(runtime.log_segment.next_index(), LogIndex(4));
    assert_eq!(runtime.log_segment.replay_entries(), Vec::new());
}

#[test]
fn runtime_restarted_follower_catches_up_from_compacted_leader_snapshot() {
    let snapshot = compacted_leader_snapshot();
    let mut follower = stale_snapshot_follower();
    let transfer_id =
        RaftSnapshot::from_payload(snapshot.metadata.clone(), &snapshot.application_payload)
            .transfer_id();
    let split_at = 4;

    let first_outputs = install_snapshot_chunk(&mut follower, &snapshot, transfer_id, 0, split_at);
    assert_partial_snapshot_transfer(&follower, &first_outputs, split_at as u64);

    let mut restarted_follower = restart_snapshot_follower(&follower);
    let final_outputs = install_snapshot_chunk(
        &mut restarted_follower,
        &snapshot,
        transfer_id,
        split_at,
        snapshot.application_payload.len(),
    );
    let applied_snapshot = applied_snapshot_from(&final_outputs);

    assert_eq!(
        applied_snapshot,
        &RaftSnapshot::new(
            snapshot.metadata.clone(),
            snapshot.application_payload.len() as u64,
            rafter_storage::crc32(&snapshot.application_payload),
        )
    );
    assert_eq!(restarted_follower.snapshot_index(), LogIndex(3));
    assert_eq!(restarted_follower.commit_index(), LogIndex(3));
    assert_eq!(restarted_follower.snapshot_store.current(), Some(&snapshot));

    let hydrated_follower = restart_snapshot_follower(&restarted_follower);
    assert_eq!(hydrated_follower.snapshot_index(), LogIndex(3));
    assert_eq!(
        hydrated_follower
            .snapshot_store
            .current()
            .expect("promoted snapshot survives restart")
            .application_payload
            .as_slice(),
        b"opaque application snapshot".as_slice()
    );
}
