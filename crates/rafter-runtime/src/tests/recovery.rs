use super::recording_stores::{
    RecordingHardStateStore, RecordingLogSegment, RecordingSnapshotStore, StoreJournal,
};
use super::snapshot::{raft_snapshot, FailingSnapshotStore};
use super::*;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};

#[test]
fn restarted_node_recovers_persisted_configuration_entry() {
    let mut runtime = durable_node_with_log(
        1,
        &[],
        InMemoryRaftHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
    );
    assert!(runtime
        .step(RaftInput::Tick)
        .expect("leader elected")
        .is_empty());
    let configuration = learner_configuration_entry(ConfigurationId(1));
    let _ = runtime
        .step(RaftInput::AddLearner {
            learner_id: RaftNodeId(2),
        })
        .expect("configuration entry persists");
    let hard_state_store = runtime.hard_state_store.clone();
    let log_segment = runtime.log_segment.clone();

    let restarted = durable_node_with_log(1, &[], hard_state_store, log_segment);

    assert_eq!(restarted.last_log_index(), LogIndex(2));
    assert_eq!(
        restarted.effective_configuration_entry(),
        Some(configuration.clone())
    );
    assert_eq!(
        restarted.log_entries_from(LogIndex(1)),
        vec![
            LogEntry::noop(Term(1)),
            LogEntry::configuration(Term(1), configuration)
        ]
    );
}

#[test]
fn restarted_node_recovers_committed_dynamic_membership_suffix_after_snapshot() {
    let (mut restarted, stable, new) = dynamic_membership_recovery_fixture();

    oracle_assert_eq!(restarted.commit_index(), LogIndex(16));
    oracle_assert_eq!(restarted.committed_configuration_entry(), Some(stable));
    oracle_assert_eq!(
        restarted.committed_membership(),
        MembershipConfig::stable(new)
    );

    elect_runtime_leader_with_grant(&mut restarted, RaftNodeId(3));
    let outputs = propose_and_ack_runtime_entry(
        &mut restarted,
        RaftNodeId(3),
        LogIndex(18),
        b"after-restart",
    );

    oracle_assert_eq!(restarted.commit_index(), LogIndex(18));
    oracle_assert!(outputs.iter().any(|output| matches!(
        output,
        RaftOutput::Apply {
            index: LogIndex(18),
            payload,
            ..
        } if payload.as_ref() == b"after-restart"
    )));
}

pub(super) fn dynamic_membership_recovery_fixture() -> (
    DurableRaftNode<InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore>,
    ConfigurationEntry,
    MembershipSet,
) {
    let old = membership_set(&[1, 2, 3]);
    let new = membership_set(&[1, 3, 4]);
    let joint = ConfigurationEntry::joint(
        ConfigurationId(10),
        JointMembership::new(old.clone(), new.clone()),
    );
    let stable = ConfigurationEntry::stable(ConfigurationId(11), new.clone());
    let mut hard_state_store = InMemoryRaftHardStateStore::new();
    hard_state_store
        .write_hard_state(RaftHardState {
            current_term: Term(8),
            voted_for: None,
            commit_index: LogIndex(16),
            committed_configuration: Some(CommittedConfiguration {
                index: LogIndex(16),
                config_id: ConfigurationId(11),
            }),
        })
        .expect("hard state writes");
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .compact_prefix_through(LogIndex(5))
        .expect("snapshot boundary compacts");
    let entries = dynamic_membership_recovery_log(joint, stable.clone());
    log_segment
        .append_entries(&entries)
        .expect("retained post-snapshot log writes");
    let snapshot_store = InMemoryRaftSnapshotStore::with_snapshot(snapshot_at_five(old));

    let restarted = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(1, &[2, 3]),
        hard_state_store,
        log_segment,
        snapshot_store,
    )
    .expect("node restarts from committed dynamic membership suffix");

    (restarted, stable, new)
}

fn dynamic_membership_recovery_log(
    joint: ConfigurationEntry,
    stable: ConfigurationEntry,
) -> Vec<PersistedRaftLogEntry> {
    let mut entries = (6..15)
        .map(|index| {
            PersistedRaftLogEntry::application(LogIndex(index), Term(8), b"normal".to_vec())
        })
        .collect::<Vec<_>>();
    entries.push(PersistedRaftLogEntry::configuration(
        LogIndex(15),
        Term(8),
        joint,
    ));
    entries.push(PersistedRaftLogEntry::configuration(
        LogIndex(16),
        Term(8),
        stable,
    ));
    entries
}

fn snapshot_at_five(committed_membership: MembershipSet) -> PersistedRaftSnapshot {
    PersistedRaftSnapshot {
        metadata: RaftSnapshotMetadata::new(
            SnapshotGroupId::new("dynamic-membership").expect("snapshot group id is valid"),
            RaftNodeId(1),
            LogIndex(5),
            Term(6),
            Term(8),
            ApplicationSnapshotMetadata::new(
                ApplicationSnapshotKind::new("kv").expect("snapshot kind is valid"),
                ApplicationSnapshotVersion::new(1).expect("snapshot version is valid"),
            ),
        )
        .expect("snapshot metadata is valid")
        .with_committed_membership(MembershipConfig::stable(committed_membership)),
        application_payload: Vec::new(),
    }
}

pub(super) fn elect_runtime_leader_with_grant<H, L, S>(
    runtime: &mut DurableRaftNode<H, L, S>,
    voter_id: RaftNodeId,
) where
    H: RaftHardStateStore,
    L: RaftLogSegment,
    S: RaftSnapshotStore + SnapshotChunkSource,
{
    let outputs = runtime.step(RaftInput::Tick).expect("pre-vote starts");
    let poll_term = outputs
        .iter()
        .find_map(|output| match output {
            RaftOutput::Send {
                message: Message::PreVote(request),
                ..
            } => Some(request.term),
            _ => None,
        })
        .expect("pre-vote request is sent");
    let outputs = runtime
        .step(RaftInput::Message {
            from: voter_id,
            message: Message::PreVoteResponse(PreVoteResponse {
                term: poll_term,
                voter_id,
                vote_granted: true,
            }),
        })
        .expect("pre-vote grant starts election");
    let vote_term = outputs
        .iter()
        .find_map(|output| match output {
            RaftOutput::Send {
                message: Message::RequestVote(request),
                ..
            } => Some(request.term),
            _ => None,
        })
        .expect("request-vote is sent");
    runtime
        .step(RaftInput::Message {
            from: voter_id,
            message: Message::RequestVoteResponse(RequestVoteResponse {
                term: vote_term,
                voter_id,
                vote_granted: true,
            }),
        })
        .expect("vote grant elects leader");
    assert_eq!(runtime.role(), RaftRole::Leader);
}

fn propose_and_ack_runtime_entry<H, L, S>(
    runtime: &mut DurableRaftNode<H, L, S>,
    follower_id: RaftNodeId,
    match_index: LogIndex,
    payload: &[u8],
) -> Vec<RaftOutput>
where
    H: RaftHardStateStore,
    L: RaftLogSegment,
    S: RaftSnapshotStore + SnapshotChunkSource,
{
    let _ = runtime
        .step(RaftInput::ClientProposal {
            payload: payload.to_vec(),
        })
        .expect("leader appends after restart");
    runtime
        .step(RaftInput::Message {
            from: follower_id,
            message: Message::AppendEntriesResponse(AppendEntriesResponse {
                sequence: 0,
                term: runtime.current_term(),
                follower_id,
                success: true,
                match_index,
            }),
        })
        .expect("follower acknowledgement commits new entry")
}

/// The whole reason the value is not `commit_index`: elections and membership
/// changes commit entries the state machine never sees.
#[test]
fn committed_application_index_ignores_noop_and_configuration_entries() {
    let mut runtime = durable_node_with_log(
        1,
        &[],
        InMemoryRaftHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
    );

    let _ = runtime.step(RaftInput::Tick).expect("single node elects");
    oracle_assert_eq!(runtime.commit_index(), LogIndex(1));
    oracle_assert_eq!(
        runtime.committed_application_index(),
        LogIndex::ZERO,
        "a committed leadership noop is not an application entry"
    );

    let _ = runtime
        .step(RaftInput::ClientProposal {
            payload: b"one".to_vec(),
        })
        .expect("application entry commits");
    oracle_assert_eq!(runtime.commit_index(), LogIndex(2));
    oracle_assert_eq!(runtime.committed_application_index(), LogIndex(2));

    let _ = runtime
        .step(RaftInput::AddLearner {
            learner_id: RaftNodeId(2),
        })
        .expect("configuration entry commits");
    oracle_assert_eq!(runtime.commit_index(), LogIndex(3));
    oracle_assert_eq!(
        runtime.committed_application_index(),
        LogIndex(2),
        "a caught-up state machine trails the committed index forever here"
    );
}

/// A snapshot subsumes every application entry it covers, so the boundary is a
/// floor — and compaction above the last committed application entry raises the
/// value in one jump, which is still non-decreasing.
#[test]
fn committed_application_index_uses_the_snapshot_boundary_after_compaction() {
    let mut runtime = durable_node_with_log(
        1,
        &[],
        InMemoryRaftHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
    );
    let _ = runtime.step(RaftInput::Tick).expect("single node elects");
    let _ = runtime
        .step(RaftInput::ClientProposal {
            payload: b"one".to_vec(),
        })
        .expect("application entry commits");
    let _ = runtime
        .step(RaftInput::AddLearner {
            learner_id: RaftNodeId(2),
        })
        .expect("configuration entry commits");
    oracle_assert_eq!(runtime.committed_application_index(), LogIndex(2));

    runtime
        .compact_log_with_snapshot(raft_snapshot(3, 1, 1, b"payload"))
        .expect("local snapshot compacts the log");

    oracle_assert_eq!(runtime.snapshot_index(), LogIndex(3));
    oracle_assert_eq!(
        runtime.committed_application_index(),
        LogIndex(3),
        "the snapshot covers the application entry the compacted log no longer holds"
    );
}

/// A group that has only ever elected and reconfigured has nothing for a state
/// machine to catch up to — and is the backward scan's worst case, since no
/// application entry ends it early.
#[test]
fn committed_application_index_is_zero_on_a_node_with_no_application_entries() {
    let fresh = durable_node_with_log(
        1,
        &[2],
        InMemoryRaftHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
    );
    oracle_assert_eq!(fresh.committed_application_index(), LogIndex::ZERO);
    drop(fresh);

    let mut hard_state_store = InMemoryRaftHardStateStore::new();
    hard_state_store
        .write_hard_state(RaftHardState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex(2),
            committed_configuration: None,
        })
        .expect("hard state writes");
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[
            PersistedRaftLogEntry::noop(LogIndex(1), Term(1)),
            PersistedRaftLogEntry::configuration(
                LogIndex(2),
                Term(1),
                learner_configuration_entry(ConfigurationId(1)),
            ),
        ])
        .expect("noop and configuration entries persist");

    let recovered = durable_node_with_log(1, &[2], hard_state_store, log_segment);

    oracle_assert_eq!(recovered.commit_index(), LogIndex(2));
    oracle_assert_eq!(recovered.committed_application_index(), LogIndex::ZERO);
}

/// Decomposition is the in-process half of restart: the stores that come back
/// must recover the same node the retired incarnation was.
#[test]
fn into_storage_returns_stores_that_recover_to_the_same_state() {
    let mut runtime = durable_node_with_log(
        1,
        &[2],
        hard_state_store(0, None),
        InMemoryRaftLogSegment::new(),
    );
    elect_runtime_leader_with_grant(&mut runtime, RaftNodeId(2));
    let _ = propose_and_ack_runtime_entry(&mut runtime, RaftNodeId(2), LogIndex(2), b"before");
    let expected_hard_state = runtime.hard_state_store.current();
    let expected_entries = runtime.log_segment.replay_entries();
    let expected_snapshot_index = runtime.snapshot_index();
    let expected_commit_index = runtime.commit_index();

    let storage = runtime.into_storage();

    oracle_assert_eq!(storage.hard_state_store.current(), expected_hard_state);
    oracle_assert_eq!(storage.log_segment.replay_entries(), expected_entries);
    let recovered = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(1, &[2]),
        storage.hard_state_store,
        storage.log_segment,
        storage.snapshot_store,
    )
    .expect("returned stores recover a node");
    oracle_assert_eq!(recovered.commit_index(), expected_commit_index);
    oracle_assert_eq!(recovered.snapshot_index(), expected_snapshot_index);
    oracle_assert_eq!(recovered.last_log_index(), LogIndex(2));
    oracle_assert_eq!(
        recovered.log_entries_from(LogIndex(1)),
        vec![
            LogEntry::noop(Term(1)),
            LogEntry::application(Term(1), b"before".to_vec()),
        ]
    );
}

/// Poison is the state a caller most needs to leave, so decomposition is
/// allowed there and returns the medium rather than the in-memory state that
/// ran ahead of it.
#[test]
fn into_storage_after_a_fatal_persistence_error_returns_the_durable_stores() {
    let mut runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(1, &[]),
        hard_state_store(0, None),
        InMemoryRaftLogSegment::new(),
        FailingSnapshotStore,
    )
    .expect("single-voter node hydrates");
    runtime.step(RaftInput::Tick).expect("single node elects");
    let durable_before = runtime.log_segment.replay_entries();

    let error = runtime
        .compact_log_with_snapshot(raft_snapshot(1, 1, 1, b"payload"))
        .expect_err("the snapshot store refuses every write");
    oracle_assert!(matches!(error, RaftRuntimeError::SnapshotWrite(_)));
    assert_poisoned_after_failure(&mut runtime, |cause| {
        matches!(cause, RaftRuntimeFatalError::SnapshotWrite(_))
    });

    let storage = runtime.into_storage();

    // The medium never took the snapshot, so what comes back is the
    // pre-failure node rather than the boundary the poisoned runtime implied.
    oracle_assert_eq!(storage.log_segment.replay_entries(), durable_before);
    oracle_assert_eq!(storage.snapshot_store.current_snapshot(), None);
    let recovered = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(1, &[]),
        storage.hard_state_store,
        storage.log_segment,
        InMemoryRaftSnapshotStore::new(),
    )
    .expect("poisoned runtime's stores still recover");
    oracle_assert_eq!(recovered.snapshot_index(), LogIndex::ZERO);
    oracle_assert_eq!(recovered.last_log_index(), LogIndex(1));
}

/// Decomposition writes nothing: it neither flushes nor closes, and a caller
/// reopening the same medium is responsible for dropping the returned handles
/// first.
#[test]
fn into_storage_does_not_flush_or_close() {
    let journal = StoreJournal::new();
    let mut runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(1, &[]),
        RecordingHardStateStore::new(&journal, hard_state_store(0, None)),
        RecordingLogSegment::new(&journal, InMemoryRaftLogSegment::new()),
        RecordingSnapshotStore::new(&journal, InMemoryRaftSnapshotStore::new()),
    )
    .expect("single-voter node hydrates");
    runtime.step(RaftInput::Tick).expect("single node elects");
    // Positive control: the journal does observe what a step writes, so an
    // empty delta below means "wrote nothing", not "watched nothing".
    oracle_assert!(!journal.is_empty());
    let observed_while_live = journal.entries();

    let storage = runtime.into_storage();

    oracle_assert_eq!(journal.entries(), observed_while_live);
    drop(storage);
    oracle_assert_eq!(journal.entries(), observed_while_live);
}
