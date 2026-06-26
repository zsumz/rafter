use super::*;
use rafter::{
    AppendEntries, AppendEntriesResponse, ApplicationSnapshotKind, ApplicationSnapshotMetadata,
    ApplicationSnapshotVersion, CommittedConfiguration, ConfigurationEntry, ConfigurationId,
    JointMembership, LocalProposalId, MembershipConfig, MembershipSet, Message,
    NodeConfig as RaftNodeConfig, PreVoteResponse, ReadId, RequestVoteResponse, SnapshotGroupId,
};
use std::path::PathBuf;

mod conflict_repair;
mod crash_window;
mod group_commit;
mod hard_state;
mod local_ids;
mod snapshot;

#[test]
fn leader_proposal_log_entry_is_persisted_before_apply_output_escapes() {
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

    let outputs = runtime
        .step(RaftInput::ClientProposal {
            payload: b"create".to_vec(),
        })
        .expect("log entry persists");

    assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![
            PersistedRaftLogEntry::noop(LogIndex(1), Term(1)),
            PersistedRaftLogEntry::application(LogIndex(2), Term(1), b"create".to_vec(),)
        ]
    );
    assert_eq!(
        outputs,
        vec![RaftOutput::Apply {
            index: LogIndex(2),
            term: Term(1),
            payload: b"create".to_vec().into(),
            local_proposal_id: None,
        }]
    );
}

#[test]
fn follower_append_entries_are_persisted_before_success_response_escapes() {
    let mut runtime = durable_node_with_log(
        2,
        &[1, 3],
        InMemoryRaftHardStateStore::new(),
        InMemoryRaftLogSegment::new(),
    );

    let outputs = runtime
        .step(RaftInput::Message {
            from: RaftNodeId(1),
            message: Message::AppendEntries(AppendEntries {
                sequence: 0,
                term: Term(2),
                leader_id: RaftNodeId(1),
                prev_log_index: LogIndex::ZERO,
                prev_log_term: Term::default(),
                entries: vec![LogEntry::application(Term(2), b"append".to_vec())],
                leader_commit: LogIndex::ZERO,
            }),
        })
        .expect("log entry persists");

    assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![PersistedRaftLogEntry::application(
            LogIndex(1),
            Term(2),
            b"append".to_vec(),
        )]
    );
    assert!(matches!(
        outputs.as_slice(),
        [RaftOutput::Send {
            message: Message::AppendEntriesResponse(response),
            ..
        }] if response.success && response.match_index == LogIndex(1)
    ));
}

#[test]
fn configuration_proposal_log_entry_is_persisted_before_outputs_escape() {
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

    let outputs = runtime
        .step(RaftInput::AddLearner {
            learner_id: RaftNodeId(2),
        })
        .expect("configuration entry persists");

    assert_eq!(
        runtime.log_segment.replay_entries(),
        vec![
            PersistedRaftLogEntry::noop(LogIndex(1), Term(1)),
            PersistedRaftLogEntry::configuration(LogIndex(2), Term(1), configuration.clone(),)
        ]
    );
    assert_eq!(
        runtime.effective_configuration_entry(),
        Some(configuration.clone())
    );
    assert_eq!(runtime.committed_configuration_entry(), Some(configuration));
    assert!(!outputs
        .iter()
        .any(|output| matches!(output, RaftOutput::Apply { .. })));
}

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

    assert_eq!(restarted.commit_index(), LogIndex(16));
    assert_eq!(restarted.committed_configuration_entry(), Some(stable));
    assert_eq!(
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

    assert_eq!(restarted.commit_index(), LogIndex(18));
    assert!(outputs.iter().any(|output| matches!(
        output,
        RaftOutput::Apply {
            index: LogIndex(18),
            payload,
            ..
        } if payload.as_ref() == b"after-restart"
    )));
}

fn dynamic_membership_recovery_fixture() -> (
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

fn elect_runtime_leader_with_grant<H, L, S>(
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

fn membership_set(voters: &[u64]) -> MembershipSet {
    MembershipSet::new(voters.iter().copied().map(RaftNodeId).collect(), Vec::new())
        .expect("membership is valid")
}

#[test]
fn log_append_failure_suppresses_apply_outputs() {
    let mut runtime = durable_node_with_log(
        1,
        &[],
        InMemoryRaftHardStateStore::new(),
        FailingAfterElectionNoopLogSegment::new(),
    );
    assert!(runtime
        .step(RaftInput::Tick)
        .expect("leader elected")
        .is_empty());

    let error = runtime
        .step(RaftInput::ClientProposal {
            payload: b"create".to_vec(),
        })
        .expect_err("log append fails");

    assert!(matches!(
        error,
        RaftRuntimeError::LogAppend(RaftLogSegmentAppendError::Io {
            operation: "append test raft log entries",
            ..
        })
    ));
    // Poisoned accessors may run ahead of durability; the contract is that
    // nothing durable recorded the entry and no output ever will.
    let error = runtime
        .step(RaftInput::Tick)
        .expect_err("a poisoned runtime refuses further inputs");
    assert!(matches!(error, RaftRuntimeError::Poisoned { .. }));
}

#[test]
fn log_append_failure_poisons_runtime_until_restart() {
    let mut runtime = durable_node_with_log(
        1,
        &[],
        InMemoryRaftHardStateStore::new(),
        FailingAfterElectionNoopLogSegment::new(),
    );
    assert!(runtime
        .step(RaftInput::Tick)
        .expect("leader elected")
        .is_empty());

    let error = runtime
        .step(RaftInput::ClientProposal {
            payload: b"create".to_vec(),
        })
        .expect_err("log append fails");
    assert!(matches!(error, RaftRuntimeError::LogAppend(_)));

    assert_poisoned_after_failure(&mut runtime, |cause| {
        matches!(cause, RaftRuntimeFatalError::LogAppend(_))
    });
}

#[test]
fn restarted_node_recovers_persisted_log_entries() {
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
    let _ = runtime
        .step(RaftInput::ClientProposal {
            payload: b"create".to_vec(),
        })
        .expect("log entry persists");
    let hard_state_store = runtime.hard_state_store.clone();
    let log_segment = runtime.log_segment.clone();

    let restarted = durable_node_with_log(1, &[], hard_state_store, log_segment);

    assert_eq!(restarted.last_log_index(), LogIndex(2));
    assert_eq!(
        restarted.log_entries_from(LogIndex(1)),
        vec![
            LogEntry::noop(Term(1)),
            LogEntry::application(Term(1), b"create".to_vec())
        ]
    );
}

#[test]
fn restarted_runtime_drains_committed_entries_above_applied_floor_immediately() {
    let mut hard_state_store = InMemoryRaftHardStateStore::new();
    hard_state_store
        .write_hard_state(RaftHardState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex(3),
            committed_configuration: None,
        })
        .expect("hard state writes");
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[
            PersistedRaftLogEntry::application(LogIndex(1), Term(1), b"one".to_vec()),
            PersistedRaftLogEntry::application(LogIndex(2), Term(1), b"two".to_vec()),
            PersistedRaftLogEntry::application(LogIndex(3), Term(1), b"three".to_vec()),
        ])
        .expect("committed log writes");

    let mut runtime = DurableRaftNode::with_storage_and_snapshot_store_applied_through(
        raft_config(1, &[2, 3]),
        hard_state_store,
        log_segment,
        InMemoryRaftSnapshotStore::new(),
        LogIndex(1),
    )
    .expect("runtime restarts with an applied floor");

    assert_eq!(
        runtime.drain_committed_outputs(),
        vec![
            RaftOutput::Apply {
                index: LogIndex(2),
                term: Term(1),
                payload: b"two".to_vec().into(),
                local_proposal_id: None,
            },
            RaftOutput::Apply {
                index: LogIndex(3),
                term: Term(1),
                payload: b"three".to_vec().into(),
                local_proposal_id: None,
            },
        ],
        "recovery drain emits committed-but-unapplied entries without another Raft message"
    );
    assert!(runtime.drain_committed_outputs().is_empty());
}

#[test]
fn recovery_constructor_returns_committed_entries_above_applied_floor() {
    let mut hard_state_store = InMemoryRaftHardStateStore::new();
    hard_state_store
        .write_hard_state(RaftHardState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex(3),
            committed_configuration: None,
        })
        .expect("hard state writes");
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[
            PersistedRaftLogEntry::application(LogIndex(1), Term(1), b"one".to_vec()),
            PersistedRaftLogEntry::application(LogIndex(2), Term(1), b"two".to_vec()),
            PersistedRaftLogEntry::application(LogIndex(3), Term(1), b"three".to_vec()),
        ])
        .expect("committed log writes");

    let recovered = DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
        raft_config(1, &[2, 3]),
        hard_state_store,
        log_segment,
        InMemoryRaftSnapshotStore::new(),
        LogIndex(1),
    )
    .expect("runtime recovers with explicit outputs");
    let (mut runtime, recovery_outputs) = recovered.into_parts();

    assert_eq!(
        recovery_outputs,
        vec![
            RaftOutput::Apply {
                index: LogIndex(2),
                term: Term(1),
                payload: b"two".to_vec().into(),
                local_proposal_id: None,
            },
            RaftOutput::Apply {
                index: LogIndex(3),
                term: Term(1),
                payload: b"three".to_vec().into(),
                local_proposal_id: None,
            },
        ],
        "recovery constructor returns committed-but-unapplied entries without another Raft message"
    );
    assert!(runtime.drain_committed_outputs().is_empty());
}

#[test]
fn restart_after_leadership_noop_commit_replays_unapplied_prior_entry() {
    let mut hard_state_store = InMemoryRaftHardStateStore::new();
    hard_state_store
        .write_hard_state(RaftHardState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
        })
        .expect("hard state writes");
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[PersistedRaftLogEntry::application(
            LogIndex(1),
            Term(1),
            b"old-entry".to_vec(),
        )])
        .expect("prior uncommitted entry persists");

    let mut runtime =
        DurableRaftNode::with_storage(raft_config(1, &[]), hard_state_store, log_segment)
            .expect("runtime starts from prior entry");
    let outputs = runtime.step(RaftInput::Tick).expect("leader noop commits");
    assert_eq!(
        outputs,
        vec![RaftOutput::Apply {
            index: LogIndex(1),
            term: Term(1),
            payload: b"old-entry".to_vec().into(),
            local_proposal_id: None,
        }]
    );
    assert_eq!(runtime.commit_index(), LogIndex(2));

    let hard_state_store = runtime.hard_state_store.clone();
    let log_segment = runtime.log_segment.clone();
    let recovered = DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
        raft_config(1, &[]),
        hard_state_store,
        log_segment,
        InMemoryRaftSnapshotStore::new(),
        LogIndex::ZERO,
    )
    .expect("runtime recovers after crash before app apply");
    let (_runtime, recovery_outputs) = recovered.into_parts();

    assert_eq!(
        recovery_outputs,
        vec![RaftOutput::Apply {
            index: LogIndex(1),
            term: Term(1),
            payload: b"old-entry".to_vec().into(),
            local_proposal_id: None,
        }],
        "the application entry is replayed after a crash before state-machine apply"
    );
}

fn assert_poisoned_after_failure<H, L, S, F>(
    runtime: &mut DurableRaftNode<H, L, S>,
    matches_cause: F,
) where
    H: RaftHardStateStore,
    L: RaftLogSegment,
    S: RaftSnapshotStore + SnapshotChunkSource,
    F: Fn(&RaftRuntimeFatalError) -> bool,
{
    for input in post_failure_inputs() {
        let error = runtime
            .step(input)
            .expect_err("poisoned runtime rejects all later inputs");

        assert!(matches!(
            error,
            RaftRuntimeError::Poisoned { cause } if matches_cause(&cause)
        ));
    }
}

fn post_failure_inputs() -> Vec<RaftInput> {
    vec![
        RaftInput::Tick,
        RaftInput::ClientProposal {
            payload: b"after-failure".to_vec(),
        },
        RaftInput::Message {
            from: RaftNodeId(2),
            message: Message::RequestVoteResponse(RequestVoteResponse {
                term: Term(1),
                voter_id: RaftNodeId(2),
                vote_granted: true,
            }),
        },
    ]
}

fn durable_node<H: RaftHardStateStore>(
    id: u64,
    peers: &[u64],
    hard_state_store: H,
) -> DurableRaftNode<H> {
    DurableRaftNode::new(
        RaftNodeConfig::new(
            RaftNodeId(id),
            peers.iter().copied().map(RaftNodeId).collect(),
            1,
        )
        .expect("test Raft node config is valid"),
        hard_state_store,
    )
    .expect("node hydrates")
}

fn durable_node_with_log<H: RaftHardStateStore, L: RaftLogSegment>(
    id: u64,
    peers: &[u64],
    hard_state_store: H,
    log_segment: L,
) -> DurableRaftNode<H, L> {
    DurableRaftNode::with_storage(raft_config(id, peers), hard_state_store, log_segment)
        .expect("node hydrates")
}

fn raft_config(id: u64, peers: &[u64]) -> RaftNodeConfig {
    RaftNodeConfig::new(
        RaftNodeId(id),
        peers.iter().copied().map(RaftNodeId).collect(),
        1,
    )
    .expect("test Raft node config is valid")
}

fn learner_configuration_entry(config_id: ConfigurationId) -> ConfigurationEntry {
    ConfigurationEntry::stable(
        config_id,
        MembershipSet::new(vec![RaftNodeId(1)], vec![RaftNodeId(2)]).expect("membership is valid"),
    )
}

fn hard_state_store(term: u64, voted_for: Option<u64>) -> InMemoryRaftHardStateStore {
    let mut store = InMemoryRaftHardStateStore::new();
    store
        .write_hard_state(RaftHardState {
            current_term: Term(term),
            voted_for: voted_for.map(RaftNodeId),
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
        })
        .expect("in-memory hard state writes");
    store
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FailingHardStateStore {
    current: RaftHardState,
}

impl RaftHardStateStore for FailingHardStateStore {
    fn write_hard_state(
        &mut self,
        _state: RaftHardState,
    ) -> Result<(), RaftHardStateStoreWriteError> {
        Err(RaftHardStateStoreWriteError::Io {
            operation: "write test raft hard state",
            path: PathBuf::from("test-hard-state"),
            message: "injected failure".to_string(),
        })
    }

    fn current(&self) -> RaftHardState {
        self.current
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FailingAfterElectionNoopLogSegment {
    inner: InMemoryRaftLogSegment,
    allowed: u32,
}

impl FailingAfterElectionNoopLogSegment {
    fn new() -> Self {
        Self {
            inner: InMemoryRaftLogSegment::new(),
            allowed: 1,
        }
    }
}

impl RaftLogSegment for FailingAfterElectionNoopLogSegment {
    fn append_entries(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), RaftLogSegmentAppendError> {
        if entries.is_empty() {
            return self.inner.append_entries(entries);
        }
        if self.allowed > 0 {
            self.allowed -= 1;
            return self.inner.append_entries(entries);
        }
        Err(RaftLogSegmentAppendError::Io {
            operation: "append test raft log entries",
            message: "injected failure".to_string(),
        })
    }

    fn truncate_suffix(&mut self, from_index: LogIndex) -> Result<(), RaftLogSegmentTruncateError> {
        self.inner.truncate_suffix(from_index)
    }

    fn compact_prefix_through(
        &mut self,
        through_index: LogIndex,
    ) -> Result<(), RaftLogSegmentCompactError> {
        self.inner.compact_prefix_through(through_index)
    }

    fn replay_entries(&self) -> Vec<PersistedRaftLogEntry> {
        self.inner.replay_entries()
    }

    fn next_index(&self) -> LogIndex {
        self.inner.next_index()
    }

    fn compacted_through(&self) -> LogIndex {
        self.inner.compacted_through()
    }
}
