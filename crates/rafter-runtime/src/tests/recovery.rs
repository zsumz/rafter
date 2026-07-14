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
