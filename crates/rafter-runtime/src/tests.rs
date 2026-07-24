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
mod file_backed_fixture;
mod group_commit;
mod hard_state;
mod local_ids;
mod persistence_contract;
mod persistence_ordering;
mod recording_stores;
mod recovery;
mod replay_recovery;
mod snapshot;

use recovery::{dynamic_membership_recovery_fixture, elect_runtime_leader_with_grant};

fn membership_set(voters: &[u64]) -> MembershipSet {
    MembershipSet::new(voters.iter().copied().map(RaftNodeId).collect(), Vec::new())
        .expect("membership is valid")
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
            source: std::io::Error::other("injected failure").into(),
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
            source: std::io::Error::other("injected failure").into(),
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
