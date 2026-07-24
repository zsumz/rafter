//! Rafter MultiRaft harness: many real groups stepped in round-robin batches.
//!
//! This is Rafter-only evidence for preserving explicit proposal batches across
//! the app and many-group host boundary. Each group is a single-voter
//! in-memory RaftGroup, elected before measurement, and the workload submits
//! bounded proposal batches in deterministic group-id order.

use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bench_compare::{
    payload_of_size, report_json, MultiRaftShapeMetrics, WorkloadMetrics, MULTIRAFT_BATCH_DEPTH,
    MULTIRAFT_GROUPS, MULTIRAFT_ROUNDS, PAYLOAD_BYTES,
};
use rafter::{
    ClientProposalInput, Input as RaftInput, LocalProposalId, LogIndex, MembershipConfig,
    NodeConfig, NodeId, Output as RaftOutput, ReplicationProgress, Role, Term,
};
use rafter_app::{
    group::{GroupInput, RaftGroup},
    proposal::Proposal,
    state_machine::{
        ApplicationSnapshot, ApplyBatch, ApplyResult, ReadBarrier, ReplicatedStateMachine,
    },
};
use rafter_multiraft::TypedMultiRaftHost;
use rafter_runtime::{DurableRaftNode, PersistedRaftRuntime, RaftRuntimeError};
use rafter_storage::InMemoryRaftHardStateStore;

type BenchGroup = RaftGroup<u64, BenchStateMachine, RecordingRuntime>;
type BenchHost = TypedMultiRaftHost<u64, Vec<u8>, ()>;

fn main() {
    let round_robin = round_robin_batches();
    println!(
        "{}",
        report_json(
            "rafter-multiraft",
            "path:../crates (workspace @ HEAD)",
            "proposal batch submitted through TypedMultiRaftHost -> every tracked write applies",
            &[round_robin],
        )
    );
}

fn round_robin_batches() -> WorkloadMetrics {
    let total_proposals = MULTIRAFT_GROUPS
        .saturating_mul(MULTIRAFT_ROUNDS)
        .saturating_mul(MULTIRAFT_BATCH_DEPTH);
    let stats = Arc::new(Mutex::new(RuntimeBatchStats::default()));
    let mut host = BenchHost::new();
    let mut next_proposal_ids = Vec::with_capacity(MULTIRAFT_GROUPS);

    for group_id in group_ids() {
        host.open_group(group_id, group(group_id, stats.clone()))
            .expect("open multiraft group");
        let report = host
            .step_group(&group_id, GroupInput::Tick)
            .expect("single-voter group elects");
        assert!(report.peer_messages.is_empty());
        next_proposal_ids.push(LocalProposalId(1));
    }

    let mut latencies = Vec::with_capacity(total_proposals);
    let mut multiraft_shape = MultiRaftShapeMetrics {
        groups: MULTIRAFT_GROUPS,
        rounds: MULTIRAFT_ROUNDS,
        ..MultiRaftShapeMetrics::default()
    };
    let started = Instant::now();

    for _ in 0..MULTIRAFT_ROUNDS {
        for (slot, group_id) in group_ids().enumerate() {
            multiraft_shape.group_batches += 1;
            let submitted = Instant::now();
            let proposals = proposal_batch(&mut next_proposal_ids[slot]);
            let report = host
                .step_group(&group_id, GroupInput::ProposalBatch { proposals })
                .expect("proposal batch applies");
            assert_eq!(
                report.applied.len(),
                MULTIRAFT_BATCH_DEPTH,
                "single-voter group applies the whole submitted batch"
            );
            multiraft_shape.applied_proposals += report.applied.len();
            for _ in 0..report.applied.len() {
                latencies.push(submitted.elapsed());
            }
        }
    }
    let elapsed = started.elapsed();

    let stats = stats.lock().expect("runtime stats lock");
    multiraft_shape.runtime_step_batches = stats.runtime_step_batches;
    multiraft_shape.tracked_proposals = stats.tracked_proposals;
    assert_eq!(multiraft_shape.applied_proposals, total_proposals);
    assert_eq!(multiraft_shape.tracked_proposals, total_proposals);

    WorkloadMetrics {
        name: "round_robin_batches",
        proposals: total_proposals,
        payload_bytes: PAYLOAD_BYTES,
        max_in_flight: MULTIRAFT_BATCH_DEPTH,
        elapsed,
        latencies,
        shape: None,
        service_shape: None,
        read_shape: None,
        codec_shape: None,
        multiraft_shape: Some(multiraft_shape),
        failover_shape: None,
    }
}

fn group_ids() -> impl Iterator<Item = u64> {
    (1..=MULTIRAFT_GROUPS).map(|id| id as u64)
}

fn proposal_batch(next_proposal_id: &mut LocalProposalId) -> Vec<Proposal<Vec<u8>>> {
    (0..MULTIRAFT_BATCH_DEPTH)
        .map(|_| {
            let local_proposal_id = *next_proposal_id;
            *next_proposal_id = LocalProposalId(next_proposal_id.0 + 1);
            Proposal {
                local_proposal_id,
                client_request_id: None,
                command: payload_of_size(PAYLOAD_BYTES),
            }
        })
        .collect()
}

fn group(group_id: u64, stats: Arc<Mutex<RuntimeBatchStats>>) -> BenchGroup {
    let config = NodeConfig::new(NodeId(1), Vec::new(), 1).expect("bench node config is valid");
    let runtime = RecordingRuntime {
        inner: DurableRaftNode::new(config, InMemoryRaftHardStateStore::new())
            .expect("in-memory durable node opens"),
        stats,
    };
    RaftGroup::new(group_id, NodeId(1), runtime, BenchStateMachine::default())
}

#[derive(Debug, Default)]
struct BenchStateMachine {
    applied_index: LogIndex,
}

impl ReplicatedStateMachine for BenchStateMachine {
    type Command = Vec<u8>;
    type CommandResult = ();
    type Query = ();
    type QueryResult = ();
    type Error = BenchStateMachineError;

    fn applied_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.applied_index)
    }

    fn encode_command(&self, command: &Self::Command) -> Result<Vec<u8>, Self::Error> {
        Ok(command.clone())
    }

    fn decode_command(&self, payload: &[u8]) -> Result<Self::Command, Self::Error> {
        Ok(payload.to_vec())
    }

    fn apply_batch(
        &mut self,
        batch: ApplyBatch<Self::Command>,
    ) -> Result<Vec<ApplyResult<Self::CommandResult>>, Self::Error> {
        let mut results = Vec::with_capacity(batch.entries.len());
        for entry in batch.entries {
            self.applied_index = entry.index;
            results.push(ApplyResult {
                index: entry.index,
                term: entry.term,
                result: (),
                local_proposal_id: entry.local_proposal_id,
            });
        }
        Ok(results)
    }

    fn read(
        &self,
        _query: Self::Query,
        _barrier: ReadBarrier,
    ) -> Result<Self::QueryResult, Self::Error> {
        Ok(())
    }

    fn build_snapshot(&mut self, at: LogIndex) -> Result<ApplicationSnapshot, Self::Error> {
        Ok(ApplicationSnapshot {
            applied_index: at,
            payload: Vec::new(),
            raft_snapshot: None,
        })
    }

    fn install_snapshot(&mut self, snapshot: ApplicationSnapshot) -> Result<(), Self::Error> {
        self.applied_index = snapshot.applied_index;
        Ok(())
    }
}

#[derive(Debug)]
struct BenchStateMachineError;

impl fmt::Display for BenchStateMachineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("bench state machine error")
    }
}

impl std::error::Error for BenchStateMachineError {}

#[derive(Debug, Default)]
struct RuntimeBatchStats {
    runtime_step_batches: usize,
    tracked_proposals: usize,
}

#[derive(Debug)]
struct RecordingRuntime {
    inner: DurableRaftNode,
    stats: Arc<Mutex<RuntimeBatchStats>>,
}

impl PersistedRaftRuntime for RecordingRuntime {
    type Error = RaftRuntimeError;

    fn id(&self) -> NodeId {
        self.inner.id()
    }

    fn leader_hint(&self) -> Option<NodeId> {
        self.inner.leader_hint()
    }

    fn role(&self) -> Role {
        self.inner.role()
    }

    fn current_term(&self) -> Term {
        self.inner.current_term()
    }

    fn commit_index(&self) -> LogIndex {
        self.inner.commit_index()
    }

    fn last_log_index(&self) -> LogIndex {
        self.inner.last_log_index()
    }

    fn snapshot_index(&self) -> LogIndex {
        self.inner.snapshot_index()
    }

    fn committed_application_index(&self) -> LogIndex {
        self.inner.committed_application_index()
    }

    fn membership(&self) -> MembershipConfig {
        self.inner.membership()
    }

    fn committed_membership(&self) -> MembershipConfig {
        self.inner.committed_membership()
    }

    fn replication(&self) -> Vec<ReplicationProgress> {
        self.inner.replication()
    }

    fn step(&mut self, input: RaftInput) -> Result<Vec<RaftOutput>, Self::Error> {
        self.inner.step(input)
    }

    fn step_proposal_batch(
        &mut self,
        proposals: Vec<ClientProposalInput>,
    ) -> Result<Vec<RaftOutput>, Self::Error> {
        if !proposals.is_empty() {
            let mut stats = self.stats.lock().expect("runtime stats lock");
            stats.runtime_step_batches += 1;
            stats.tracked_proposals += proposals.len();
        }
        self.inner.step_proposal_batch(proposals)
    }

    fn step_batch(&mut self, inputs: Vec<RaftInput>) -> Result<Vec<RaftOutput>, Self::Error> {
        let tracked = inputs
            .iter()
            .filter(|input| matches!(input, RaftInput::TrackedClientProposal { .. }))
            .count();
        if tracked > 0 {
            let mut stats = self.stats.lock().expect("runtime stats lock");
            stats.runtime_step_batches += 1;
            stats.tracked_proposals += tracked;
        }
        self.inner.step_batch(inputs)
    }

    fn term_at_index(&self, index: LogIndex) -> Option<Term> {
        self.inner.term_at_index(index)
    }
}
