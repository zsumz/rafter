//! Rafter service-layer harness: tracked writes through `InMemoryRaftDriver`.
//!
//! This is intentionally separate from the cross-library protocol comparison:
//! it measures whether Rafter's own app/service stack preserves explicit write
//! batches down to the persisted runtime boundary.

use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Instant;

use bench_compare::{
    payload_of_size, report_json, ServiceShapeMetrics, WorkloadMetrics, PAYLOAD_BYTES,
    SERVICE_TRACKED_PROPOSALS, SERVICE_WRITE_BATCH_DEPTH,
};
use rafter::{
    ClientProposalInput, Input as RaftInput, LogIndex, MembershipConfig, NodeConfig, NodeId,
    Output as RaftOutput, ReplicationProgress, Role, Term,
};
use rafter_app::group::RaftGroup;
use rafter_app::state_machine::{
    ApplicationSnapshot, ApplyBatch, ApplyResult, ReadBarrier, ReplicatedStateMachine,
};
use rafter_runtime::{DurableRaftNode, PersistedRaftRuntime, RaftRuntimeError};
use rafter_service::{InMemoryRaftDriver, WriteBatchEntry};
use rafter_storage::InMemoryRaftHardStateStore;

type BenchGroup = RaftGroup<(), BenchStateMachine, RecordingRuntime>;
type BenchDriver = InMemoryRaftDriver<(), BenchStateMachine, RecordingRuntime>;

fn main() {
    let tracked_write = tracked_write_workload();
    println!(
        "{}",
        report_json(
            "rafter-service",
            "path:../crates (workspace @ HEAD)",
            "write_batch submitted to in-memory service -> every tracked write applies",
            &[tracked_write],
        )
    );
}

fn tracked_write_workload() -> WorkloadMetrics {
    let stats = Arc::new(Mutex::new(RuntimeBatchStats::default()));
    let driver = BenchDriver::new_elected(NodeId(1), groups(stats.clone()))
        .expect("primary elects for service benchmark");

    let mut latencies = Vec::with_capacity(SERVICE_TRACKED_PROPOSALS);
    let mut service_shape = ServiceShapeMetrics::default();
    let started = Instant::now();

    let mut remaining = SERVICE_TRACKED_PROPOSALS;
    while remaining > 0 {
        let batch = remaining.min(SERVICE_WRITE_BATCH_DEPTH);
        remaining -= batch;
        service_shape.write_batches += 1;
        let writes: Vec<_> = (0..batch)
            .map(|_| WriteBatchEntry::new(payload_of_size(PAYLOAD_BYTES)))
            .collect();
        let submitted = Instant::now();
        let outcomes = block_on(driver.write_batch((), writes));
        assert_eq!(
            outcomes.len(),
            batch,
            "service returns one result per write"
        );
        for outcome in outcomes {
            outcome.expect("tracked write applies");
            latencies.push(submitted.elapsed());
            service_shape.applied_writes += 1;
        }
    }
    let elapsed = started.elapsed();

    let stats = stats.lock().expect("runtime stats lock");
    service_shape.runtime_step_batches = stats.runtime_step_batches;
    service_shape.tracked_proposals = stats.tracked_proposals;

    WorkloadMetrics {
        name: "tracked_write",
        proposals: SERVICE_TRACKED_PROPOSALS,
        payload_bytes: PAYLOAD_BYTES,
        max_in_flight: SERVICE_WRITE_BATCH_DEPTH,
        elapsed,
        latencies,
        shape: None,
        service_shape: Some(service_shape),
        read_shape: None,
        codec_shape: None,
        multiraft_shape: None,
        failover_shape: None,
    }
}

fn groups(stats: Arc<Mutex<RuntimeBatchStats>>) -> Vec<BenchGroup> {
    vec![
        group(1, &[2, 3], 3, stats.clone()),
        group(2, &[1, 3], 9, stats.clone()),
        group(3, &[1, 2], 9, stats),
    ]
}

fn group(
    id: u64,
    peers: &[u64],
    election_timeout_ticks: u64,
    stats: Arc<Mutex<RuntimeBatchStats>>,
) -> BenchGroup {
    let config = NodeConfig::new(
        NodeId(id),
        peers.iter().copied().map(NodeId).collect(),
        election_timeout_ticks,
    )
    .expect("bench node config is valid");
    let runtime = RecordingRuntime {
        inner: DurableRaftNode::new(config, InMemoryRaftHardStateStore::new())
            .expect("in-memory durable node opens"),
        stats,
    };
    RaftGroup::new((), NodeId(id), runtime, BenchStateMachine::default())
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

fn block_on<F: Future>(future: F) -> F::Output {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
