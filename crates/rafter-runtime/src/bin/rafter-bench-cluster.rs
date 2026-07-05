//! In-process three-node cluster benchmark over file-backed stores.
//!
//! Measures what the C-lane optimizations claim: proposal throughput and
//! commit latency with and without group commit (C4's fsync amortization in
//! wall-clock terms), and snapshot transfer throughput over the streaming
//! path (C1). Message passing is synchronous and in-process, so the numbers
//! isolate the protocol and durable-storage path rather than a network.
//!
//! Emits one machine-readable JSON report on stdout. Run with
//! `cargo run --release -p rafter-runtime --bin rafter-bench-cluster`.

// Metrics math casts counts and durations through f64; every value involved
// is far below the 52-bit mantissa and percentile ranks are bounded by the
// sample count, so the pedantic cast lints do not apply to this bench tool.
#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::{Duration, Instant};

use rafter::{
    Input as RaftInput, LogIndex, NodeConfig, NodeId, Output as RaftOutput, RaftSnapshotMetadata,
    Role,
};
use rafter_runtime::DurableRaftNode;
use rafter_storage::{
    FileRaftHardStateStore, FileRaftLogSegment, FileRaftNodeStores, FileRaftSnapshotStore,
    PersistedRaftSnapshot,
};

type BenchNode = DurableRaftNode<FileRaftHardStateStore, FileRaftLogSegment, FileRaftSnapshotStore>;

const PROPOSAL_COUNT: usize = 512;
const PROPOSAL_PAYLOAD_BYTES: usize = 256;
const GROUP_COMMIT_BATCH: usize = 32;
const SNAPSHOT_PAYLOAD_BYTES: usize = 32 * 1024 * 1024;

fn main() {
    let scratch = std::env::temp_dir().join(format!("rafter-bench-{}", std::process::id()));
    std::fs::create_dir_all(&scratch).expect("bench scratch directory is creatable");

    let unbatched = proposal_workload(&scratch.join("unbatched"), 1);
    let batched = proposal_workload(&scratch.join("batched"), GROUP_COMMIT_BATCH);
    let snapshot = snapshot_workload(&scratch.join("snapshot"));

    std::fs::remove_dir_all(&scratch).ok();

    println!("{}", report_json(&unbatched, &batched, &snapshot));
}

struct ProposalMetrics {
    batch_size: usize,
    proposals: usize,
    payload_bytes: usize,
    elapsed: Duration,
    latencies: Vec<Duration>,
}

struct SnapshotMetrics {
    payload_bytes: usize,
    elapsed: Duration,
}

/// Drives `PROPOSAL_COUNT` proposals through an elected leader, feeding
/// follower responses synchronously, submitting `batch_size` inputs per
/// `step_batch` call. Commit latency is measured from batch submission to
/// the leader's `Apply` for each proposal's index.
fn proposal_workload(directory: &std::path::Path, batch_size: usize) -> ProposalMetrics {
    let mut cluster = Cluster::elect(directory);

    let mut latencies: Vec<Duration> = Vec::with_capacity(PROPOSAL_COUNT);
    let mut submitted_at: BTreeMap<LogIndex, Instant> = BTreeMap::new();
    let mut next_index = cluster.leader().last_log_index();
    let started = Instant::now();

    let mut remaining = PROPOSAL_COUNT;
    while remaining > 0 {
        let batch = remaining.min(batch_size);
        remaining -= batch;
        let inputs: Vec<RaftInput> = (0..batch)
            .map(|_| RaftInput::ClientProposal {
                payload: vec![0xA5; PROPOSAL_PAYLOAD_BYTES],
            })
            .collect();
        let now = Instant::now();
        for _ in 0..batch {
            next_index = LogIndex(next_index.0 + 1);
            submitted_at.insert(next_index, now);
        }
        let outputs = cluster.step_leader_batch(inputs);
        cluster.pump(outputs, &mut |index| {
            if let Some(at) = submitted_at.remove(&index) {
                latencies.push(at.elapsed());
            }
        });
    }
    assert!(
        submitted_at.is_empty(),
        "every proposal commits and applies at the leader"
    );

    ProposalMetrics {
        batch_size,
        proposals: PROPOSAL_COUNT,
        payload_bytes: PROPOSAL_PAYLOAD_BYTES,
        elapsed: started.elapsed(),
        latencies,
    }
}

/// A leader with a large durable snapshot streams it to a follower that
/// never saw the compacted prefix; measures end-to-end transfer throughput
/// including the receiver's staged appends and promotion.
///
/// The lagging follower is held back from the boundary commit (its match
/// stays honestly at zero) rather than wiped after acknowledging: a
/// follower that loses acknowledged entries violates Raft's durability
/// assumption, and the leader's match floor deliberately refuses to
/// walk back below an acknowledgement.
fn snapshot_workload(directory: &std::path::Path) -> SnapshotMetrics {
    let mut cluster = Cluster::elect(directory);

    // Commit one entry so the boundary exists — with every message to the
    // lagging follower dropped, so the quorum is the leader plus node 2 —
    // then compact through it under a synthetic payload.
    let lagging = NodeId(3);
    cluster.drop_sends_to = Some(lagging);
    let outputs = cluster.step_leader_batch(vec![RaftInput::ClientProposal {
        payload: b"boundary".to_vec(),
    }]);
    cluster.pump(outputs, &mut |_| {});
    cluster.drop_sends_to = None;
    let leader = cluster.leader();
    let boundary = leader.commit_index();
    let term = leader.current_term();
    let metadata = RaftSnapshotMetadata::new(
        rafter::SnapshotGroupId::new("bench-group").expect("valid group id"),
        cluster.leader_id,
        boundary,
        term,
        term,
        rafter::ApplicationSnapshotMetadata::new(
            rafter::ApplicationSnapshotKind::new("bench_state").expect("valid kind"),
            rafter::ApplicationSnapshotVersion::new(1).expect("valid version"),
        ),
    )
    .expect("valid snapshot metadata");
    cluster
        .leader()
        .compact_log_with_snapshot(PersistedRaftSnapshot {
            metadata,
            application_payload: vec![0x5A; SNAPSHOT_PAYLOAD_BYTES],
        })
        .expect("leader compacts through its snapshot");

    let started = Instant::now();
    // Ticking the leader reaches the lagging follower, whose rejection
    // walks the leader to the snapshot path; pumping runs the chunked
    // transfer to installation.
    while cluster.node(lagging).snapshot_index() < boundary {
        let outputs = cluster.step_leader_batch(vec![RaftInput::Tick]);
        cluster.pump(outputs, &mut |_| {});
    }

    SnapshotMetrics {
        payload_bytes: SNAPSHOT_PAYLOAD_BYTES,
        elapsed: started.elapsed(),
    }
}

struct Cluster {
    nodes: BTreeMap<NodeId, BenchNode>,
    leader_id: NodeId,
    /// When set, sends to this node are dropped — holds a follower back so
    /// it lags legitimately (its acknowledgements never happen).
    drop_sends_to: Option<NodeId>,
}

impl Cluster {
    /// Three file-backed nodes under `directory`; node 1 is elected by
    /// scripted ticks and real vote traffic.
    fn elect(directory: &std::path::Path) -> Self {
        let ids = [NodeId(1), NodeId(2), NodeId(3)];
        let mut nodes = BTreeMap::new();
        for id in ids {
            nodes.insert(id, open_node(directory, id, &ids));
        }
        let mut cluster = Self {
            nodes,
            leader_id: NodeId(1),
            drop_sends_to: None,
        };

        // Node 1's election timeout fires first (jitter-free config, ticked
        // alone); the pre-vote poll and the vote requests it earns circulate
        // synchronously.
        let outputs = cluster
            .nodes
            .get_mut(&NodeId(1))
            .expect("node 1 exists")
            .step(RaftInput::Tick)
            .expect("election tick");
        cluster.pump_from(NodeId(1), outputs, &mut |_| {});
        assert_eq!(cluster.leader().role(), Role::Leader, "node 1 wins");
        cluster
    }

    fn leader(&mut self) -> &mut BenchNode {
        self.nodes.get_mut(&self.leader_id).expect("leader exists")
    }

    fn node(&mut self, id: NodeId) -> &mut BenchNode {
        self.nodes.get_mut(&id).expect("node exists")
    }

    fn step_leader_batch(&mut self, inputs: Vec<RaftInput>) -> Vec<(NodeId, RaftOutput)> {
        let leader_id = self.leader_id;
        let outputs = self
            .leader()
            .step_batch(inputs)
            .expect("leader batch persists");
        outputs
            .into_iter()
            .map(|output| (leader_id, output))
            .collect()
    }

    /// Synchronously routes messages until the cluster is quiescent,
    /// reporting every leader-side apply index to `on_leader_apply`.
    fn pump(
        &mut self,
        outputs: Vec<(NodeId, RaftOutput)>,
        on_leader_apply: &mut dyn FnMut(LogIndex),
    ) {
        let mut queue = outputs;
        while let Some((from, output)) = queue.pop() {
            match output {
                RaftOutput::Send { to, message } => {
                    if self.drop_sends_to == Some(to) {
                        continue;
                    }
                    let responses = self
                        .node(to)
                        .step(RaftInput::Message { from, message })
                        .expect("message step persists");
                    queue.extend(responses.into_iter().map(|response| (to, response)));
                }
                RaftOutput::Apply { index, .. } if from == self.leader_id => {
                    on_leader_apply(index);
                }
                _ => {}
            }
        }
    }

    fn pump_from(
        &mut self,
        from: NodeId,
        outputs: Vec<RaftOutput>,
        on_leader_apply: &mut dyn FnMut(LogIndex),
    ) {
        let tagged = outputs.into_iter().map(|output| (from, output)).collect();
        self.pump(tagged, on_leader_apply);
    }
}

fn open_node(root: &std::path::Path, id: NodeId, ids: &[NodeId; 3]) -> BenchNode {
    let dir = root.join(format!("node-{}", id.0));
    std::fs::create_dir_all(&dir).expect("node directory is creatable");
    let peers: Vec<NodeId> = ids.iter().copied().filter(|peer| *peer != id).collect();
    // Followers are never ticked in this harness, so a one-tick timeout only
    // ever fires for node 1, whose first tick opens the pre-vote poll and the
    // synchronous pump carries the poll, the election, and the heartbeats to
    // quiescence. The same pump answers every leader tick with follower
    // acknowledgements, so check-quorum's one-tick deadline is always met.
    let config = NodeConfig::new(id, peers, 1).expect("bench config is valid");

    let (hard_state, log, snapshots) = FileRaftNodeStores::open(&dir)
        .expect("file-backed node stores open")
        .into_parts();
    DurableRaftNode::with_storage_and_snapshot_store(config, hard_state, log, snapshots)
        .expect("node hydrates")
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

fn report_json(
    unbatched: &ProposalMetrics,
    batched: &ProposalMetrics,
    snapshot: &SnapshotMetrics,
) -> String {
    let mut out = String::from("{\n  \"harness\": \"rafter-bench-cluster\",\n  \"workloads\": [\n");
    out.push_str(&proposal_json(unbatched));
    out.push_str(",\n");
    out.push_str(&proposal_json(batched));
    out.push_str(",\n");
    let _ = writeln!(
        out,
        "    {{\"name\": \"snapshot_transfer\", \"payload_bytes\": {}, \"elapsed_ms\": {}, \"throughput_mib_per_s\": {:.1}}}",
        snapshot.payload_bytes,
        snapshot.elapsed.as_millis(),
        snapshot.payload_bytes as f64 / (1024.0 * 1024.0) / snapshot.elapsed.as_secs_f64(),
    );
    out.push_str("  ]\n}");
    out
}

fn proposal_json(metrics: &ProposalMetrics) -> String {
    let mut sorted = metrics.latencies.clone();
    sorted.sort();
    format!(
        "    {{\"name\": \"proposals\", \"batch_size\": {}, \"proposals\": {}, \"payload_bytes\": {}, \"elapsed_ms\": {}, \"proposals_per_s\": {:.0}, \"commit_latency_ms\": {{\"p50\": {:.3}, \"p99\": {:.3}, \"max\": {:.3}}}}}",
        metrics.batch_size,
        metrics.proposals,
        metrics.payload_bytes,
        metrics.elapsed.as_millis(),
        metrics.proposals as f64 / metrics.elapsed.as_secs_f64(),
        percentile(&sorted, 0.50).as_secs_f64() * 1_000.0,
        percentile(&sorted, 0.99).as_secs_f64() * 1_000.0,
        percentile(&sorted, 1.0).as_secs_f64() * 1_000.0,
    )
}
