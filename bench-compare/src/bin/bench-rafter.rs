//! rafter harness: 3-node in-process cluster over in-memory stores.
//!
//! Mirrors the driving style of `rafter-runtime`'s `rafter-bench-cluster`
//! binary: node 1 is elected by a scripted tick, then every proposal batch is
//! stepped through the leader and every `Send` output is delivered
//! synchronously to its destination (responses routed back) until the cluster
//! is quiescent. Commit latency is measured from batch submission to the
//! leader emitting `Apply` for that proposal's index. The durable file-backed
//! path is deliberately not used here; see METHODOLOGY.md.

use std::collections::BTreeMap;
use std::time::Instant;

use bench_compare::{
    payload, report_json, WorkloadMetrics, PIPELINED_PROPOSALS, PIPELINE_DEPTH, SERIAL_PROPOSALS,
};
use rafter::{Input as RaftInput, LogIndex, NodeConfig, NodeId, Output as RaftOutput, Role};
use rafter_runtime::DurableRaftNode;
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
};

type BenchNode =
    DurableRaftNode<InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore>;

fn main() {
    let serial = proposal_workload("serial", SERIAL_PROPOSALS, 1);
    let pipelined = proposal_workload("pipelined", PIPELINED_PROPOSALS, PIPELINE_DEPTH);
    println!(
        "{}",
        report_json(
            "rafter",
            "path:../crates (workspace @ HEAD)",
            "proposal batch submitted to leader step -> leader emits Apply for that index",
            &[serial, pipelined],
        )
    );
}

/// Drives `total` proposals through an elected leader in submission bursts of
/// at most `window`, feeding follower responses synchronously until each
/// burst fully commits.
fn proposal_workload(name: &'static str, total: usize, window: usize) -> WorkloadMetrics {
    let mut cluster = Cluster::elect();

    let mut latencies = Vec::with_capacity(total);
    let mut submitted_at: BTreeMap<LogIndex, Instant> = BTreeMap::new();
    let mut next_index = cluster.leader().last_log_index();
    let started = Instant::now();

    let mut remaining = total;
    while remaining > 0 {
        let batch = remaining.min(window);
        remaining -= batch;
        let inputs: Vec<RaftInput> = (0..batch)
            .map(|_| RaftInput::ClientProposal { payload: payload() })
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
    let elapsed = started.elapsed();
    assert!(
        submitted_at.is_empty(),
        "every proposal commits and applies at the leader"
    );

    WorkloadMetrics {
        name,
        proposals: total,
        max_in_flight: window,
        elapsed,
        latencies,
    }
}

struct Cluster {
    nodes: BTreeMap<NodeId, BenchNode>,
    leader_id: NodeId,
}

impl Cluster {
    /// Three in-memory nodes; node 1 is elected by a scripted tick and real
    /// vote traffic, exactly as in `rafter-bench-cluster`.
    fn elect() -> Self {
        let ids = [NodeId(1), NodeId(2), NodeId(3)];
        let mut nodes = BTreeMap::new();
        for id in ids {
            nodes.insert(id, open_node(id, &ids));
        }
        let mut cluster = Self {
            nodes,
            leader_id: NodeId(1),
        };

        // Node 1's election timeout fires first (jitter-free config, ticked
        // alone); the pre-vote poll and the vote requests it earns circulate
        // synchronously before the workload clock starts.
        let outputs = cluster
            .nodes
            .get_mut(&NodeId(1))
            .expect("node 1 exists")
            .step(RaftInput::Tick)
            .expect("election tick");
        let tagged = outputs
            .into_iter()
            .map(|output| (NodeId(1), output))
            .collect();
        cluster.pump(tagged, &mut |_| {});
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
}

fn open_node(id: NodeId, ids: &[NodeId; 3]) -> BenchNode {
    let peers: Vec<NodeId> = ids.iter().copied().filter(|peer| *peer != id).collect();
    // Followers are never ticked in this harness, so a one-tick timeout only
    // ever fires for node 1, whose first tick opens the pre-vote poll; the
    // leader is never ticked afterwards, so check-quorum never evaluates.
    let config = NodeConfig::new(id, peers, 1).expect("bench config is valid");
    DurableRaftNode::new(config, InMemoryRaftHardStateStore::new()).expect("node hydrates")
}
