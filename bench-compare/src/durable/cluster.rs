//! Three durable nodes and synchronous message delivery for the benchmark.

use std::collections::BTreeMap;

use rafter::{Input as RaftInput, LogIndex, NodeConfig, NodeId, Output as RaftOutput, Role};
use rafter_runtime::DurableRaftNode;
use rafter_storage::{
    FileRaftHardStateStore, FileRaftLogSegment, FileRaftNodeStores, FileRaftSnapshotStore,
};

type BenchNode = DurableRaftNode<FileRaftHardStateStore, FileRaftLogSegment, FileRaftSnapshotStore>;

#[derive(Debug)]
pub(super) struct Cluster {
    nodes: BTreeMap<NodeId, BenchNode>,
    pub(super) leader_id: NodeId,
    /// When set, sends to this node are dropped — holds a follower back so
    /// it lags legitimately (its acknowledgements never happen).
    pub(super) drop_sends_to: Option<NodeId>,
}

impl Cluster {
    /// Three file-backed nodes under `directory`; node 1 is elected by
    /// scripted ticks and real vote traffic.
    pub(super) fn elect(directory: &std::path::Path) -> Self {
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

    pub(super) fn leader(&mut self) -> &mut BenchNode {
        self.nodes.get_mut(&self.leader_id).expect("leader exists")
    }

    pub(super) fn node(&mut self, id: NodeId) -> &mut BenchNode {
        self.nodes.get_mut(&id).expect("node exists")
    }

    pub(super) fn step_leader_batch(
        &mut self,
        inputs: Vec<RaftInput>,
    ) -> Vec<(NodeId, RaftOutput)> {
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
    pub(super) fn pump(
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

    pub(super) fn pump_from(
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
