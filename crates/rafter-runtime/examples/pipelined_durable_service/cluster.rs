//! Deterministic three-replica lifecycle around the reference composition.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::{Path, PathBuf},
};

use rafter::{Input, LocalProposalId, LogIndex, Message, NodeId, Output, Role};

use super::{
    app_state::{load_app_state, persist_app_state},
    codec::{apply_set, decode_snapshot, encode_set},
    driver::PipelinedNode,
    storage::{
        compact_snapshot, election_timeout_ticks, node_dir, node_ids, open_node,
        read_snapshot_payload,
    },
    ServiceReport,
};

const PEER_QUEUE_CAPACITY: usize = 1_024;

#[derive(Clone, Debug)]
struct Envelope {
    from: NodeId,
    to: NodeId,
    message: Message,
}

#[derive(Debug)]
struct Replica {
    node: PipelinedNode,
    kv: BTreeMap<String, String>,
    applied: LogIndex,
    directory: PathBuf,
}

#[derive(Debug)]
struct Cluster {
    root: PathBuf,
    replicas: BTreeMap<NodeId, Replica>,
    peer_queue: VecDeque<Envelope>,
    paused: BTreeSet<NodeId>,
    completed: BTreeSet<LocalProposalId>,
    next_proposal_id: u64,
    max_peer_queue_depth: usize,
    pipelined_operations: u64,
    synchronous_fallbacks: u64,
}

impl Cluster {
    fn open(root: PathBuf) -> Self {
        std::fs::create_dir_all(&root).expect("create reference service root");
        let mut cluster = Self {
            root,
            replicas: BTreeMap::new(),
            peer_queue: VecDeque::new(),
            paused: BTreeSet::new(),
            completed: BTreeSet::new(),
            next_proposal_id: 1,
            max_peer_queue_depth: 0,
            pipelined_operations: 0,
            synchronous_fallbacks: 0,
        };
        for node_id in node_ids() {
            cluster.open_replica(node_id);
        }
        cluster
    }

    fn open_replica(&mut self, node_id: NodeId) {
        let app = load_app_state(&self.root, node_id);
        let (node, recovery) = open_node(&self.root, node_id, app.applied);
        self.replicas.insert(
            node_id,
            Replica {
                node,
                kv: app.kv,
                applied: app.applied,
                directory: node_dir(&self.root, node_id),
            },
        );
        self.handle_outputs(node_id, recovery);
    }

    fn step(&mut self, node_id: NodeId, input: Input) {
        let outputs = self
            .replicas
            .get_mut(&node_id)
            .expect("replica exists")
            .node
            .step(input);
        self.handle_outputs(node_id, outputs);
    }

    fn handle_outputs(&mut self, node_id: NodeId, outputs: Vec<Output>) {
        for output in outputs {
            match output {
                Output::Send { to, message } => {
                    if self.paused.contains(&to) {
                        continue;
                    }
                    assert!(
                        self.peer_queue.len() < PEER_QUEUE_CAPACITY,
                        "bounded peer queue refused a message"
                    );
                    self.peer_queue.push_back(Envelope {
                        from: node_id,
                        to,
                        message,
                    });
                    self.max_peer_queue_depth =
                        self.max_peer_queue_depth.max(self.peer_queue.len());
                }
                Output::Apply {
                    index,
                    payload,
                    local_proposal_id,
                    ..
                } => {
                    let replica = self.replicas.get_mut(&node_id).expect("replica exists");
                    let command = std::str::from_utf8(payload.as_slice())
                        .expect("reference commands are UTF-8");
                    apply_set(command, &mut replica.kv);
                    replica.applied = index;
                    persist_app_state(&replica.directory, &replica.kv, replica.applied);
                    if let Some(proposal_id) = local_proposal_id {
                        // This is the client ACK boundary: application bytes and
                        // applied floor are durable before completion is visible.
                        self.completed.insert(proposal_id);
                    }
                }
                Output::ApplySnapshot { snapshot } => {
                    let payload = read_snapshot_payload(
                        &self.replicas.get(&node_id).expect("replica exists").node,
                        &snapshot,
                    );
                    let replica = self.replicas.get_mut(&node_id).expect("replica exists");
                    replica.kv = decode_snapshot(&payload);
                    replica.applied = snapshot.metadata.last_included_index;
                    persist_app_state(&replica.directory, &replica.kv, replica.applied);
                }
                Output::RejectProposal { reason, .. } => panic!("proposal rejected: {reason}"),
                Output::LocalProposalDropped { reason, .. } => {
                    panic!("local proposal outcome became unknown: {reason:?}")
                }
                Output::ReadIndexRejected { reason, .. } => {
                    panic!("unexpected read rejection: {reason}")
                }
                Output::ReadIndexCanceled { reason, .. } => {
                    panic!("unexpected read cancellation: {reason:?}")
                }
                Output::LeadershipTransferRejected { target, reason } => {
                    panic!("leadership transfer to {target} rejected: {reason}")
                }
                Output::ConfigurationCommitted { .. }
                | Output::LocalProposalAppended { .. }
                | Output::ReadIndexGranted { .. }
                | Output::StageSnapshotChunk { .. } => {}
                Output::SendSnapshotChunk { .. } => {
                    panic!("runtime must resolve snapshot chunk sends")
                }
            }
        }
    }

    fn pump(&mut self) {
        for _ in 0..256 {
            if self.peer_queue.is_empty() {
                return;
            }
            let wave = self.peer_queue.len();
            for _ in 0..wave {
                let envelope = self.peer_queue.pop_front().expect("queued peer message");
                self.step(
                    envelope.to,
                    Input::Message {
                        from: envelope.from,
                        message: envelope.message,
                    },
                );
            }
        }
        panic!("peer queue did not quiesce");
    }

    fn elect_node_one(&mut self) -> NodeId {
        for _ in 0..election_timeout_ticks() {
            self.step(NodeId(1), Input::Tick);
        }
        self.pump();
        assert_eq!(self.replicas[&NodeId(1)].node.ready().role(), Role::Leader);
        NodeId(1)
    }

    fn propose(&mut self, leader: NodeId, key: &str, value: &str) {
        let proposal_id = LocalProposalId(self.next_proposal_id);
        self.next_proposal_id += 1;
        self.step(
            leader,
            Input::TrackedClientProposal {
                proposal_id,
                payload: encode_set(key, value),
            },
        );
        for _ in 0..32 {
            self.pump();
            if self.completed.remove(&proposal_id) {
                return;
            }
            self.step(leader, Input::Tick);
        }
        panic!("durable proposal {proposal_id:?} did not complete");
    }

    fn restart(&mut self, node_id: NodeId) -> LogIndex {
        self.finish_replica(node_id);
        self.open_replica(node_id);
        self.replicas[&node_id].applied
    }

    fn finish_replica(&mut self, node_id: NodeId) {
        let outputs = self
            .replicas
            .get_mut(&node_id)
            .expect("replica exists")
            .node
            .complete_pending();
        self.handle_outputs(node_id, outputs);
        let replica = self.replicas.remove(&node_id).expect("replica exists");
        let (submitted, fallbacks, outputs) = replica.node.finish();
        assert!(outputs.is_empty());
        self.pipelined_operations += submitted;
        self.synchronous_fallbacks += fallbacks;
    }

    fn compact(&mut self, leader: NodeId) -> LogIndex {
        let replica = self.replicas.get_mut(&leader).expect("leader exists");
        compact_snapshot(leader, &mut replica.node, &replica.kv, replica.applied)
    }

    fn catch_up(&mut self, leader: NodeId, follower: NodeId, through: LogIndex) {
        self.paused.remove(&follower);
        for _ in 0..32 {
            self.step(leader, Input::Tick);
            self.pump();
            if self.replicas[&follower].applied >= through {
                return;
            }
        }
        panic!("lagging follower did not install the compacted snapshot");
    }

    fn shutdown(mut self) -> (u64, u64) {
        for node_id in node_ids() {
            self.finish_replica(node_id);
        }
        (self.pipelined_operations, self.synchronous_fallbacks)
    }
}

pub(super) fn run(root: &Path, keep_dir: bool) -> ServiceReport {
    let mut cluster = Cluster::open(root.to_owned());
    let initial_leader = cluster.elect_node_one();
    cluster.propose(initial_leader, "alpha", "1");
    cluster.propose(initial_leader, "beta", "2");

    let restarted_applied_floor = cluster.restart(NodeId(2));
    assert!(restarted_applied_floor > LogIndex::ZERO);

    cluster.paused.insert(NodeId(3));
    cluster.propose(initial_leader, "gamma", "3");
    let snapshot_index = cluster.compact(initial_leader);
    cluster.catch_up(initial_leader, NodeId(3), snapshot_index);
    assert_eq!(
        cluster.replicas[&NodeId(3)].kv.get("gamma"),
        Some(&"3".to_owned())
    );

    cluster.propose(initial_leader, "delta", "4");
    let final_values = cluster.replicas[&initial_leader].kv.clone();
    let max_peer_queue_depth = cluster.max_peer_queue_depth;
    let (pipelined_operations, synchronous_fallbacks) = cluster.shutdown();

    if !keep_dir {
        std::fs::remove_dir_all(root).expect("remove closed reference service directory");
    }
    ServiceReport {
        final_values,
        pipelined_operations,
        synchronous_fallbacks,
        max_peer_queue_depth,
        peer_queue_capacity: PEER_QUEUE_CAPACITY,
        snapshot_index,
        restarted_applied_floor,
        initial_leader,
    }
}
