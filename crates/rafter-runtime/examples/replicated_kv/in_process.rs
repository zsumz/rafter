use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;

use rafter::{Input, LogIndex, Message, NodeId, Output, ReadId, Role};

use super::{
    codec::{apply_set, decode_snapshot, encode_set},
    storage::{compact_kv_snapshot, open_node, read_snapshot_payload},
    types::{FileNode, ScenarioOptions, ScenarioReport, ELECTION_TIMEOUT_TICKS, NODE_IDS},
};

#[derive(Clone, Debug, Eq, PartialEq)]
struct Envelope {
    from: NodeId,
    to: NodeId,
    message: Message,
}

#[derive(Debug)]
struct Replica {
    node: FileNode,
    kv: BTreeMap<String, String>,
    applied: LogIndex,
}

#[derive(Debug)]
pub struct KvCluster {
    root: PathBuf,
    replicas: BTreeMap<NodeId, Replica>,
    queue: VecDeque<Envelope>,
    paused: BTreeSet<NodeId>,
    read_grants: BTreeMap<u64, LogIndex>,
    next_read_id: u64,
}

impl KvCluster {
    fn open(root: PathBuf) -> Self {
        let mut cluster = Self {
            root,
            replicas: BTreeMap::new(),
            queue: VecDeque::new(),
            paused: BTreeSet::new(),
            read_grants: BTreeMap::new(),
            next_read_id: 1,
        };
        for node_id in NODE_IDS {
            let (node, recovery_outputs) = open_node(&cluster.root, node_id, LogIndex::ZERO);
            cluster.replicas.insert(
                node_id,
                Replica {
                    node,
                    kv: BTreeMap::new(),
                    applied: LogIndex::ZERO,
                },
            );
            cluster.handle_outputs(node_id, recovery_outputs);
        }
        cluster
    }

    fn node(&self, node_id: NodeId) -> &FileNode {
        &self.replicas[&node_id].node
    }

    fn step(&mut self, node_id: NodeId, input: Input) {
        let outputs = self
            .replicas
            .get_mut(&node_id)
            .expect("node exists")
            .node
            .step(input)
            .expect("durable step succeeds");
        self.handle_outputs(node_id, outputs);
    }

    fn handle_outputs(&mut self, node_id: NodeId, outputs: Vec<Output>) {
        for output in outputs {
            match output {
                Output::Send { to, message } => {
                    if !self.paused.contains(&to) {
                        self.queue.push_back(Envelope {
                            from: node_id,
                            to,
                            message,
                        });
                    }
                }
                Output::Apply { index, payload, .. } => {
                    let command = std::str::from_utf8(payload.as_slice())
                        .expect("example commands are UTF-8");
                    apply_set(
                        command,
                        &mut self.replicas.get_mut(&node_id).expect("node exists").kv,
                    );
                    self.replicas
                        .get_mut(&node_id)
                        .expect("node exists")
                        .applied = index;
                }
                Output::ApplySnapshot { snapshot } => {
                    let payload = read_snapshot_payload(&self.replicas[&node_id].node, &snapshot);
                    let kv = decode_snapshot(&payload);
                    let replica = self.replicas.get_mut(&node_id).expect("node exists");
                    replica.kv = kv;
                    replica.applied = snapshot.metadata.last_included_index;
                }
                Output::ReadIndexGranted {
                    read_id,
                    read_index,
                } => {
                    let request_id = read_id.0;
                    self.read_grants.insert(request_id, read_index);
                }
                Output::RejectProposal { reason, .. } => panic!("proposal rejected: {reason}"),
                Output::ReadIndexRejected { reason, .. } => {
                    panic!("read index rejected: {reason}")
                }
                Output::ReadIndexCanceled { reason, .. } => {
                    panic!("read index canceled: {reason:?}")
                }
                Output::LeadershipTransferRejected { target, reason } => {
                    panic!("leadership transfer to {target} rejected: {reason}")
                }
                Output::LocalProposalAppended { .. }
                | Output::LocalProposalDropped { .. }
                | Output::StageSnapshotChunk { .. } => {}
                Output::SendSnapshotChunk { .. } => {
                    panic!("runtime should resolve snapshot chunk sends")
                }
            }
        }
    }

    fn pump(&mut self) {
        self.pump_until_idle(128);
    }

    fn pump_until_idle(&mut self, max_waves: usize) {
        for _ in 0..max_waves {
            if self.queue.is_empty() {
                return;
            }
            let mut batch = Vec::new();
            while let Some(envelope) = self.queue.pop_front() {
                batch.push(envelope);
            }
            for envelope in batch {
                self.step(
                    envelope.to,
                    Input::Message {
                        from: envelope.from,
                        message: envelope.message,
                    },
                );
            }
        }
        panic!("message pump did not quiesce");
    }

    fn elect_node_one(&mut self) -> NodeId {
        for _ in 0..ELECTION_TIMEOUT_TICKS {
            self.step(NodeId(1), Input::Tick);
        }
        self.pump();
        assert_eq!(self.node(NodeId(1)).role(), Role::Leader);
        NodeId(1)
    }

    fn leader(&self) -> NodeId {
        NODE_IDS
            .into_iter()
            .find(|node_id| self.node(*node_id).role() == Role::Leader)
            .expect("cluster has a leader")
    }

    fn propose_set(&mut self, leader: NodeId, key: &str, value: &str) {
        self.step(
            leader,
            Input::ClientProposal {
                payload: encode_set(key, value),
            },
        );
        self.pump();
    }

    fn linearizable_get(&mut self, leader: NodeId, key: &str) -> Option<String> {
        let request_id = self.next_read_id;
        self.next_read_id += 1;
        self.step(
            leader,
            Input::ReadIndex {
                read_id: ReadId(request_id),
            },
        );
        for _ in 0..16 {
            self.pump();
            if self.read_grants.contains_key(&request_id) {
                break;
            }
            self.step(leader, Input::Tick);
        }
        self.pump();
        let read_index = self
            .read_grants
            .remove(&request_id)
            .expect("read barrier grants");
        let replica = &self.replicas[&leader];
        assert!(
            replica.applied >= read_index,
            "state machine must apply through the granted read index"
        );
        replica.kv.get(key).cloned()
    }

    fn restart(&mut self, node_id: NodeId) {
        let Replica { node, kv, applied } = self.replicas.remove(&node_id).expect("node exists");
        drop(node);
        let (node, recovery_outputs) = open_node(&self.root, node_id, applied);
        self.replicas.insert(node_id, Replica { node, kv, applied });
        self.handle_outputs(node_id, recovery_outputs);
    }

    fn compact_leader_snapshot(&mut self, leader: NodeId) -> LogIndex {
        let replica = self.replicas.get_mut(&leader).expect("leader exists");
        compact_kv_snapshot(leader, &mut replica.node, &replica.kv, replica.applied)
    }

    fn catch_up_node(&mut self, leader: NodeId, follower: NodeId, through: LogIndex) {
        self.paused.remove(&follower);
        for _ in 0..16 {
            self.step(leader, Input::Tick);
            self.pump();
            if self.replicas[&follower].applied >= through {
                return;
            }
        }
        panic!("{follower} did not catch up through snapshot {through}");
    }

    fn transfer_leadership(&mut self, leader: NodeId, target: NodeId) {
        self.step(leader, Input::TransferLeadership { target });
        self.pump();
        for _ in 0..ELECTION_TIMEOUT_TICKS {
            self.step(target, Input::Tick);
            self.pump();
            if self.node(target).role() == Role::Leader {
                return;
            }
        }
        panic!("leadership did not transfer to {target}");
    }
}

/// Runs the in-process durable KV scenario under `root`.
///
/// # Panics
///
/// Panics when the deterministic example invariant fails or when temporary
/// file-backed storage cannot be opened.
#[must_use]
pub fn run_in_process_demo(root: PathBuf, options: ScenarioOptions) -> ScenarioReport {
    std::fs::create_dir_all(&root).expect("create example directory");
    let mut cluster = KvCluster::open(root);

    let initial_leader = cluster.elect_node_one();
    cluster.propose_set(initial_leader, "alpha", "1");
    cluster.propose_set(initial_leader, "beta", "2");
    let alpha_read = cluster.linearizable_get(initial_leader, "alpha");

    cluster.restart(NodeId(2));
    let restarted_applied_floor = cluster.replicas[&NodeId(2)].applied;
    cluster.step(initial_leader, Input::Tick);
    cluster.pump();
    assert_eq!(
        cluster.replicas[&NodeId(2)].kv.get("alpha"),
        Some(&"1".to_string())
    );

    cluster.paused.insert(NodeId(3));
    cluster.propose_set(initial_leader, "gamma", "3");
    let snapshot_index = cluster.compact_leader_snapshot(initial_leader);
    cluster.catch_up_node(initial_leader, NodeId(3), snapshot_index);
    assert_eq!(
        cluster.replicas[&NodeId(3)].kv.get("gamma"),
        Some(&"3".to_string())
    );

    cluster.transfer_leadership(initial_leader, NodeId(2));
    let transferred_leader = cluster.leader();
    assert_eq!(transferred_leader, NodeId(2));
    cluster.propose_set(transferred_leader, "delta", "4");
    assert_eq!(
        cluster.linearizable_get(transferred_leader, "delta"),
        Some("4".to_string())
    );

    let final_values = cluster.replicas[&transferred_leader].kv.clone();
    if options.verbose {
        println!(
            "replicated kv: leader {initial_leader} -> {transferred_leader}, read alpha={alpha_read:?}, snapshot {snapshot_index}, final {final_values:?}"
        );
    }
    if !options.keep_dir {
        std::fs::remove_dir_all(&cluster.root).ok();
    }

    ScenarioReport {
        initial_leader,
        transferred_leader,
        alpha_read,
        final_values,
        snapshot_index,
        restarted_applied_floor,
    }
}
