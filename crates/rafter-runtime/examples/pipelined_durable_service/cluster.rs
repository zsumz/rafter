//! Deterministic three-replica lifecycle around the reference composition.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    path::{Path, PathBuf},
};

use super::{
    application::{self, SharedState, Worker},
    codec::encode_set,
    driver::PipelinedNode,
    storage::{compact_snapshot, election_timeout_ticks, node_ids, open_node},
    ServiceReport,
};
use rafter::{Input, LocalProposalId, LogIndex, Message, NodeId, Role};

#[path = "cluster/apply.rs"]
mod apply;
#[path = "cluster/outputs.rs"]
mod outputs;

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
    application: Option<Worker>,
    state: SharedState,
    dispatched: LogIndex,
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
        let (state, application) = application::open(&self.root, node_id);
        let applied = application::snapshot(&state).applied;
        let (node, recovery) = open_node(&self.root, node_id, applied);
        self.replicas.insert(
            node_id,
            Replica {
                node,
                application: Some(application),
                state,
                dispatched: applied,
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

    fn pump(&mut self) {
        for _ in 0..256 {
            self.poll_applications();
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
            self.poll_applications();
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
            self.drain_application(leader);
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
        application::snapshot(&self.replicas[&node_id].state).applied
    }

    fn finish_replica(&mut self, node_id: NodeId) {
        let outputs = self
            .replicas
            .get_mut(&node_id)
            .expect("replica exists")
            .node
            .complete_pending();
        self.handle_outputs(node_id, outputs);
        self.drain_application(node_id);
        let mut replica = self.replicas.remove(&node_id).expect("replica exists");
        replica
            .application
            .take()
            .expect("application worker is running")
            .shutdown()
            .expect("idle application worker stops");
        let (submitted, fallbacks, outputs) = replica.node.finish();
        assert!(outputs.is_empty());
        self.pipelined_operations += submitted;
        self.synchronous_fallbacks += fallbacks;
    }

    fn compact(&mut self, leader: NodeId) -> LogIndex {
        self.drain_application(leader);
        let replica = self.replicas.get_mut(&leader).expect("leader exists");
        let state = application::snapshot(&replica.state);
        compact_snapshot(leader, &mut replica.node, &state.kv, state.applied)
    }

    fn catch_up(&mut self, leader: NodeId, follower: NodeId, through: LogIndex) {
        self.paused.remove(&follower);
        for _ in 0..32 {
            self.step(leader, Input::Tick);
            self.pump();
            if application::snapshot(&self.replicas[&follower].state).applied >= through {
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
        application::snapshot(&cluster.replicas[&NodeId(3)].state)
            .kv
            .get("gamma"),
        Some(&"3".to_owned())
    );

    cluster.propose(initial_leader, "delta", "4");
    let final_values = application::snapshot(&cluster.replicas[&initial_leader].state).kv;
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
