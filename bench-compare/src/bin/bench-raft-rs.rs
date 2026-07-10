//! raft-rs (tikv `raft` crate) harness: three `RawNode<MemStorage>` peers.
//!
//! Runs the canonical ready-loop from raft-rs's `five_mem_node` example:
//! ready -> send messages -> apply committed entries -> persist entries and
//! hard state into `MemStorage` -> send persisted messages -> advance ->
//! handle the light ready -> advance_apply. Message passing is synchronous
//! and in-process. Commit latency is measured from proposal submission on the
//! leader to the leader handling the committed entry through its apply path.

use std::collections::{BTreeMap, VecDeque};
use std::time::Instant;

use bench_compare::{
    payload_of_size, report_json, WorkloadMetrics, LARGE_PAYLOAD_BYTES,
    LARGE_PAYLOAD_PIPELINE_DEPTH, LARGE_PAYLOAD_PROPOSALS, PAYLOAD_BYTES, PIPELINED_PROPOSALS,
    PIPELINE_DEPTH, SERIAL_PROPOSALS,
};
use raft::prelude::{Config, Message, Snapshot};
use raft::storage::MemStorage;
use raft::{RawNode, StateRole};

const VOTERS: [u64; 3] = [1, 2, 3];
const LEADER: u64 = 1;

fn main() {
    let serial = proposal_workload("serial", SERIAL_PROPOSALS, PAYLOAD_BYTES, 1);
    let pipelined = proposal_workload(
        "pipelined",
        PIPELINED_PROPOSALS,
        PAYLOAD_BYTES,
        PIPELINE_DEPTH,
    );
    let large_payload = proposal_workload(
        "large_payload",
        LARGE_PAYLOAD_PROPOSALS,
        LARGE_PAYLOAD_BYTES,
        LARGE_PAYLOAD_PIPELINE_DEPTH,
    );
    println!(
        "{}",
        report_json(
            "raft-rs",
            "0.7.0 (crate `raft`, prost-codec)",
            "propose() on leader -> leader takes the committed entry from ready/light-ready",
            &[serial, pipelined, large_payload],
        )
    );
}

/// Drives `total` proposals through the elected leader in submission bursts
/// of at most `window`, pumping the cluster to quiescence after each burst.
fn proposal_workload(
    name: &'static str,
    total: usize,
    payload_bytes: usize,
    window: usize,
) -> WorkloadMetrics {
    let mut cluster = Cluster::elect();

    let mut latencies = Vec::with_capacity(total);
    let mut submitted_at: BTreeMap<u64, Instant> = BTreeMap::new();
    let started = Instant::now();

    let mut remaining = total;
    while remaining > 0 {
        let batch = remaining.min(window);
        remaining -= batch;
        let now = Instant::now();
        {
            let leader = cluster.nodes.get_mut(&LEADER).expect("leader exists");
            for _ in 0..batch {
                let index = leader.raft.raft_log.last_index() + 1;
                leader
                    .propose(vec![], payload_of_size(payload_bytes))
                    .expect("leader accepts proposal");
                assert_eq!(
                    leader.raft.raft_log.last_index(),
                    index,
                    "proposal is appended at the expected index"
                );
                submitted_at.insert(index, now);
            }
        }
        cluster.pump(&mut |index| {
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
        payload_bytes,
        max_in_flight: window,
        elapsed,
        latencies,
        shape: None,
        service_shape: None,
        read_shape: None,
        codec_shape: None,
        multiraft_shape: None,
        failover_shape: None,
    }
}

struct Cluster {
    nodes: BTreeMap<u64, RawNode<MemStorage>>,
}

impl Cluster {
    /// Three peers sharing one static voter set; node 1 campaigns directly
    /// and wins with synchronous vote traffic.
    fn elect() -> Self {
        let mut cluster = Self {
            nodes: VOTERS.iter().map(|id| (*id, open_node(*id))).collect(),
        };
        cluster
            .nodes
            .get_mut(&LEADER)
            .expect("node 1 exists")
            .campaign()
            .expect("node 1 campaigns");
        cluster.pump(&mut |_| {});
        assert_eq!(
            cluster.nodes[&LEADER].raft.state,
            StateRole::Leader,
            "node 1 wins"
        );
        cluster
    }

    /// Processes readies and routes messages synchronously until no node has
    /// a ready and no message is undelivered, reporting every leader-side
    /// committed proposal index to `on_leader_apply`.
    fn pump(&mut self, on_leader_apply: &mut dyn FnMut(u64)) {
        let mut inbox: VecDeque<Message> = VecDeque::new();
        loop {
            let mut progressed = false;
            for id in VOTERS {
                while self.nodes.get_mut(&id).expect("node exists").has_ready() {
                    progressed = true;
                    let messages = self.handle_ready(id, on_leader_apply);
                    inbox.extend(messages);
                }
            }
            while let Some(message) = inbox.pop_front() {
                progressed = true;
                let to = message.to;
                // Errors from step (e.g. stale term) are dropped like lost
                // messages, matching the raft-rs examples.
                let _ = self.nodes.get_mut(&to).expect("node exists").step(message);
            }
            if !progressed {
                break;
            }
        }
    }

    /// The canonical raft-rs ready-loop, verbatim from the `five_mem_node`
    /// example, returning the outbound messages instead of mailing them.
    fn handle_ready(&mut self, id: u64, on_leader_apply: &mut dyn FnMut(u64)) -> Vec<Message> {
        let is_leader_node = id == LEADER;
        let node = self.nodes.get_mut(&id).expect("node exists");
        let store = node.raft.raft_log.store.clone();
        let mut out = Vec::new();

        let mut ready = node.ready();
        out.extend(ready.take_messages());
        if *ready.snapshot() != Snapshot::default() {
            store
                .wl()
                .apply_snapshot(ready.snapshot().clone())
                .expect("snapshot applies");
        }
        for entry in ready.take_committed_entries() {
            if is_leader_node && !entry.data.is_empty() {
                on_leader_apply(entry.index);
            }
        }
        store.wl().append(ready.entries()).expect("entries persist");
        if let Some(hard_state) = ready.hs() {
            store.wl().set_hardstate(hard_state.clone());
        }
        out.extend(ready.take_persisted_messages());

        let mut light_ready = node.advance(ready);
        if let Some(commit) = light_ready.commit_index() {
            store.wl().mut_hard_state().set_commit(commit);
        }
        out.extend(light_ready.take_messages());
        for entry in light_ready.take_committed_entries() {
            if is_leader_node && !entry.data.is_empty() {
                on_leader_apply(entry.index);
            }
        }
        node.advance_apply();
        out
    }
}

fn open_node(id: u64) -> RawNode<MemStorage> {
    let config = Config {
        id,
        election_tick: 10,
        heartbeat_tick: 3,
        // Matches rafter's 512 KiB per-append byte cap; the raft-rs default
        // of 0 means one entry per append message, which would be unfair.
        max_size_per_msg: 512 * 1024,
        ..Config::default()
    };
    let storage = MemStorage::new_with_conf_state((VOTERS.to_vec(), vec![]));
    let logger = slog::Logger::root(slog::Discard, slog::o!());
    RawNode::new(&config, storage, &logger).expect("valid raft-rs config")
}
