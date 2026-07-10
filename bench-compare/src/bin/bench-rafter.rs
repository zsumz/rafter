//! rafter harness: 3-node in-process cluster over in-memory stores.
//!
//! Mirrors the driving style of `rafter-runtime`'s `rafter-bench-cluster`
//! binary: node 1 is elected by a scripted tick, then every proposal batch is
//! stepped through the leader and every `Send` output is delivered
//! synchronously to its destination (responses routed back) until the cluster
//! is quiescent. Commit latency is measured from batch submission to the
//! leader emitting `Apply` for that proposal's index. The durable file-backed
//! path is deliberately not used here; see METHODOLOGY.md.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::Instant;

use bench_compare::{
    payload_of_size, report_json, FailoverShapeMetrics, ReadShapeMetrics, ShapeMetrics,
    WorkloadMetrics, FAILOVER_QUEUED_PROPOSALS, FAILOVER_ROUNDS, LARGE_PAYLOAD_BYTES,
    LARGE_PAYLOAD_PIPELINE_DEPTH, LARGE_PAYLOAD_PROPOSALS, LEASE_READ_REQUESTS, PAYLOAD_BYTES,
    PIPELINED_PROPOSALS, PIPELINE_DEPTH, READ_BATCH_DEPTH, READ_BATCH_REQUESTS,
    READ_LOAD_PROPOSALS, READ_LOAD_WRITE_BATCH_DEPTH, SERIAL_PROPOSALS,
};
use rafter::{
    Input as RaftInput, LogIndex, Message as RaftMessage, NodeConfig, NodeId, Output as RaftOutput,
    ReadId, Role,
};
use rafter_runtime::DurableRaftNode;
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
};

type BenchNode =
    DurableRaftNode<InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore>;

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
    let mut workloads = vec![serial, pipelined, large_payload];
    if std::env::var_os("BENCH_RAFTER_EXTRA_WORKLOADS").is_some() {
        workloads.push(read_index_under_write_load());
        workloads.push(read_index_batch());
        workloads.push(lease_read());
        workloads.push(leader_failover_under_queued_proposals());
    }
    println!(
        "{}",
        report_json(
            "rafter",
            "path:../crates (workspace @ HEAD)",
            "proposal batch submitted to leader step -> leader emits Apply for that index",
            &workloads,
        )
    );
}

/// Drives `total` proposals through an elected leader in submission bursts of
/// at most `window`, feeding follower responses synchronously until each
/// burst fully commits.
fn proposal_workload(
    name: &'static str,
    total: usize,
    payload_bytes: usize,
    window: usize,
) -> WorkloadMetrics {
    let mut cluster = Cluster::elect();

    let mut latencies = Vec::with_capacity(total);
    let mut submitted_at: BTreeMap<LogIndex, Instant> = BTreeMap::new();
    let mut next_index = cluster.leader().last_log_index();
    let mut shape = ShapeMetrics::default();
    let started = Instant::now();

    let mut remaining = total;
    while remaining > 0 {
        let batch = remaining.min(window);
        remaining -= batch;
        shape.proposal_batches += 1;
        let now = Instant::now();
        for _ in 0..batch {
            next_index = LogIndex(next_index.0 + 1);
            submitted_at.insert(next_index, now);
        }
        let outputs = if batch == 1 {
            cluster.step_leader(RaftInput::ClientProposal {
                payload: payload_of_size(payload_bytes),
            })
        } else {
            let inputs: Vec<RaftInput> = (0..batch)
                .map(|_| RaftInput::ClientProposal {
                    payload: payload_of_size(payload_bytes),
                })
                .collect();
            cluster.step_leader_batch(inputs)
        };
        shape.leader_broadcast_rounds += leader_broadcast_rounds(&outputs, cluster.leader_id);
        cluster.pump(
            outputs,
            &mut |index| {
                if let Some(at) = submitted_at.remove(&index) {
                    latencies.push(at.elapsed());
                }
            },
            &mut |_, _| {},
            &mut shape,
        );
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
        shape: Some(shape),
        service_shape: None,
        read_shape: None,
        codec_shape: None,
        multiraft_shape: None,
        failover_shape: None,
    }
}

/// Drives proposal bursts while registering one read-index barrier before each
/// burst's replication messages are delivered. This keeps writes in flight
/// while proving read barriers still grant promptly and in order.
fn read_index_under_write_load() -> WorkloadMetrics {
    let mut cluster = Cluster::elect();

    let mut latencies = Vec::with_capacity(READ_LOAD_PROPOSALS);
    let mut submitted_at: BTreeMap<LogIndex, Instant> = BTreeMap::new();
    let mut read_submitted_at: BTreeMap<ReadId, Instant> = BTreeMap::new();
    let mut next_index = cluster.leader().last_log_index();
    let mut next_read_id = 1_u64;
    let mut shape = ShapeMetrics::default();
    let mut read_shape = ReadShapeMetrics::default();
    let started = Instant::now();

    let mut remaining = READ_LOAD_PROPOSALS;
    while remaining > 0 {
        let batch = remaining.min(READ_LOAD_WRITE_BATCH_DEPTH);
        remaining -= batch;
        shape.proposal_batches += 1;
        let inputs: Vec<RaftInput> = (0..batch)
            .map(|_| RaftInput::ClientProposal {
                payload: payload_of_size(PAYLOAD_BYTES),
            })
            .collect();
        let now = Instant::now();
        for _ in 0..batch {
            next_index = LogIndex(next_index.0 + 1);
            submitted_at.insert(next_index, now);
        }

        let mut outputs = cluster.step_leader_batch(inputs);
        shape.leader_broadcast_rounds += leader_broadcast_rounds(&outputs, cluster.leader_id);

        let read_id = ReadId(next_read_id);
        next_read_id += 1;
        read_shape.read_requests += 1;
        read_submitted_at.insert(read_id, Instant::now());
        let read_outputs = cluster.step_leader_batch(vec![RaftInput::ReadIndex { read_id }]);
        read_shape.confirmation_rounds += leader_broadcast_rounds(&read_outputs, cluster.leader_id);
        shape.leader_broadcast_rounds += leader_broadcast_rounds(&read_outputs, cluster.leader_id);
        outputs.extend(read_outputs);

        cluster.pump(
            outputs,
            &mut |index| {
                if let Some(at) = submitted_at.remove(&index) {
                    latencies.push(at.elapsed());
                }
            },
            &mut |read_id, _read_index| {
                if let Some(at) = read_submitted_at.remove(&read_id) {
                    read_shape.latencies.push(at.elapsed());
                    read_shape.read_grants += 1;
                }
            },
            &mut shape,
        );
    }
    let elapsed = started.elapsed();
    assert!(
        submitted_at.is_empty(),
        "every write commits and applies at the leader"
    );
    assert!(
        read_submitted_at.is_empty(),
        "every read-index barrier grants"
    );
    assert_eq!(read_shape.read_requests, read_shape.read_grants);

    WorkloadMetrics {
        name: "read_index_load",
        proposals: READ_LOAD_PROPOSALS,
        payload_bytes: PAYLOAD_BYTES,
        max_in_flight: READ_LOAD_WRITE_BATCH_DEPTH,
        elapsed,
        latencies,
        shape: Some(shape),
        service_shape: None,
        read_shape: Some(read_shape),
        codec_shape: None,
        multiraft_shape: None,
        failover_shape: None,
    }
}

/// Measures deterministic read-barrier batching: consecutive read-index
/// inputs submitted through one `step_batch` share one confirmation heartbeat
/// round and one quorum-evidence set while preserving per-read grants.
fn read_index_batch() -> WorkloadMetrics {
    let mut cluster = Cluster::elect();
    let mut setup_shape = ShapeMetrics::default();
    let setup_outputs = cluster.step_leader(RaftInput::ClientProposal {
        payload: payload_of_size(PAYLOAD_BYTES),
    });
    cluster.pump(
        setup_outputs,
        &mut |_| {},
        &mut |_, _| {},
        &mut setup_shape,
    );
    assert!(
        cluster.leader().commit_index() > LogIndex::ZERO,
        "setup commits a current-term entry before read-index batches are measured"
    );

    let mut latencies = Vec::with_capacity(READ_BATCH_REQUESTS);
    let mut read_submitted_at: BTreeMap<ReadId, Instant> = BTreeMap::new();
    let mut read_shape = ReadShapeMetrics::default();
    let started = Instant::now();

    let mut next_read_id = 1_u64;
    let mut remaining = READ_BATCH_REQUESTS;
    while remaining > 0 {
        let batch = remaining.min(READ_BATCH_DEPTH);
        remaining -= batch;
        let submitted_at = Instant::now();
        let inputs: Vec<_> = (0..batch)
            .map(|_| {
                let read_id = ReadId(next_read_id);
                next_read_id += 1;
                read_shape.read_requests += 1;
                read_submitted_at.insert(read_id, submitted_at);
                RaftInput::ReadIndex { read_id }
            })
            .collect();

        let outputs = cluster.step_leader_batch(inputs);
        read_shape.confirmation_rounds += leader_broadcast_rounds(&outputs, cluster.leader_id);
        cluster.pump(
            outputs,
            &mut |_| {},
            &mut |read_id, _read_index| {
                if let Some(at) = read_submitted_at.remove(&read_id) {
                    let latency = at.elapsed();
                    latencies.push(latency);
                    read_shape.latencies.push(latency);
                    read_shape.read_grants += 1;
                }
            },
            &mut ShapeMetrics::default(),
        );
    }
    let elapsed = started.elapsed();

    assert!(
        read_submitted_at.is_empty(),
        "every batched read-index barrier grants"
    );
    assert_eq!(read_shape.read_requests, read_shape.read_grants);
    assert_eq!(
        read_shape.confirmation_rounds,
        READ_BATCH_REQUESTS.div_ceil(READ_BATCH_DEPTH),
        "one confirmation round covers each read batch"
    );

    WorkloadMetrics {
        name: "read_index_batch",
        proposals: READ_BATCH_REQUESTS,
        payload_bytes: 0,
        max_in_flight: READ_BATCH_DEPTH,
        elapsed,
        latencies,
        shape: None,
        service_shape: None,
        read_shape: Some(read_shape),
        codec_shape: None,
        multiraft_shape: None,
        failover_shape: None,
    }
}

/// Measures the read-heavy product knob: with lease reads explicitly enabled,
/// and after a current-term commit plus quorum acknowledgement establishes the
/// lease, every read barrier should grant locally with no confirmation round.
///
/// This workload is Rafter-only because it depends on Rafter's documented
/// tick-skew lease assumption and the pre-vote/check-quorum foundation exposed
/// by `NodeConfig::with_lease_reads`.
fn lease_read() -> WorkloadMetrics {
    let mut cluster = Cluster::elect_with_config(8, true);
    let mut setup_shape = ShapeMetrics::default();
    let setup_outputs = cluster.step_leader(RaftInput::ClientProposal {
        payload: payload_of_size(PAYLOAD_BYTES),
    });
    cluster.pump(
        setup_outputs,
        &mut |_| {},
        &mut |_, _| {},
        &mut setup_shape,
    );
    assert!(
        cluster.leader().commit_index() > LogIndex::ZERO,
        "setup commits a current-term entry before lease reads are measured"
    );

    let mut latencies = Vec::with_capacity(LEASE_READ_REQUESTS);
    let mut read_shape = ReadShapeMetrics::default();
    let started = Instant::now();

    for request in 0..LEASE_READ_REQUESTS {
        let read_id = ReadId(request as u64 + 1);
        let submitted_at = Instant::now();
        read_shape.read_requests += 1;
        let outputs = cluster.step_leader(RaftInput::ReadIndex { read_id });
        read_shape.confirmation_rounds += leader_broadcast_rounds(&outputs, cluster.leader_id);

        let mut granted = false;
        cluster.pump(
            outputs,
            &mut |_| {},
            &mut |granted_id, _read_index| {
                if granted_id == read_id {
                    let latency = submitted_at.elapsed();
                    latencies.push(latency);
                    read_shape.latencies.push(latency);
                    read_shape.read_grants += 1;
                    granted = true;
                }
            },
            &mut ShapeMetrics::default(),
        );
        assert!(granted, "lease read grants synchronously");
    }

    assert_eq!(read_shape.read_requests, read_shape.read_grants);
    assert_eq!(
        read_shape.confirmation_rounds, 0,
        "lease reads must not broadcast confirmation rounds while the lease holds"
    );

    WorkloadMetrics {
        name: "lease_read",
        proposals: LEASE_READ_REQUESTS,
        payload_bytes: 0,
        max_in_flight: 1,
        elapsed: started.elapsed(),
        latencies,
        shape: None,
        service_shape: None,
        read_shape: Some(read_shape),
        codec_shape: None,
        multiraft_shape: None,
        failover_shape: None,
    }
}

/// Queues one proposal burst on an old leader, replicates it to the successor
/// without delivering the acknowledgements, then partitions the old leader and
/// measures when the successor applies the queued proposal range after winning.
fn leader_failover_under_queued_proposals() -> WorkloadMetrics {
    let total = FAILOVER_ROUNDS.saturating_mul(FAILOVER_QUEUED_PROPOSALS);
    let mut latencies = Vec::with_capacity(total);
    let mut failover_shape = FailoverShapeMetrics {
        failovers: FAILOVER_ROUNDS,
        queued_proposals: total,
        ..FailoverShapeMetrics::default()
    };
    let started = Instant::now();

    for _ in 0..FAILOVER_ROUNDS {
        let mut cluster = Cluster::elect();
        let old_leader = NodeId(1);
        let successor = NodeId(2);
        let mut submitted_at: BTreeMap<LogIndex, Instant> = BTreeMap::new();
        let mut next_index = cluster.leader().last_log_index();

        let inputs: Vec<RaftInput> = (0..FAILOVER_QUEUED_PROPOSALS)
            .map(|_| RaftInput::ClientProposal {
                payload: payload_of_size(PAYLOAD_BYTES),
            })
            .collect();
        let now = Instant::now();
        for _ in 0..FAILOVER_QUEUED_PROPOSALS {
            next_index = LogIndex(next_index.0 + 1);
            submitted_at.insert(next_index, now);
        }

        let outputs = cluster.step_leader_batch(inputs);
        cluster.replicate_to_successor_without_ack(outputs, successor, &mut failover_shape);
        assert_eq!(
            failover_shape.old_leader_applies, 0,
            "the old leader never applies the queued proposal burst"
        );

        let election_ticks = cluster.elect_with_partition(successor, old_leader, &mut |index| {
            if let Some(at) = submitted_at.remove(&index) {
                latencies.push(at.elapsed());
                failover_shape.successor_applies += 1;
            }
        });
        failover_shape.election_ticks += election_ticks;
        for _ in 0..16 {
            if submitted_at.is_empty() {
                break;
            }
            let outputs = cluster.step_leader_batch(vec![RaftInput::Tick]);
            cluster.pump_partitioned(outputs, old_leader, &mut |index| {
                if let Some(at) = submitted_at.remove(&index) {
                    latencies.push(at.elapsed());
                    failover_shape.successor_applies += 1;
                }
            });
        }
        assert!(
            submitted_at.is_empty(),
            "successor applies every queued proposal after failover"
        );
    }
    let elapsed = started.elapsed();
    assert_eq!(latencies.len(), total);

    WorkloadMetrics {
        name: "leader_failover_queued",
        proposals: total,
        payload_bytes: PAYLOAD_BYTES,
        max_in_flight: FAILOVER_QUEUED_PROPOSALS,
        elapsed,
        latencies,
        shape: None,
        service_shape: None,
        read_shape: None,
        codec_shape: None,
        multiraft_shape: None,
        failover_shape: Some(failover_shape),
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
        Self::elect_with_config(1, false)
    }

    fn elect_with_config(election_timeout_ticks: u64, lease_reads: bool) -> Self {
        let ids = [NodeId(1), NodeId(2), NodeId(3)];
        let mut nodes = BTreeMap::new();
        for id in ids {
            nodes.insert(id, open_node(id, &ids, election_timeout_ticks, lease_reads));
        }
        let mut cluster = Self {
            nodes,
            leader_id: NodeId(1),
        };

        // Node 1's election timeout fires first (jitter-free config, ticked
        // alone); the pre-vote poll and the vote requests it earns circulate
        // synchronously before the workload clock starts.
        for _ in 0..election_timeout_ticks {
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
            cluster.pump(
                tagged,
                &mut |_| {},
                &mut |_, _| {},
                &mut ShapeMetrics::default(),
            );
        }
        assert_eq!(cluster.leader().role(), Role::Leader, "node 1 wins");
        cluster
    }

    fn leader(&mut self) -> &mut BenchNode {
        self.nodes.get_mut(&self.leader_id).expect("leader exists")
    }

    fn node(&mut self, id: NodeId) -> &mut BenchNode {
        self.nodes.get_mut(&id).expect("node exists")
    }

    fn step_leader(&mut self, input: RaftInput) -> Vec<(NodeId, RaftOutput)> {
        let leader_id = self.leader_id;
        let outputs = self.leader().step(input).expect("leader step persists");
        outputs
            .into_iter()
            .map(|output| (leader_id, output))
            .collect()
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

    fn replicate_to_successor_without_ack(
        &mut self,
        outputs: Vec<(NodeId, RaftOutput)>,
        successor: NodeId,
        failover_shape: &mut FailoverShapeMetrics,
    ) {
        for (from, output) in outputs {
            match output {
                RaftOutput::Send {
                    to,
                    message: RaftMessage::AppendEntries(request),
                } if to == successor => {
                    failover_shape.successor_prefailover_append_messages += 1;
                    let responses = self
                        .node(to)
                        .step(RaftInput::Message {
                            from,
                            message: RaftMessage::AppendEntries(request),
                        })
                        .expect("successor append persists");
                    std::hint::black_box(responses);
                }
                RaftOutput::Apply { .. } if from == self.leader_id => {
                    failover_shape.old_leader_applies += 1;
                }
                _ => {}
            }
        }
    }

    fn elect_with_partition(
        &mut self,
        target: NodeId,
        partitioned: NodeId,
        on_leader_apply: &mut dyn FnMut(LogIndex),
    ) -> usize {
        self.leader_id = target;
        for tick in 1..=32 {
            let active_nodes = self
                .nodes
                .keys()
                .copied()
                .filter(|node_id| *node_id != partitioned)
                .collect::<Vec<_>>();
            let mut outputs = Vec::new();
            for node_id in active_nodes {
                outputs.extend(
                    self.node(node_id)
                        .step(RaftInput::Tick)
                        .expect("active voter election tick persists")
                        .into_iter()
                        .map(|output| (node_id, output)),
                );
            }
            self.pump_partitioned(outputs, partitioned, on_leader_apply);
            if self.node(target).role() == Role::Leader {
                return tick;
            }
        }
        panic!("successor did not win leadership under old-leader partition");
    }

    /// Synchronously routes messages until the cluster is quiescent,
    /// reporting every leader-side apply index to `on_leader_apply`.
    fn pump(
        &mut self,
        outputs: Vec<(NodeId, RaftOutput)>,
        on_leader_apply: &mut dyn FnMut(LogIndex),
        on_leader_read_grant: &mut dyn FnMut(ReadId, LogIndex),
        shape: &mut ShapeMetrics,
    ) {
        let mut queue: VecDeque<_> = outputs.into();
        while let Some((from, output)) = queue.pop_front() {
            record_shape(from, self.leader_id, &output, shape);
            match output {
                RaftOutput::Send { to, message } => {
                    if self.response_can_evaluate_commit(from, to, &message) {
                        shape.commit_evaluations += 1;
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
                RaftOutput::ReadIndexGranted {
                    read_id,
                    read_index,
                } if from == self.leader_id => {
                    on_leader_read_grant(read_id, read_index);
                }
                _ => {}
            }
        }
    }

    fn pump_partitioned(
        &mut self,
        outputs: Vec<(NodeId, RaftOutput)>,
        partitioned: NodeId,
        on_leader_apply: &mut dyn FnMut(LogIndex),
    ) {
        let mut queue: VecDeque<_> = outputs.into();
        while let Some((from, output)) = queue.pop_front() {
            match output {
                RaftOutput::Send { to, message } => {
                    if from == partitioned || to == partitioned {
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

    fn response_can_evaluate_commit(
        &self,
        from: NodeId,
        to: NodeId,
        message: &RaftMessage,
    ) -> bool {
        if to != self.leader_id {
            return false;
        }
        let RaftMessage::AppendEntriesResponse(response) = message else {
            return false;
        };
        if !response.success || response.follower_id != from {
            return false;
        }
        let Some(leader) = self.nodes.get(&to) else {
            return false;
        };
        if leader.role() != Role::Leader || response.term != leader.current_term() {
            return false;
        }

        let reported_match_index = std::cmp::min(response.match_index, leader.last_log_index());
        leader
            .leader_replication_progress()
            .into_iter()
            .find(|progress| progress.follower_id == from)
            .is_some_and(|progress| {
                let acknowledged = progress.match_index.max(reported_match_index);
                acknowledged > progress.match_index && acknowledged > leader.commit_index()
            })
    }
}

fn open_node(
    id: NodeId,
    ids: &[NodeId; 3],
    election_timeout_ticks: u64,
    lease_reads: bool,
) -> BenchNode {
    let peers: Vec<NodeId> = ids.iter().copied().filter(|peer| *peer != id).collect();
    let config = NodeConfig::new(id, peers, election_timeout_ticks)
        .expect("bench config is valid")
        .with_lease_reads(lease_reads);
    DurableRaftNode::new(config, InMemoryRaftHardStateStore::new()).expect("node hydrates")
}

fn record_shape(from: NodeId, leader_id: NodeId, output: &RaftOutput, shape: &mut ShapeMetrics) {
    shape.outputs += 1;
    let RaftOutput::Send {
        message: RaftMessage::AppendEntries(request),
        ..
    } = output
    else {
        return;
    };
    if from != leader_id {
        return;
    }
    shape.append_messages += 1;
    shape.append_entries += request.entries.len();
}

fn leader_broadcast_rounds(outputs: &[(NodeId, RaftOutput)], leader_id: NodeId) -> usize {
    outputs
        .iter()
        .filter_map(|(from, output)| {
            if *from != leader_id {
                return None;
            }
            match output {
                RaftOutput::Send {
                    message: RaftMessage::AppendEntries(request),
                    ..
                } => Some(request.sequence),
                _ => None,
            }
        })
        .collect::<BTreeSet<_>>()
        .len()
}
