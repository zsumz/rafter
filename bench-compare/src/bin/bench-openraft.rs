//! openraft harness: three `Raft` instances on a current-thread tokio
//! runtime, with hand-rolled in-memory `RaftLogStorage`/`RaftStateMachine`
//! implementations (the `storage-v2` traits, following the upstream memstore
//! pattern) and a `RaftNetwork` that routes RPCs directly to the target
//! `Raft` handle in the same process.
//!
//! Commit latency is measured from proposal submission (immediately before
//! `client_write`) to `client_write` returning, which is openraft's
//! linearizable acknowledgement: the entry is committed and applied to the
//! leader's state machine before the response resolves.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bench_compare::{
    payload_of_size, report_json, WorkloadMetrics, LARGE_PAYLOAD_BYTES,
    LARGE_PAYLOAD_PIPELINE_DEPTH, LARGE_PAYLOAD_PROPOSALS, PAYLOAD_BYTES, PIPELINED_PROPOSALS,
    PIPELINE_DEPTH, SERIAL_PROPOSALS,
};
use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{LogFlushed, LogState, RaftLogStorage, RaftStateMachine};
use openraft::{
    BasicNode, Config, Entry, EntryPayload, LogId, OptionalSend, Raft, RaftLogReader, RaftNetwork,
    RaftNetworkFactory, RaftSnapshotBuilder, ServerState, Snapshot, SnapshotMeta, SnapshotPolicy,
    StorageError, StoredMembership, Vote,
};

openraft::declare_raft_types!(
    /// Bench type config: 512-byte opaque payloads, unit responses.
    pub TypeConfig:
        D = Vec<u8>,
        R = (),
);

type NodeId = u64;

const VOTERS: [NodeId; 3] = [1, 2, 3];
const LEADER: NodeId = 1;

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread tokio runtime");
    let report = runtime.block_on(async {
        let serial = proposal_workload("serial", SERIAL_PROPOSALS, PAYLOAD_BYTES, 1).await;
        let pipelined = proposal_workload(
            "pipelined",
            PIPELINED_PROPOSALS,
            PAYLOAD_BYTES,
            PIPELINE_DEPTH,
        )
        .await;
        let large_payload = proposal_workload(
            "large_payload",
            LARGE_PAYLOAD_PROPOSALS,
            LARGE_PAYLOAD_BYTES,
            LARGE_PAYLOAD_PIPELINE_DEPTH,
        )
        .await;
        report_json(
            "openraft",
            "0.9.24 (features: storage-v2; current-thread tokio)",
            "client_write submitted -> client_write response (committed and applied on leader)",
            &[serial, pipelined, large_payload],
        )
    });
    println!("{report}");
}

/// Drives `total` proposals through the elected leader in submission bursts
/// of at most `window` concurrent `client_write` calls, waiting for every
/// write in a burst to be acknowledged before submitting the next burst.
async fn proposal_workload(
    name: &'static str,
    total: usize,
    payload_bytes: usize,
    window: usize,
) -> WorkloadMetrics {
    let cluster = Cluster::start().await;
    let leader = cluster.leader();

    // Warmup barrier: the election's blank + membership entries are applied
    // before the clock starts, matching the other harnesses, which finish
    // their elections synchronously before timing.
    leader
        .client_write(Vec::new())
        .await
        .expect("warmup write commits");

    let mut latencies = Vec::with_capacity(total);
    let started = Instant::now();
    if window == 1 {
        for _ in 0..total {
            let submitted = Instant::now();
            leader
                .client_write(payload_of_size(payload_bytes))
                .await
                .expect("client_write commits");
            latencies.push(submitted.elapsed());
        }
    } else {
        let mut remaining = total;
        while remaining > 0 {
            let batch = remaining.min(window);
            remaining -= batch;
            let mut joins = tokio::task::JoinSet::new();
            // One clock for the whole burst, matching the other two
            // harnesses: latency runs from burst submission, including any
            // scheduler queueing after spawn.
            let submitted = Instant::now();
            for _ in 0..batch {
                let raft = leader.clone();
                joins.spawn(async move {
                    raft.client_write(payload_of_size(payload_bytes))
                        .await
                        .expect("client_write commits");
                    submitted.elapsed()
                });
            }
            while let Some(joined) = joins.join_next().await {
                latencies.push(joined.expect("write task joins"));
            }
        }
    }
    let elapsed = started.elapsed();
    cluster.shutdown().await;

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
    rafts: BTreeMap<NodeId, Raft<TypeConfig>>,
}

impl Cluster {
    /// Three voters with in-memory stores; node 1 is initialized with the
    /// full membership and campaigns immediately, so it wins the first
    /// election against its pristine peers.
    async fn start() -> Self {
        let config = Arc::new(
            Config {
                cluster_name: "bench-compare".to_string(),
                // Long timeouts keep followers from campaigning under load;
                // replication traffic constantly refreshes their timers.
                election_timeout_min: 1_000,
                election_timeout_max: 2_000,
                heartbeat_interval: 100,
                // The other harnesses never snapshot or purge; disable
                // openraft's automatic snapshot building for parity.
                snapshot_policy: SnapshotPolicy::Never,
                ..Config::default()
            }
            .validate()
            .expect("valid openraft config"),
        );

        let registry: Registry = Arc::new(Mutex::new(BTreeMap::new()));
        let mut rafts = BTreeMap::new();
        for id in VOTERS {
            let raft = Raft::new(
                id,
                config.clone(),
                Router {
                    registry: registry.clone(),
                },
                MemLogStore::default(),
                MemStateMachine::default(),
            )
            .await
            .expect("raft node starts");
            registry
                .lock()
                .expect("registry lock")
                .insert(id, raft.clone());
            rafts.insert(id, raft);
        }

        let members: BTreeMap<NodeId, BasicNode> = VOTERS
            .iter()
            .map(|id| (*id, BasicNode::default()))
            .collect();
        rafts[&LEADER]
            .initialize(members)
            .await
            .expect("cluster initializes");
        rafts[&LEADER]
            .wait(Some(Duration::from_secs(10)))
            .state(ServerState::Leader, "node 1 leads")
            .await
            .expect("node 1 becomes leader");

        Self { rafts }
    }

    fn leader(&self) -> Raft<TypeConfig> {
        self.rafts[&LEADER].clone()
    }

    async fn shutdown(self) {
        for raft in self.rafts.into_values() {
            let _ = raft.shutdown().await;
        }
    }
}

// ---------------------------------------------------------------------------
// In-process network: RPCs are direct calls on the target Raft handle.
// ---------------------------------------------------------------------------

type Registry = Arc<Mutex<BTreeMap<NodeId, Raft<TypeConfig>>>>;

#[derive(Clone)]
struct Router {
    registry: Registry,
}

impl RaftNetworkFactory<TypeConfig> for Router {
    type Network = Connection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        Connection {
            registry: self.registry.clone(),
            target,
        }
    }
}

struct Connection {
    registry: Registry,
    target: NodeId,
}

impl Connection {
    /// Clones the target handle out of the registry; the guard is dropped
    /// before any await point.
    fn target_raft(&self) -> Raft<TypeConfig> {
        self.registry
            .lock()
            .expect("registry lock")
            .get(&self.target)
            .expect("target node is registered")
            .clone()
    }
}

impl RaftNetwork<TypeConfig> for Connection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.target_raft()
            .append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, InstallSnapshotError>>,
    > {
        self.target_raft()
            .install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.target_raft()
            .vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }
}

// ---------------------------------------------------------------------------
// In-memory log store (RaftLogStorage) following the memstore pattern.
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct LogInner {
    log: BTreeMap<u64, Entry<TypeConfig>>,
    committed: Option<LogId<NodeId>>,
    vote: Option<Vote<NodeId>>,
    last_purged: Option<LogId<NodeId>>,
}

#[derive(Debug, Default, Clone)]
struct MemLogStore {
    inner: Arc<Mutex<LogInner>>,
}

impl RaftLogReader<TypeConfig> for MemLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock().expect("log lock");
        Ok(inner
            .log
            .range(range)
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl RaftLogStorage<TypeConfig> for MemLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock().expect("log lock");
        let last_in_log = inner.log.iter().next_back().map(|(_, entry)| entry.log_id);
        let last_log_id = last_in_log.or(inner.last_purged);
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.inner.lock().expect("log lock").vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().expect("log lock").vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.inner.lock().expect("log lock").committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().expect("log lock").committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let mut inner = self.inner.lock().expect("log lock");
            for entry in entries {
                inner.log.insert(entry.log_id.index, entry);
            }
        }
        // Everything is in memory by design; report the write as flushed.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().expect("log lock");
        // Keep entries strictly below `log_id.index`.
        inner.log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().expect("log lock");
        inner.last_purged = Some(log_id);
        inner.log = inner.log.split_off(&(log_id.index + 1));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// In-memory state machine (RaftStateMachine): counts applied payload bytes.
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct StateMachineInner {
    applied: Option<LogId<NodeId>>,
    membership: StoredMembership<NodeId, BasicNode>,
    applied_bytes: u64,
}

#[derive(Debug, Default, Clone)]
struct MemStateMachine {
    inner: Arc<Mutex<StateMachineInner>>,
}

impl RaftSnapshotBuilder<TypeConfig> for MemStateMachine {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        // Never triggered: the config pins SnapshotPolicy::Never and nothing
        // calls trigger_snapshot. Kept honest for completeness.
        let inner = self.inner.lock().expect("state machine lock");
        Ok(Snapshot {
            meta: SnapshotMeta {
                last_log_id: inner.applied,
                last_membership: inner.membership.clone(),
                snapshot_id: format!("bench-{}", inner.applied.map_or(0, |id| id.index)),
            },
            snapshot: Box::new(Cursor::new(Vec::new())),
        })
    }
}

impl RaftStateMachine<TypeConfig> for MemStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let inner = self.inner.lock().expect("state machine lock");
        Ok((inner.applied, inner.membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<()>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut inner = self.inner.lock().expect("state machine lock");
        let mut replies = Vec::new();
        for entry in entries {
            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(data) => inner.applied_bytes += data.len() as u64,
                EntryPayload::Membership(membership) => {
                    inner.membership = StoredMembership::new(Some(entry.log_id), membership);
                }
            }
            inner.applied = Some(entry.log_id);
            replies.push(());
        }
        Ok(replies)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        _snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().expect("state machine lock");
        inner.applied = meta.last_log_id;
        inner.membership = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        Ok(None)
    }
}
