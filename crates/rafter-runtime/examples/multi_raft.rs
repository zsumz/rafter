//! Multi-raft: hundreds of independent Raft groups in one OS process.
//!
//! Sharded databases run many consensus groups ("shards") per process: each
//! group is an independent 3-voter Raft, but all groups share the process's
//! tick driver, message router, and storage directory tree. This example
//! builds that pattern with three in-process "hosts" — stand-ins for three
//! server processes — where host `h` owns replica `node-h` of EVERY group,
//! one `DurableRaftNode` per (host, group), file-backed under the layout a
//! sharded database would use:
//!
//! ```text
//! <tempdir>/host-<h>/group-<g>/hard-state   (file)
//! <tempdir>/host-<h>/group-<g>/log          (file)
//! <tempdir>/host-<h>/group-<g>/snapshots/   (directory)
//! ```
//!
//! Messages route as `(group, from, to, message)`. The driving loop delivers
//! each wave in destination batches through `step_batch`, so a burst of
//! traffic to one replica costs one durable flush — group commit per
//! replica, the only fsync amortization available while every group owns
//! its own files (a WAL shared across groups is the classical next step).
//! It is not a production network, authentication, shared-WAL, or app-state
//! schema template.
//!
//! The nodes run the default production posture: pre-vote and check-quorum
//! are enabled by `NodeConfig` defaults, so every election below goes
//! through a pre-vote round and leaders stay leaders only while the tick
//! driver keeps pumping follower responses back to them.
//!
//! Phases:
//! 1. open      — open file-backed stores for every (host, group) replica;
//!    the standard store bundle batches fresh log/snapshot creation syncs
//! 2. elect     — tick every group's host-1 replica to its timeout, then
//!    pump: hundreds of pre-vote elections storm through one router queue
//! 3. steady    — shared tick driver: each pass ticks every leader once;
//!    heartbeat coalescing creates zero-message passes while check-quorum
//!    stays satisfied
//! 4. workload  — proposals round-robin across groups, commit them all, then
//!    carry the commit floors on the next coalesced heartbeat
//! 5. snapshot  — compact group 0 through a snapshot; others untouched
//! 6. restart   — drop host 3 entirely, reopen every store from disk
//! 7. recovery  — every group commits again; host 3 rebuilds its ledgers
//!
//! Run with `cargo run --release -p rafter-runtime --example multi_raft --
//! [groups]` (default 256). The report prints wall time, node steps, routed
//! messages, applied entries, and approximate resident memory per phase, so
//! a reader can see memory grow linearly in the number of groups.

use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::time::Instant;

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, Input,
    LogIndex, Message, NodeConfig, NodeId, Output, RaftSnapshotMetadata, Role, SnapshotGroupId,
};
use rafter_runtime::DurableRaftNode;
use rafter_storage::{
    FileRaftHardStateStore, FileRaftLogSegment, FileRaftNodeStores, FileRaftSnapshotStore,
    PersistedRaftSnapshot,
};

const HOST_COUNT: usize = 3;
const DEFAULT_GROUPS: usize = 256;
const PROPOSALS_PER_GROUP: usize = 4;
const STEADY_TICK_PASSES: usize = 8;
const ELECTION_TIMEOUT_TICKS: u64 = 3;
const HEARTBEAT_INTERVAL_TICKS: u64 = ELECTION_TIMEOUT_TICKS - 1;

/// One replica's full durable footprint: hard-state file, log file, and
/// snapshot directory, all under `<root>/host-<h>/group-<g>/`.
type FileBackedNode =
    DurableRaftNode<FileRaftHardStateStore, FileRaftLogSegment, FileRaftSnapshotStore>;

/// A message in flight between two replicas of one group.
struct Envelope {
    group: usize,
    from: NodeId,
    to: NodeId,
    message: Message,
}

/// One "process host": replica `node-<h>` of every group plus the applied
/// ledger its state machine would hold per group.
struct Host {
    nodes: Vec<FileBackedNode>,
    applied: Vec<Vec<Vec<u8>>>,
}

impl Host {
    /// Opens (or reopens after a crash/restart) every group replica this
    /// host is responsible for, straight from the on-disk layout.
    fn open(root: &Path, host_index: usize, groups: usize) -> Self {
        let nodes = (0..groups)
            .map(|group| open_node(root, host_index, group))
            .collect();
        Self {
            nodes,
            applied: vec![Vec::new(); groups],
        }
    }
}

#[derive(Clone, Copy, Default)]
struct Stats {
    /// `step_batch` calls — each one allocates at least one `Vec<Output>`
    /// and ends in at most one durable flush per changed store.
    steps: u64,
    /// Messages routed between replicas.
    messages: u64,
    /// Committed entries handed to state machines.
    applies: u64,
}

struct PhaseRow {
    name: &'static str,
    seconds: f64,
    steps: u64,
    messages: u64,
    applies: u64,
    rss_kilobytes: u64,
}

struct Cluster {
    root: PathBuf,
    groups: usize,
    hosts: Vec<Host>,
    queue: VecDeque<Envelope>,
    /// Per-group expected ledger: every payload proposed, in commit order.
    expected: Vec<Vec<Vec<u8>>>,
    stats: Stats,
}

fn node_id_for(host_index: usize) -> NodeId {
    NodeId(host_index as u64 + 1)
}

fn host_index_for(node: NodeId) -> usize {
    usize::try_from(node.0).expect("host ids fit in usize") - 1
}

fn open_node(root: &Path, host_index: usize, group: usize) -> FileBackedNode {
    let node_id = node_id_for(host_index);
    let dir = root
        .join(format!("host-{}", node_id.0))
        .join(format!("group-{group}"));
    std::fs::create_dir_all(&dir).expect("create replica directory");
    let (hard_state, log, snapshots) = FileRaftNodeStores::open(&dir)
        .expect("open file-backed node stores")
        .into_parts();
    let peers = (0..HOST_COUNT)
        .filter(|other| *other != host_index)
        .map(node_id_for)
        .collect();
    // Defaults are the production posture: pre-vote and check-quorum ON.
    let config = NodeConfig::new(node_id, peers, ELECTION_TIMEOUT_TICKS)
        .expect("static 3-voter configuration is valid")
        .with_heartbeat_interval_ticks(HEARTBEAT_INTERVAL_TICKS);
    DurableRaftNode::with_storage_and_snapshot_store(config, hard_state, log, snapshots)
        .expect("hydrate durable node from its on-disk stores")
}

impl Cluster {
    fn new(root: PathBuf, groups: usize) -> Self {
        Self {
            root,
            groups,
            hosts: Vec::new(),
            queue: VecDeque::new(),
            expected: vec![Vec::new(); groups],
            stats: Stats::default(),
        }
    }

    /// Steps one replica with a batch of inputs (one durable flush per
    /// changed store) and routes every output: sends into the router queue,
    /// applies into the host's per-group ledger.
    fn step_node(&mut self, host_index: usize, group: usize, inputs: Vec<Input>) {
        let from = node_id_for(host_index);
        let outputs = self.hosts[host_index].nodes[group]
            .step_batch(inputs)
            .expect("step persists durably");
        self.stats.steps += 1;
        for output in outputs {
            match output {
                Output::Send { to, message } => {
                    self.stats.messages += 1;
                    self.queue.push_back(Envelope {
                        group,
                        from,
                        to,
                        message,
                    });
                }
                Output::Apply { payload, .. } => {
                    self.stats.applies += 1;
                    self.hosts[host_index].applied[group].push(payload.as_slice().to_vec());
                }
                other => panic!("output this example never provokes: {other:?}"),
            }
        }
    }

    fn tick(&mut self, host_index: usize, group: usize) {
        self.step_node(host_index, group, vec![Input::Tick]);
    }

    /// Delivers queued messages wave by wave until the cluster is quiescent.
    /// Each wave is grouped by destination replica so one `step_batch` call
    /// — one durable flush — absorbs every message bound for that replica.
    fn pump(&mut self) {
        while !self.queue.is_empty() {
            let mut batches: BTreeMap<(usize, usize), Vec<Input>> = BTreeMap::new();
            while let Some(envelope) = self.queue.pop_front() {
                batches
                    .entry((host_index_for(envelope.to), envelope.group))
                    .or_default()
                    .push(Input::Message {
                        from: envelope.from,
                        message: envelope.message,
                    });
            }
            for ((host_index, group), inputs) in batches {
                self.step_node(host_index, group, inputs);
            }
        }
    }

    fn leader(&self, group: usize) -> &FileBackedNode {
        &self.hosts[0].nodes[group]
    }
}

// ---------------------------------------------------------------------------
// Phases
// ---------------------------------------------------------------------------

/// Phase 1: open every (host, group) store from the shared directory tree.
fn phase_open(cluster: &mut Cluster) {
    for host_index in 0..HOST_COUNT {
        let host = Host::open(&cluster.root, host_index, cluster.groups);
        cluster.hosts.push(host);
    }
}

/// Phase 2: tick every group's host-1 replica to its election timeout, then
/// pump once. Every group's pre-vote round, real election, and first
/// heartbeat exchange flow through the single router queue back to back —
/// the startup thundering herd a multi-raft process must absorb.
fn phase_elect(cluster: &mut Cluster) {
    for group in 0..cluster.groups {
        for _ in 0..ELECTION_TIMEOUT_TICKS {
            cluster.tick(0, group);
        }
    }
    cluster.pump();
    for group in 0..cluster.groups {
        assert_eq!(
            cluster.leader(group).role(),
            Role::Leader,
            "group {group}: host-1 replica should have won its pre-vote election"
        );
        assert_eq!(cluster.hosts[1].nodes[group].role(), Role::Follower);
        assert_eq!(cluster.hosts[2].nodes[group].role(), Role::Follower);
    }
}

/// Phase 3: the shared tick driver. One pass ticks every leader replica
/// once and pumps to quiescence. Heartbeat coalescing makes some passes
/// silent; the broadcast passes still return quorum evidence before the
/// check-quorum deadline.
fn phase_steady(cluster: &mut Cluster) {
    let mut quiesced_passes = 0;
    for _ in 0..STEADY_TICK_PASSES {
        let messages_before = cluster.stats.messages;
        for group in 0..cluster.groups {
            cluster.tick(0, group);
        }
        cluster.pump();
        if cluster.stats.messages == messages_before {
            quiesced_passes += 1;
        }
    }
    assert!(
        quiesced_passes > 0,
        "heartbeat coalescing should produce zero-message steady tick passes"
    );
    for group in 0..cluster.groups {
        assert_eq!(
            cluster.leader(group).role(),
            Role::Leader,
            "group {group}: leader should survive check-quorum under the shared tick driver"
        );
    }
}

/// Phase 4: proposals round-robin across groups — the access pattern of a
/// sharded workload — then one pump commits the leaders. Followers observe the
/// commit floors on the next coalesced heartbeat pass.
fn phase_workload(cluster: &mut Cluster) {
    for round in 0..PROPOSALS_PER_GROUP {
        for group in 0..cluster.groups {
            let payload = format!("group-{group} entry-{round}").into_bytes();
            cluster.expected[group].push(payload.clone());
            cluster.step_node(0, group, vec![Input::ClientProposal { payload }]);
        }
    }
    cluster.pump();
    flush_commit_notifications(cluster);
    verify_ledgers(cluster);
}

/// Phase 5: snapshot + compact exactly one group on its leader and prove
/// the other groups' durable state is untouched — groups fail, compact,
/// and snapshot independently even though they share a directory tree.
fn phase_snapshot_group_zero(cluster: &mut Cluster) {
    let boundary = cluster.leader(0).commit_index();
    // Every entry in this run committed in the leader's single elected term,
    // so the boundary term is the current term; `compact_log_with_snapshot`
    // re-validates this against the log and would refuse a mismatch.
    let term = cluster.leader(0).current_term();
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("group-0").expect("valid snapshot group id"),
        NodeId(1),
        boundary,
        term,
        term,
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("ledger-v1").expect("valid snapshot kind"),
            ApplicationSnapshotVersion::new(1).expect("valid snapshot version"),
        ),
    )
    .expect("snapshot metadata describes the committed prefix");
    let application_payload = cluster.expected[0].join(&b'\n');
    cluster.hosts[0].nodes[0]
        .compact_log_with_snapshot(PersistedRaftSnapshot {
            metadata,
            application_payload,
        })
        .expect("compact group 0 through its snapshot");

    assert_eq!(cluster.leader(0).snapshot_index(), boundary);
    for group in 1..cluster.groups {
        assert_eq!(
            cluster.leader(group).snapshot_index(),
            LogIndex::ZERO,
            "group {group}: snapshotting group 0 must not touch other groups"
        );
    }
}

/// Phase 6: restart host 3 — drop all of its replicas (closing every file)
/// and reopen each one from disk through the same constructors, exactly as
/// a crashed process would on its way back up.
fn phase_restart_host_three(cluster: &mut Cluster) {
    cluster.hosts[2] = Host::open(&cluster.root, 2, cluster.groups);
    for group in 0..cluster.groups {
        assert_eq!(
            cluster.hosts[2].nodes[group].last_log_index(),
            cluster.leader(group).last_log_index(),
            "group {group}: host 3 should recover its full acknowledged log"
        );
        assert_eq!(
            cluster.hosts[2].nodes[group].current_term(),
            cluster.leader(group).current_term(),
            "group {group}: host 3 should recover its persisted term"
        );
    }
}

/// Phase 7: every group commits one more entry. The next coalesced heartbeat
/// re-establishes contact with restarted host 3, whose commit index — and
/// applied ledger — rebuild from zero as the leader's commit floor arrives.
fn phase_recovery(cluster: &mut Cluster) {
    for group in 0..cluster.groups {
        let payload = format!("group-{group} recovery").into_bytes();
        cluster.expected[group].push(payload.clone());
        cluster.step_node(0, group, vec![Input::ClientProposal { payload }]);
    }
    cluster.pump();
    flush_commit_notifications(cluster);
    verify_ledgers(cluster);
}

fn flush_commit_notifications(cluster: &mut Cluster) {
    for _ in 0..HEARTBEAT_INTERVAL_TICKS {
        for group in 0..cluster.groups {
            cluster.tick(0, group);
        }
        cluster.pump();
    }
}

/// Every host's applied ledger for every group must equal the proposal
/// history for that group, independently of every other group.
fn verify_ledgers(cluster: &Cluster) {
    for group in 0..cluster.groups {
        for (host_index, host) in cluster.hosts.iter().enumerate() {
            assert_eq!(
                host.applied[group],
                cluster.expected[group],
                "host {} group {group}: applied ledger diverged",
                host_index + 1
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

/// Approximate resident set size, in kilobytes, read from `ps`. Spawning a
/// child for a memory reading is acceptable in an example; a server would
/// use a proper metrics source.
fn rss_kilobytes() -> u64 {
    let output = std::process::Command::new("ps")
        .args(["-o", "rss=", "-p"])
        .arg(std::process::id().to_string())
        .output()
        .expect("run ps for an RSS reading");
    String::from_utf8_lossy(&output.stdout)
        .trim()
        .parse()
        .unwrap_or(0)
}

fn run_phase(
    cluster: &mut Cluster,
    name: &'static str,
    phase: impl FnOnce(&mut Cluster),
) -> PhaseRow {
    let before = cluster.stats;
    let start = Instant::now();
    phase(cluster);
    let seconds = start.elapsed().as_secs_f64();
    let after = cluster.stats;
    PhaseRow {
        name,
        seconds,
        steps: after.steps - before.steps,
        messages: after.messages - before.messages,
        applies: after.applies - before.applies,
        rss_kilobytes: rss_kilobytes(),
    }
}

// Report-only arithmetic: precision loss in averages is irrelevant.
#[allow(clippy::cast_precision_loss)]
fn print_report(cluster: &Cluster, baseline_rss_kilobytes: u64, rows: &[PhaseRow]) {
    let groups = cluster.groups;
    println!(
        "\n{groups} groups x 3 replicas = {} durable nodes",
        groups * 3
    );
    println!(
        "{:<10} {:>9} {:>10} {:>10} {:>10} {:>9}",
        "phase", "seconds", "steps", "messages", "applies", "rss-mb"
    );
    for row in rows {
        println!(
            "{:<10} {:>9.3} {:>10} {:>10} {:>10} {:>9.1}",
            row.name,
            row.seconds,
            row.steps,
            row.messages,
            row.applies,
            row.rss_kilobytes as f64 / 1024.0
        );
    }

    let total_committed = groups * (PROPOSALS_PER_GROUP + 1);
    println!(
        "\ntotal proposals committed: {total_committed} ({} per group)",
        PROPOSALS_PER_GROUP + 1
    );

    if let Some(steady) = rows.iter().find(|row| row.name == "steady") {
        let passes = STEADY_TICK_PASSES as f64;
        println!(
            "steady state, per tick pass over {groups} leaders: {:.0} steps, {:.0} messages ({:.1} messages per group per pass)",
            steady.steps as f64 / passes,
            steady.messages as f64 / passes,
            steady.messages as f64 / passes / groups as f64,
        );
    }

    let final_rss = rows.last().map_or(0, |row| row.rss_kilobytes);
    let delta = final_rss.saturating_sub(baseline_rss_kilobytes);
    println!(
        "resident memory: {:.1} MB baseline, {:.1} MB final; growth {:.1} MB total = ~{:.1} KB per group ({} bytes per replica)",
        baseline_rss_kilobytes as f64 / 1024.0,
        final_rss as f64 / 1024.0,
        delta as f64 / 1024.0,
        delta as f64 / groups as f64,
        delta * 1024 / (groups as u64 * 3),
    );
}

fn main() {
    let groups: usize = std::env::args().nth(1).map_or(DEFAULT_GROUPS, |raw| {
        raw.parse()
            .expect("GROUPS argument must be a positive integer")
    });
    assert!(groups > 0, "at least one group is required");

    let root = std::env::temp_dir().join(format!("rafter-multi-raft-{}", std::process::id()));
    let _cleanup = TempDirGuard(root.clone());
    let baseline_rss = rss_kilobytes();

    let mut cluster = Cluster::new(root, groups);
    let rows = vec![
        run_phase(&mut cluster, "open", phase_open),
        run_phase(&mut cluster, "elect", phase_elect),
        run_phase(&mut cluster, "steady", phase_steady),
        run_phase(&mut cluster, "workload", phase_workload),
        run_phase(&mut cluster, "snapshot", phase_snapshot_group_zero),
        run_phase(&mut cluster, "restart", phase_restart_host_three),
        run_phase(&mut cluster, "recovery", phase_recovery),
    ];

    print_report(&cluster, baseline_rss, &rows);
    println!(
        "\nall {groups} groups elected, committed, snapshotted/survived restart, and recovered"
    );
}

/// Removes the example's temporary directory tree on exit, including exits
/// by panic while unwinding.
struct TempDirGuard(PathBuf);

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
