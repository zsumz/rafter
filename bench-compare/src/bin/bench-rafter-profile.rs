//! Rafter serial profiling harness.
//!
//! This binary is deliberately separate from `bench-rafter`: it installs a
//! counting allocator, so it must not perturb the comparison scoreboard. The
//! workload mirrors the serial Rafter benchmark and reports coarse phase
//! timings plus allocation deltas. It is a portable fallback for development
//! environments without `perf`, `cargo-flamegraph`, valgrind, or heaptrack.

use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use bench_compare::{payload_of_size, PAYLOAD_BYTES, SERIAL_PROPOSALS};
use rafter::{
    Input as RaftInput, LogIndex, NodeConfig, NodeId, Output as RaftOutput, Role,
};
use rafter_runtime::DurableRaftNode;
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
};
use std::sync::atomic::{AtomicU64, Ordering};

type BenchNode =
    DurableRaftNode<InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore>;

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

static ALLOCS: AtomicU64 = AtomicU64::new(0);
static DEALLOCS: AtomicU64 = AtomicU64::new(0);
static REALLOCS: AtomicU64 = AtomicU64::new(0);
static ALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);
static DEALLOCATED_BYTES: AtomicU64 = AtomicU64::new(0);

struct CountingAllocator;

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        DEALLOCS.fetch_add(1, Ordering::Relaxed);
        DEALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) };
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let next = unsafe { System.realloc(ptr, layout, new_size) };
        if !next.is_null() {
            REALLOCS.fetch_add(1, Ordering::Relaxed);
            ALLOCATED_BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
            DEALLOCATED_BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        next
    }
}

#[derive(Clone, Copy, Default)]
struct AllocSnapshot {
    allocs: u64,
    deallocs: u64,
    reallocs: u64,
    allocated_bytes: u64,
    deallocated_bytes: u64,
}

impl AllocSnapshot {
    fn now() -> Self {
        Self {
            allocs: ALLOCS.load(Ordering::Relaxed),
            deallocs: DEALLOCS.load(Ordering::Relaxed),
            reallocs: REALLOCS.load(Ordering::Relaxed),
            allocated_bytes: ALLOCATED_BYTES.load(Ordering::Relaxed),
            deallocated_bytes: DEALLOCATED_BYTES.load(Ordering::Relaxed),
        }
    }

    fn delta_from(self, before: Self) -> Self {
        Self {
            allocs: self.allocs.saturating_sub(before.allocs),
            deallocs: self.deallocs.saturating_sub(before.deallocs),
            reallocs: self.reallocs.saturating_sub(before.reallocs),
            allocated_bytes: self.allocated_bytes.saturating_sub(before.allocated_bytes),
            deallocated_bytes: self.deallocated_bytes.saturating_sub(before.deallocated_bytes),
        }
    }
}

#[derive(Default)]
struct PhaseProfile {
    calls: u64,
    elapsed: Duration,
    allocs: u64,
    deallocs: u64,
    reallocs: u64,
    allocated_bytes: u64,
    deallocated_bytes: u64,
}

impl PhaseProfile {
    fn observe<T>(&mut self, operation: impl FnOnce() -> T) -> T {
        let alloc_before = AllocSnapshot::now();
        let started = Instant::now();
        let value = operation();
        let elapsed = started.elapsed();
        let alloc_delta = AllocSnapshot::now().delta_from(alloc_before);

        self.record(elapsed, alloc_delta);
        value
    }

    fn record(&mut self, elapsed: Duration, alloc_delta: AllocSnapshot) {
        self.calls += 1;
        self.elapsed += elapsed;
        self.allocs += alloc_delta.allocs;
        self.deallocs += alloc_delta.deallocs;
        self.reallocs += alloc_delta.reallocs;
        self.allocated_bytes += alloc_delta.allocated_bytes;
        self.deallocated_bytes += alloc_delta.deallocated_bytes;
    }
}

#[derive(Default)]
struct SerialProfile {
    payload: PhaseProfile,
    submit_tracking: PhaseProfile,
    leader_step: PhaseProfile,
    pump_total: PhaseProfile,
    follower_step: PhaseProfile,
    queue_extend: PhaseProfile,
    applies: usize,
}

fn main() {
    let proposals = std::env::var("BENCH_RAFTER_PROFILE_PROPOSALS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(SERIAL_PROPOSALS);
    let payload_bytes = std::env::var("BENCH_RAFTER_PROFILE_PAYLOAD_BYTES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(PAYLOAD_BYTES);

    let report = profile_serial(proposals, payload_bytes);
    print_report(&report);
}

struct ProfileReport {
    proposals: usize,
    payload_bytes: usize,
    elapsed: Duration,
    allocations: AllocSnapshot,
    profile: SerialProfile,
}

fn profile_serial(proposals: usize, payload_bytes: usize) -> ProfileReport {
    let mut cluster = Cluster::elect();
    let mut profile = SerialProfile::default();
    let mut submitted_at: BTreeMap<LogIndex, Instant> = BTreeMap::new();
    let mut next_index = cluster.leader().last_log_index();

    let alloc_before = AllocSnapshot::now();
    let started = Instant::now();

    for _ in 0..proposals {
        let payload = profile
            .payload
            .observe(|| payload_of_size(payload_bytes));
        let input = RaftInput::ClientProposal { payload };

        let now = Instant::now();
        profile.submit_tracking.observe(|| {
            next_index = LogIndex(next_index.0 + 1);
            submitted_at.insert(next_index, now);
        });

        let outputs = profile
            .leader_step
            .observe(|| cluster.step_leader(input));

        let alloc_before = AllocSnapshot::now();
        let pump_started = Instant::now();
        let mut applied = 0usize;
        cluster.pump(outputs, &mut profile, &mut |index| {
            if submitted_at.remove(&index).is_some() {
                applied += 1;
            }
        });
        profile.pump_total.record(
            pump_started.elapsed(),
            AllocSnapshot::now().delta_from(alloc_before),
        );
        profile.applies += applied;
    }

    let elapsed = started.elapsed();
    let allocations = AllocSnapshot::now().delta_from(alloc_before);
    assert!(
        submitted_at.is_empty(),
        "every proposal commits and applies at the leader"
    );
    assert_eq!(
        profile.applies, proposals,
        "leader applies every serial proposal"
    );
    ProfileReport {
        proposals,
        payload_bytes,
        elapsed,
        allocations,
        profile,
    }
}

struct Cluster {
    nodes: BTreeMap<NodeId, BenchNode>,
    leader_id: NodeId,
}

impl Cluster {
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
        let mut profile = SerialProfile::default();
        cluster.pump(tagged, &mut profile, &mut |_| {});
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

    fn pump(
        &mut self,
        outputs: Vec<(NodeId, RaftOutput)>,
        profile: &mut SerialProfile,
        on_leader_apply: &mut dyn FnMut(LogIndex),
    ) {
        let mut queue: VecDeque<_> = outputs.into();
        while let Some((from, output)) = queue.pop_front() {
            match output {
                RaftOutput::Send { to, message } => {
                    let responses = profile.follower_step.observe(|| {
                        self.node(to)
                            .step(RaftInput::Message { from, message })
                            .expect("message step persists")
                    });
                    profile.queue_extend.observe(|| {
                        queue.extend(responses.into_iter().map(|response| (to, response)));
                    });
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
    let config = NodeConfig::new(id, peers, 1).expect("bench config is valid");
    DurableRaftNode::new(config, InMemoryRaftHardStateStore::new()).expect("node hydrates")
}

fn print_report(report: &ProfileReport) {
    println!(
        "profile=rafter-serial proposals={} payload_bytes={} elapsed_ms={:.3} proposals_per_s={:.0}",
        report.proposals,
        report.payload_bytes,
        report.elapsed.as_secs_f64() * 1_000.0,
        report.proposals as f64 / report.elapsed.as_secs_f64()
    );
    println!(
        "total_allocations allocs={} reallocs={} deallocs={} allocated_bytes={} deallocated_bytes={} allocs_per_proposal={:.3} allocated_bytes_per_proposal={:.1}",
        report.allocations.allocs,
        report.allocations.reallocs,
        report.allocations.deallocs,
        report.allocations.allocated_bytes,
        report.allocations.deallocated_bytes,
        report.allocations.allocs as f64 / report.proposals as f64,
        report.allocations.allocated_bytes as f64 / report.proposals as f64,
    );
    println!(
        "{:<18} {:>8} {:>10} {:>7} {:>10} {:>10} {:>10} {:>14}",
        "phase", "calls", "ms", "pct", "allocs", "reallocs", "bytes", "allocs/call"
    );
    print_phase("payload", &report.profile.payload, report.elapsed);
    print_phase(
        "submit_tracking",
        &report.profile.submit_tracking,
        report.elapsed,
    );
    print_phase("leader_step", &report.profile.leader_step, report.elapsed);
    print_phase("pump_total", &report.profile.pump_total, report.elapsed);
    print_phase(
        "follower_step",
        &report.profile.follower_step,
        report.elapsed,
    );
    print_phase(
        "queue_extend",
        &report.profile.queue_extend,
        report.elapsed,
    );
}

fn print_phase(name: &str, phase: &PhaseProfile, total: Duration) {
    let calls = phase.calls.max(1);
    println!(
        "{:<18} {:>8} {:>10.3} {:>6.1}% {:>10} {:>10} {:>10} {:>14.3}",
        name,
        phase.calls,
        phase.elapsed.as_secs_f64() * 1_000.0,
        100.0 * phase.elapsed.as_secs_f64() / total.as_secs_f64(),
        phase.allocs,
        phase.reallocs,
        phase.allocated_bytes,
        phase.allocs as f64 / calls as f64,
    );
}
