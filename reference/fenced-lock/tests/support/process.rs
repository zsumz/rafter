//! Orchestration for a cluster of real `lock-node` processes.
//!
//! This is consumer-owned exemplar code in the same sense the deterministic
//! driver is: it spawns processes, kills them, restarts them, and talks the
//! documented client protocol. It uses no privileged observation — no internal
//! hooks, no shared memory, no reaching into a replica's state — because an
//! external user orchestrating Rafter has none of those either. The response
//! parsing below is written against the protocol document rather than shared
//! with the binary that produces it, so a change that broke one half without
//! the other is exactly what this suite catches.
//!
//! # Where real time enters, and how far it can go wrong
//!
//! The deterministic driver decides when a node ticks. This one cannot: a real
//! process ticks on its own clock, a real socket delivers when it delivers, and
//! a real `SIGKILL` lands where it lands. Real time is load bearing in exactly
//! four places, and each is bounded rather than assumed:
//!
//! 1. **Waiting for a condition.** Every wait is a predicate polled on a short
//!    interval against a deadline ([`wait_until`]). There is no "sleep and
//!    assume" anywhere in this file: a wait either observes the condition and
//!    returns, or fails the test naming the condition it was waiting for.
//!    Slowness makes a test slower, never greener. The one `sleep` in this
//!    module is the poll interval inside such a loop, which is a rate rather
//!    than an assumption.
//! 2. **Election timing.** Election timeouts are distinct per node and Rafter's
//!    election jitter defaults to zero, so *which* replica wins a given
//!    election is determined rather than raced; only *when* it takes office is
//!    timing. Determined is not the same as obvious: a first election is won by
//!    the shortest timeout, while a failover is won by the *longest* one among
//!    the survivors, because pre-vote leader stickiness makes every other
//!    survivor refuse the shortest replica's poll until its own timeout has
//!    elapsed. Tests assert the first case exactly and assert only "a survivor
//!    leads" for the second.
//! 3. **Where a `SIGKILL` lands.** A kill during an in-flight write genuinely
//!    may land before or after the commit point, and no harness can pin that
//!    down. Tests that kill mid-write assert the property that holds for both
//!    readings rather than asserting one and hoping.
//! 4. **Whether a kill leaves a restartable store.** This one is specific to
//!    the lock, and it is not a timing accident that can be engineered away.
//!    A publication writes a whole image and then seals it; a kill between
//!    those two writes leaves a complete image whose seal never landed, which
//!    the store refuses to adopt because it cannot be told apart from a live
//!    slot whose seal byte rotted. A plain restart of such a replica announces
//!    `NEEDS_DECISION` and refuses to serve. [`ProcessCluster::restart`] treats
//!    that as the legitimate outcome it is: it reads the mode the process
//!    names, restarts under it, and records the escalation in
//!    [`ProcessCluster::escalations`] so a test can assert what it cost rather
//!    than never learning it happened.
//!
//! Logical time is *not* in that list, and that is the lock's own advantage
//! over a lease system that expires on a clock. A lease here expires because
//! somebody submitted `ExpireThrough`, so every expiry in this suite is an
//! explicit replicated command with a deterministic effect. No test waits for a
//! lease to lapse.
//!
//! Ports are never assigned by this harness. Each replica binds an ephemeral
//! port and publishes it, which removes the one flake source a port-picking
//! harness would have: two processes racing for the same number.
//!
//! # Timeouts
//!
//! The deadlines below are deliberately generous. They exist to turn a hang
//! into a readable failure on a loaded machine, not to measure anything, so
//! making them tighter would buy a faster failure at the cost of a flakier
//! pass.

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Command as OsCommand,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use rafter::{LogIndex, NodeId};
use rafter_reference_fenced_lock::{
    ApplyDisposition, ClientId, Command, FencingToken, HistoryEvent, LockConfig, LockRejection,
    LockResponse, LogicalTime, OperationId, OperationResult, RequestRejection, ResourceName,
    Sequence, SessionEpoch,
};
use rafter_reference_harness::process::{
    ChildProcess, ConnectionTimeouts, LineConnection, ReconnectingClient, Wait,
};

use crate::scratch::ScratchDir;

#[path = "process_history.rs"]
pub(crate) mod process_history;

/// Node identifiers of the three-replica process cluster.
pub const NODE_IDS: [NodeId; 3] = [NodeId(1), NodeId(2), NodeId(3)];

/// Election timeouts, in ticks, one per node.
///
/// Distinct values with Rafter's default zero jitter keep *who* wins a given
/// election determined rather than raced.
///
/// The absolute size is an operational choice, and it is deliberately generous:
/// a leader that does not hear from a quorum within its election timeout steps
/// down, so the timeout has to exceed the real round trip by a wide margin. At
/// 20 ms per tick these are 400, 600, and 800 ms, which survives a machine
/// running the whole suite in parallel. No test depends on leadership *not*
/// changing, so this only buys the suite fewer detours.
const ELECTION_TIMEOUT_TICKS: [(NodeId, u64); 3] =
    [(NodeId(1), 20), (NodeId(2), 30), (NodeId(3), 40)];

/// Milliseconds between a replica's ticks.
const TICK_INTERVAL_MS: u64 = 20;

/// How long a replica may wait for another process to release its directory.
const OWNERSHIP_WAIT_MS: u64 = 30_000;

/// How long a replica waits for one client request before answering `UNKNOWN`.
const REQUEST_TIMEOUT_MS: u64 = 5_000;

/// How long this harness waits for a lifecycle line or a cluster condition.
pub const WAIT_TIMEOUT: Duration = Duration::from_secs(30);

/// How long this harness waits for one client response.
///
/// Comfortably above the replica's own request timeout, so a slow request is
/// answered by the replica rather than abandoned by the socket.
const CLIENT_READ_TIMEOUT: Duration = Duration::from_secs(20);

/// How often a bounded wait re-evaluates its predicate.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

const PROCESS_WAIT: Wait = Wait::new(WAIT_TIMEOUT, POLL_INTERVAL);

const CONNECTION_TIMEOUTS: ConnectionTimeouts = ConnectionTimeouts::new(
    Duration::from_secs(5),
    CLIENT_READ_TIMEOUT,
    Duration::from_secs(5),
);

/// Polls `predicate` until it yields a value or [`WAIT_TIMEOUT`] elapses.
///
/// This is the only shape of waiting this harness performs. It never sleeps for
/// a fixed duration and then proceeds, because that turns a slow machine into a
/// wrong answer instead of a slow one.
///
/// # Panics
///
/// Panics with `description` when the deadline passes first.
#[track_caller]
pub fn wait_until<T>(description: &str, predicate: impl FnMut() -> Option<T>) -> T {
    PROCESS_WAIT
        .until(description, predicate)
        .unwrap_or_else(|error| panic!("{error}"))
}

/// Terminal outcome of one submitted command, as this client observed it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubmitOutcome {
    /// The replica returned a replicated response.
    Applied {
        disposition: ApplyDisposition,
        response: LockResponse,
    },
    /// The replica refused the command before it entered any log.
    NotCommitted {
        kind: String,
        leader_hint: Option<NodeId>,
    },
    /// The replica was not serving; the command was never proposed.
    NotReady { applied: u64, committed: u64 },
    /// The client cannot tell whether the command committed.
    Unknown { detail: String },
}

impl SubmitOutcome {
    /// Returns the replicated response when the command committed.
    pub fn response(&self) -> Option<&LockResponse> {
        match self {
            Self::Applied { response, .. } => Some(response),
            _ => None,
        }
    }

    /// Returns the operation result of a committed operation.
    ///
    /// A command that did not commit, or that committed as a session response
    /// or a request rejection, is a different kind of event and panics rather
    /// than collapsing into `None`: a caller reaching for an operation result
    /// has already decided the operation ran.
    #[track_caller]
    pub fn operation(&self) -> OperationResult {
        match self.response() {
            Some(LockResponse::Operation(result)) => *result,
            other => panic!("expected a committed operation, observed {other:?}"),
        }
    }

    /// Returns the fencing token of a committed acquisition.
    #[track_caller]
    pub fn acquired_token(&self) -> FencingToken {
        match self.operation() {
            OperationResult::Acquired { token, .. } => token,
            other => panic!("expected an acquisition, observed {other:?}"),
        }
    }
}

/// Terminal outcome of one query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryOutcome {
    /// The query returned a lock's status.
    Ready(LockView),
    /// The query returned no value, so it constrains no ordering.
    Abandoned { detail: String },
    /// The replica was not serving.
    NotReady { applied: u64, committed: u64 },
}

impl QueryOutcome {
    /// Returns the answered status.
    #[track_caller]
    pub fn view(&self) -> &LockView {
        match self {
            Self::Ready(view) => view,
            other => panic!("expected an answered query, observed {other:?}"),
        }
    }
}

/// One resource's status as a replica reported it over the wire.
///
/// This is the harness's own decoding of the `OK LOCK` line rather than the
/// crate's `ResourceStatus`. Keeping it separate is the point: the wire
/// contract has two halves, and the test half must be able to disagree with the
/// binary half.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LockView {
    pub resource: String,
    pub owner: Option<u32>,
    pub held_token: Option<u64>,
    pub expiry: Option<u64>,
    pub token_floor: Option<u64>,
    pub logical_time: u64,
}

impl LockView {
    /// Returns whether anybody holds this lock.
    pub const fn is_held(&self) -> bool {
        self.owner.is_some()
    }
}

/// One replica's reported status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeStatus {
    pub ready: bool,
    pub role: String,
    pub term: u64,
    pub applied: u64,
    pub committed: u64,
    pub leader: Option<NodeId>,
}

/// How a restarted replica got its durable store open.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Escalation {
    /// The replica whose store refused a plainer mode.
    pub node_id: NodeId,
    /// The mode the process itself named as the next step.
    pub mode: String,
    /// The refusal line the process printed.
    pub refusal: String,
}

/// What a freshly spawned replica settled into.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Started {
    /// Recovery completed and the replica serves from this applied floor.
    Ready { applied: LogIndex },
    /// The durable store will not open under the mode this process was given.
    NeedsDecision { line: String, next_mode: String },
}

/// How a replica that refused to open ended.
///
/// Both halves are load-bearing and neither stands alone. The status is the
/// contract with a supervisor — a refusal that exits zero is read as a clean
/// stop and restarted into whatever it refused over — and the stdout is what
/// tells an operator which artifact disagreed.
#[derive(Debug)]
pub struct Refusal {
    pub status: std::process::ExitStatus,
    pub stdout: String,
}

/// One `lock-node` process.
#[derive(Debug)]
pub struct NodeProcess {
    node_id: NodeId,
    child: ChildProcess,
    client_addr: SocketAddr,
    client: ReconnectingClient,
}

impl NodeProcess {
    /// Spawns one replica under the plain recovery mode and waits for it to
    /// open its client port.
    ///
    /// Spawning deliberately does not wait for readiness. A caller that wants a
    /// serving replica asks for one with [`NodeProcess::wait_ready`]; a caller
    /// testing the readiness gate needs the window in between.
    pub fn spawn(cluster_dir: &Path, node_id: NodeId, config: LockConfig) -> Self {
        Self::spawn_with_mode(cluster_dir, node_id, config, "open")
    }

    /// Spawns one replica under a named recovery mode.
    ///
    /// Anything above `open` discards durable state, so it is never reached by
    /// restarting: a caller has to name it, and [`ProcessCluster::restart`]
    /// only names it after a process refused and said which one.
    pub fn spawn_with_mode(
        cluster_dir: &Path,
        node_id: NodeId,
        config: LockConfig,
        mode: &str,
    ) -> Self {
        Self::spawn_with(cluster_dir, node_id, config, mode, None)
    }

    /// Spawns one replica whose peer control plane stops being writable after
    /// `fault_after` client operations.
    ///
    /// The one state this suite cannot reach by damaging an artifact from
    /// outside. A replica that cannot record what it retired must stop serving
    /// and exit nonzero, and in a cluster whose membership never changes the
    /// driver's checkpoint epoch stands still after the first write — so no
    /// later write is attempted, and revoking a permission or filling a disk
    /// would inject a fault into a call that never happens. The binary carries
    /// the seam instead, addressed by client operation; see its
    /// `--control-plane-fault-after` argument.
    pub fn spawn_with_control_plane_fault(
        cluster_dir: &Path,
        node_id: NodeId,
        config: LockConfig,
        fault_after: u64,
    ) -> Self {
        Self::spawn_with(cluster_dir, node_id, config, "open", Some(fault_after))
    }

    fn spawn_with(
        cluster_dir: &Path,
        node_id: NodeId,
        config: LockConfig,
        mode: &str,
        control_plane_fault_after: Option<u64>,
    ) -> Self {
        let election_timeout_ticks = ELECTION_TIMEOUT_TICKS
            .iter()
            .find(|(id, _)| *id == node_id)
            .map_or(8, |(_, ticks)| *ticks);
        let members = NODE_IDS
            .iter()
            .map(|id| id.0.to_string())
            .collect::<Vec<_>>()
            .join(",");

        let mut command = OsCommand::new(env!("CARGO_BIN_EXE_lock-node"));
        command
            .arg("--id")
            .arg(node_id.0.to_string())
            .arg("--members")
            .arg(members)
            .arg("--cluster-dir")
            .arg(cluster_dir)
            .arg("--election-timeout-ticks")
            .arg(election_timeout_ticks.to_string())
            .arg("--tick-interval-ms")
            .arg(TICK_INTERVAL_MS.to_string())
            .arg("--ownership-wait-ms")
            .arg(OWNERSHIP_WAIT_MS.to_string())
            .arg("--request-timeout-ms")
            .arg(REQUEST_TIMEOUT_MS.to_string())
            .arg("--max-clients")
            .arg(config.max_clients().to_string())
            .arg("--max-resources")
            .arg(config.max_resources().to_string())
            .arg("--recover")
            .arg(mode);
        if let Some(fault_after) = control_plane_fault_after {
            command
                .arg("--control-plane-fault-after")
                .arg(fault_after.to_string());
        }

        let child = ChildProcess::spawn(format!("replica {}", node_id.0), &mut command)
            .unwrap_or_else(|error| panic!("could not spawn replica {}: {error}", node_id.0));
        let placeholder = "127.0.0.1:0".parse().expect("a placeholder address parses");

        let mut process = Self {
            node_id,
            child,
            client_addr: placeholder,
            client: ReconnectingClient::new(placeholder, CONNECTION_TIMEOUTS),
        };
        let listening = process.wait_for_line("LISTENING");
        process.client_addr = listening
            .split_whitespace()
            .nth(2)
            .expect("a LISTENING line names an address")
            .parse()
            .expect("the announced client address parses");
        process.client.set_addr(process.client_addr);
        process
    }

    /// Returns this replica's client address.
    pub const fn client_addr(&self) -> SocketAddr {
        self.client_addr
    }

    /// Returns one replica's durable lock-store directory under a cluster root.
    ///
    /// A test that wants to reach past the process and at the artifact — to
    /// damage a slot, or to see what a repair left — needs the same path the
    /// replica uses.
    pub fn app_dir(cluster_dir: &Path, node_id: NodeId) -> PathBuf {
        cluster_dir.join(format!("node-{}", node_id.0)).join("app")
    }

    /// Returns one replica's own directory, which is where the process publishes
    /// the durable state that is neither Raft's nor the application's — its
    /// peer address, and its peer-control-plane checkpoint.
    pub fn node_dir(cluster_dir: &Path, node_id: NodeId) -> PathBuf {
        cluster_dir.join(format!("node-{}", node_id.0))
    }

    /// Waits for this replica to either finish recovery or refuse it.
    ///
    /// Both are terminal, and the refusal is not a failure of this harness: a
    /// killed replica whose publication was interrupted between its image and
    /// its seal legitimately refuses a plain restart. Returning it rather than
    /// panicking is what lets a caller escalate deliberately.
    pub fn wait_started(&mut self) -> Started {
        let node_id = self.node_id;
        self.child
            .wait_for_stdout("replica to serve or refuse", PROCESS_WAIT, |lines| {
                if let Some(line) = lines.iter().find(|line| line.starts_with("NEEDS_DECISION")) {
                    return Some(Started::NeedsDecision {
                        line: line.clone(),
                        next_mode: next_recovery_mode(line),
                    });
                }
                lines
                    .iter()
                    .find(|line| line.starts_with("READY"))
                    .map(|line| Started::Ready {
                        applied: LogIndex(
                            line.split_whitespace()
                                .nth(3)
                                .expect("a READY line names an applied index")
                                .parse()
                                .expect("the announced applied index parses"),
                        ),
                    })
            })
            .unwrap_or_else(|error| {
                panic!("replica {} neither served nor refused: {error}", node_id.0)
            })
    }

    /// Waits for this replica to announce complete recovery.
    ///
    /// Returns the applied index it recovered to, which is the durable floor a
    /// restart test asserts against.
    ///
    /// # Panics
    ///
    /// Panics when the replica refuses to open its store, because a caller that
    /// asked for readiness has already decided the store must open.
    #[track_caller]
    pub fn wait_ready(&mut self) -> LogIndex {
        match self.wait_started() {
            Started::Ready { applied } => applied,
            Started::NeedsDecision { line, .. } => {
                panic!("replica {} refused to serve: {line}", self.node_id.0)
            }
        }
    }

    /// Returns whether this replica has already announced a line with `prefix`.
    ///
    /// Non-blocking, so a test can assert that a replica is *not* ready without
    /// waiting for it to become so.
    pub fn has_announced(&mut self, prefix: &str) -> bool {
        self.child.has_stdout_prefix(prefix)
    }

    /// Waits for a lifecycle line beginning with `prefix`.
    pub fn wait_for_line(&mut self, prefix: &str) -> String {
        self.child
            .wait_for_stdout_prefix(prefix, PROCESS_WAIT)
            .unwrap_or_else(|error| panic!("{error}"))
    }

    /// Sends one protocol line and returns the raw response.
    ///
    /// A connection failure is reported rather than panicked: a client whose
    /// replica was killed mid-request observes exactly this, and turning it
    /// into a panic would delete the unknown outcome the suite is here to test.
    pub fn ask(&mut self, line: &str) -> Result<String, String> {
        self.client.request(line).map_err(|error| error.to_string())
    }

    /// Waits for this replica to exit on its own, and reports how it went.
    ///
    /// For the refusals that are not [`Started::NeedsDecision`]: a process that
    /// will not open at all announces `FATAL` and dies rather than reaching
    /// readiness, so `wait_started` would only ever time out on it. Every line
    /// the process wrote is collected, because the reader thread runs to EOF and
    /// the channel closes when the child's stdout does.
    pub fn wait_refused(&mut self) -> Refusal {
        let status = self
            .child
            .wait_for_exit(PROCESS_WAIT)
            .unwrap_or_else(|error| panic!("{error}"));
        Refusal {
            status,
            stdout: self.child.stdout_lines().join("\n"),
        }
    }

    /// Kills this replica with `SIGKILL` and reaps it.
    ///
    /// `SIGKILL` is the harshest exit available and runs no cleanup path, which
    /// is the point: every recovery this suite asserts is recovery from a
    /// process that was given no chance to tidy up.
    pub fn kill(&mut self) {
        self.client.disconnect();
        drop(self.child.kill_and_reap());
    }

    /// Asks this replica to stop cleanly and waits for it to exit.
    pub fn shutdown(&mut self) {
        drop(self.ask("SHUTDOWN"));
        self.client.disconnect();
        self.child
            .wait_for_exit(PROCESS_WAIT)
            .unwrap_or_else(|error| panic!("{error}"));
    }
}

/// Reads the mode a refusal named as its next step.
fn next_recovery_mode(line: &str) -> String {
    let tokens: Vec<&str> = line.split_whitespace().collect();
    tokens
        .iter()
        .position(|token| *token == "--recover")
        .and_then(|index| tokens.get(index + 1))
        .map_or_else(
            || panic!("a refusal must name the mode that follows it: {line}"),
            |mode| (*mode).to_string(),
        )
}

/// A continuous stream of immediately-answered client requests against one
/// replica.
///
/// **The load a fair loop has to survive.** `STATUS` is the cheapest request
/// this protocol has — it answers from memory, touches no store, and reaches
/// neither Raft nor the lock — so a replica that cannot keep serving its
/// protocol under a flood of them is not being slowed down by work. It is being
/// starved by a loop that answers clients until they stop asking.
///
/// Each connection is strictly sequential, which is what makes the flood
/// continuous rather than bursty: the next request is written as soon as the
/// previous answer arrives, so on loopback the replica's job queue is refilled
/// far faster than its idle poll interval.
#[derive(Debug)]
pub struct StatusFlood {
    stop: Arc<AtomicBool>,
    answered: Arc<AtomicU64>,
    abandoned: Arc<AtomicU64>,
    threads: Vec<thread::JoinHandle<()>>,
}

impl StatusFlood {
    /// Opens `connections` sockets to `addr` and keeps `STATUS` in flight on all
    /// of them until this is dropped or stopped.
    pub fn start(addr: SocketAddr, connections: usize) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let answered = Arc::new(AtomicU64::new(0));
        let abandoned = Arc::new(AtomicU64::new(0));
        let mut threads = Vec::new();
        for _ in 0..connections {
            let stop = Arc::clone(&stop);
            let answered = Arc::clone(&answered);
            let abandoned = Arc::clone(&abandoned);
            threads.push(thread::spawn(move || {
                // A connection that cannot be opened, or that the replica drops
                // as it stops, ends this thread rather than failing the test:
                // the flood is load, and a load generator that panics when its
                // target legitimately exits would turn every success into a
                // failure.
                let Ok(mut conn) = LineConnection::connect(addr, CONNECTION_TIMEOUTS) else {
                    return;
                };
                while !stop.load(Ordering::Relaxed) {
                    let Ok(response) = conn.request("STATUS") else {
                        return;
                    };
                    answered.fetch_add(1, Ordering::Relaxed);
                    // The terminal readiness word, counted rather than only
                    // passed over. A flood is the only client that reliably has
                    // a request in flight during the short window between a
                    // replica entering its terminal state and the loop ending,
                    // so it is the only observer that can say what `STATUS`
                    // answered there.
                    if response.starts_with("STATUS abandoned") {
                        abandoned.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }));
        }
        Self {
            stop,
            answered,
            abandoned,
            threads,
        }
    }

    /// How many requests this flood has had answered so far.
    pub fn answered(&self) -> u64 {
        self.answered.load(Ordering::Relaxed)
    }

    /// How many of those answers reported the replica abandoned.
    pub fn abandoned(&self) -> u64 {
        self.abandoned.load(Ordering::Relaxed)
    }

    /// Stops every connection and waits for its thread.
    pub fn stop(mut self) -> u64 {
        self.halt();
        self.answered()
    }

    fn halt(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        for thread in self.threads.drain(..) {
            drop(thread.join());
        }
    }
}

impl Drop for StatusFlood {
    fn drop(&mut self) {
        self.halt();
    }
}

/// One client connection whose request is written without waiting for its
/// answer.
///
/// **The primitive a "queued behind" test needs and `NodeProcess::ask` cannot
/// give.** A connection is strictly sequential, so a client cannot have two
/// requests outstanding — which means the only way to put a second request in a
/// replica's job queue *while it is handling the first* is a second connection
/// whose line has already been written. Splitting the write from the read is
/// what makes that expressible.
#[derive(Debug)]
pub struct QueuedRequest {
    conn: LineConnection,
}

impl QueuedRequest {
    /// Opens a connection and leaves it idle, with nothing written yet.
    ///
    /// Connecting is the slow half and is deliberately done ahead of time, so
    /// [`QueuedRequest::send`] is one write on an established socket.
    pub fn connect(addr: SocketAddr) -> Self {
        Self {
            conn: LineConnection::connect(addr, CONNECTION_TIMEOUTS)
                .expect("the replica accepts a client connection"),
        }
    }

    /// Writes one request line and returns without reading the answer.
    pub fn send(&mut self, line: &str) {
        self.conn
            .send_line(line)
            .expect("the request is written and flushed");
    }

    /// Attempts to write one request when the process may already be exiting.
    pub fn try_send(&mut self, line: &str) -> Result<(), String> {
        self.conn.send_line(line).map_err(|error| error.to_string())
    }

    /// Reads the answer to whatever was sent.
    pub fn recv(&mut self) -> Result<String, String> {
        self.conn.receive_line().map_err(|error| error.to_string())
    }
}

/// A submission started but not yet resolved.
///
/// Its history invocation is already recorded, so the operation's real-time
/// interval is open from the moment this handle exists. That is what lets a
/// test kill a replica *during* a write and still record a history whose
/// intervals mean something.
#[derive(Debug)]
pub struct PendingSubmit {
    operation_id: OperationId,
    handle: thread::JoinHandle<Result<String, String>>,
}

/// A cluster of three real replica processes.
#[derive(Debug)]
pub struct ProcessCluster {
    root: ScratchDir,
    config: LockConfig,
    nodes: BTreeMap<NodeId, NodeProcess>,
    history: Vec<HistoryEvent>,
    escalations: Vec<Escalation>,
    next_operation_id: u64,
}

impl ProcessCluster {
    /// Spawns three replicas and waits for every one of them to serve.
    pub fn start(label: &str, config: LockConfig) -> Self {
        let root = ScratchDir::new(label);
        let mut cluster = Self {
            root,
            config,
            nodes: BTreeMap::new(),
            history: Vec::new(),
            escalations: Vec::new(),
            next_operation_id: 1,
        };
        for node_id in NODE_IDS {
            let process = NodeProcess::spawn(cluster.root.path(), node_id, config);
            cluster.nodes.insert(node_id, process);
        }
        for node_id in NODE_IDS {
            cluster.node_mut(node_id).wait_ready();
        }
        cluster
    }

    /// Returns the directory holding every replica's durable state.
    pub fn root(&self) -> &Path {
        self.root.path()
    }

    /// Returns the recorded client history.
    pub fn history(&self) -> &[HistoryEvent] {
        &self.history
    }

    /// Returns every restart that had to escalate past the plain recovery mode.
    ///
    /// Empty is the common case and not something a test may assume: whether a
    /// kill lands in the window that leaves an unsealed complete image is not
    /// under this harness's control.
    pub fn escalations(&self) -> &[Escalation] {
        &self.escalations
    }

    /// Returns the live replicas, lowest identifier first.
    pub fn live_nodes(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    /// Returns one live replica's process handle.
    #[track_caller]
    pub fn node_mut(&mut self, node_id: NodeId) -> &mut NodeProcess {
        self.nodes
            .get_mut(&node_id)
            .unwrap_or_else(|| panic!("replica {} is not running", node_id.0))
    }

    /// Returns one replica's reported status, or `None` when it cannot answer.
    pub fn status(&mut self, node_id: NodeId) -> Option<NodeStatus> {
        let response = self.node_mut(node_id).ask("STATUS").ok()?;
        parse_status(&response)
    }

    /// Waits until a live replica reports itself leader, and returns it.
    ///
    /// Which replica that is, is *not* simply the shortest election timeout
    /// among the survivors. Pre-vote leader stickiness makes a follower refuse
    /// a poll while it still believes a leader is current, and it believes that
    /// for its own election timeout — so after a leader dies, the replica with
    /// the *shortest* timeout polls first and is refused by the one with the
    /// longest, which then times out and wins. A first, uncontested election is
    /// different: nobody has heard from a leader, nobody is sticky, and the
    /// shortest timeout wins.
    pub fn wait_for_leader(&mut self) -> NodeId {
        let live = self.live_nodes();
        wait_until("a replica to report itself leader", || {
            for node_id in &live {
                if let Some(status) = self.status(*node_id) {
                    if status.ready && status.role == "leader" {
                        return Some(*node_id);
                    }
                }
            }
            None
        })
    }

    /// Waits until every live replica names the same leader.
    ///
    /// A replica learns who leads from that leader's traffic, so agreement
    /// trails the election by a round trip. Waiting for it is what a client
    /// routing on a leader hint does; asserting it the instant an election
    /// completes would be asserting a message had already arrived.
    pub fn wait_for_agreed_leader(&mut self) -> NodeId {
        let live = self.live_nodes();
        wait_until("every live replica to name the same leader", || {
            let mut agreed = None;
            for node_id in &live {
                let status = self.status(*node_id)?;
                if !status.ready {
                    return None;
                }
                match (agreed, status.leader) {
                    (None, Some(leader)) => agreed = Some(leader),
                    (Some(known), Some(leader)) if known == leader => {}
                    _ => return None,
                }
            }
            agreed
        })
    }

    /// Submits one command to whichever replica currently leads, retrying a
    /// refusal against the leader it names.
    ///
    /// This is what a real client does, and it is safe precisely because the
    /// lock's request identity makes a retry idempotent. Only provable
    /// refusals are retried: an unknown outcome is returned to the caller
    /// untouched, because deciding what to do about one is the caller's
    /// contract obligation and hiding it here would delete the case the suite
    /// exists to test.
    pub fn submit_to_leader(&mut self, command: Command) -> SubmitOutcome {
        let mut last_outcome = None;
        match PROCESS_WAIT.until("a replica to accept the command", || {
            let leader = self.wait_for_leader();
            let outcome = self.submit(leader, command);
            if matches!(
                outcome,
                SubmitOutcome::NotCommitted { .. } | SubmitOutcome::NotReady { .. }
            ) {
                last_outcome = Some(outcome);
                None
            } else {
                Some(outcome)
            }
        }) {
            Ok(outcome) => outcome,
            Err(error) => panic!("{error}: last outcome {last_outcome:?}"),
        }
    }

    /// Runs one linearizable `GetLock` against the current leader, retrying
    /// while it answers nothing.
    ///
    /// A barrier refused or canceled by a leadership change delivered no value,
    /// so retrying it is a client decision rather than a weakening of the
    /// check: an attempt that answered nothing constrains no ordering.
    pub fn get_lock(&mut self, resource: ResourceName) -> LockView {
        let mut last_outcome = None;
        match PROCESS_WAIT.until("a replica to answer the query", || {
            let leader = self.wait_for_leader();
            let outcome = self.query(leader, resource);
            if let QueryOutcome::Ready(view) = outcome {
                Some(view)
            } else {
                last_outcome = Some(outcome);
                None
            }
        }) {
            Ok(view) => view,
            Err(error) => panic!("{error}: last outcome {last_outcome:?}"),
        }
    }

    /// Kills one replica with `SIGKILL` and forgets it.
    #[track_caller]
    pub fn kill(&mut self, node_id: NodeId) {
        let mut process = self
            .nodes
            .remove(&node_id)
            .unwrap_or_else(|| panic!("replica {} is not running", node_id.0));
        process.kill();
    }

    /// Stops one replica cleanly and forgets it.
    ///
    /// The suite kills with `SIGKILL` everywhere the point is crash recovery.
    /// This exists for the one place where it is not: the readiness gate needs
    /// an incumbent to release its directory, and killing it there would mix a
    /// crash-recovery outcome into a test about a gate. A clean stop seals the
    /// store and releases the operating-system lock before this returns.
    #[track_caller]
    pub fn stop(&mut self, node_id: NodeId) {
        let mut process = self
            .nodes
            .remove(&node_id)
            .unwrap_or_else(|| panic!("replica {} is not running", node_id.0));
        process.shutdown();
    }

    /// Spawns a replacement process for a replica whose directory is still
    /// owned by a live one.
    ///
    /// This is how the readiness gate is tested without a test hook. The
    /// returned process is not part of the cluster: it has opened its client
    /// port and is refusing service while it waits for the incumbent to exit.
    pub fn spawn_contender(&mut self, node_id: NodeId) -> NodeProcess {
        NodeProcess::spawn(self.root.path(), node_id, self.config)
    }

    /// Adds an already-spawned process back into the cluster.
    pub fn adopt(&mut self, node_id: NodeId, process: NodeProcess) {
        self.nodes.insert(node_id, process);
    }

    /// Starts a replica that is not currently running and waits for it to serve.
    ///
    /// A plain restart is tried first, always. When the store refuses it, the
    /// process names the mode that follows and this restarts under exactly that
    /// mode, once, recording the escalation. Escalating without being told to
    /// would discard durable state a plain restart would have kept.
    #[track_caller]
    pub fn restart(&mut self, node_id: NodeId) -> LogIndex {
        assert!(
            !self.nodes.contains_key(&node_id),
            "replica {} is still running; kill it before restarting it",
            node_id.0
        );
        let mut process = NodeProcess::spawn(self.root.path(), node_id, self.config);
        let applied = match process.wait_started() {
            Started::Ready { applied } => applied,
            Started::NeedsDecision { line, next_mode } => {
                drop(process);
                self.escalations.push(Escalation {
                    node_id,
                    mode: next_mode.clone(),
                    refusal: line,
                });
                process = NodeProcess::spawn_with_mode(
                    self.root.path(),
                    node_id,
                    self.config,
                    &next_mode,
                );
                process.wait_ready()
            }
        };
        self.nodes.insert(node_id, process);
        applied
    }

    /// Starts a replica whose control plane stops being writable after
    /// `fault_after` client operations, and waits for it to serve.
    ///
    /// It opens normally — the fault is armed ahead of it, not at it — so the
    /// replica really does reach readiness and really is serving when the first
    /// client operation makes it undurable. That ordering is the point: the
    /// state under test is a *running* replica that has just learned its next
    /// restart would forget what it retired, not one that refused to start.
    ///
    /// Removed from the cluster on return, like
    /// [`ProcessCluster::restart_expecting_failure`], because this process is
    /// going to exit on its own and a later `shutdown` must not try to talk to
    /// it. The handle is the caller's to drive.
    #[track_caller]
    pub fn restart_with_control_plane_fault(
        &mut self,
        node_id: NodeId,
        fault_after: u64,
    ) -> NodeProcess {
        assert!(
            !self.nodes.contains_key(&node_id),
            "replica {} is still running; kill it before restarting it",
            node_id.0
        );
        let mut process = NodeProcess::spawn_with_control_plane_fault(
            self.root.path(),
            node_id,
            self.config,
            fault_after,
        );
        process.wait_ready();
        process
    }

    /// Restarts a replica that is expected to refuse, and reports how it ended.
    ///
    /// Deliberately not an escalation path. [`ProcessCluster::restart`] escalates
    /// when a process names the mode that follows, because that refusal has a
    /// remedy; this is for refusals that do not — a durable artifact is gone, and
    /// the honest outcome is a process that will not start until somebody
    /// decides what to do about it. The replica is not added back to the
    /// cluster, so a later `shutdown` does not try to talk to it.
    #[track_caller]
    pub fn restart_expecting_failure(&mut self, node_id: NodeId) -> Refusal {
        assert!(
            !self.nodes.contains_key(&node_id),
            "replica {} is still running; kill it before restarting it",
            node_id.0
        );
        NodeProcess::spawn(self.root.path(), node_id, self.config).wait_refused()
    }

    /// Submits one command and waits for its terminal outcome.
    pub fn submit(&mut self, node_id: NodeId, command: Command) -> SubmitOutcome {
        let pending = self.begin_submit(node_id, command);
        self.resolve_submit(pending)
    }

    /// Sends a hand-authored protocol line and retains its exact command.
    pub fn submit_raw(
        &mut self,
        node_id: NodeId,
        command: Command,
        line: &str,
    ) -> Result<String, String> {
        let operation_id = self.allocate_operation_id();
        self.history.push(HistoryEvent::Invoked {
            operation_id,
            command,
        });
        let raw = self.node_mut(node_id).ask(line);
        let outcome = match &raw {
            Ok(response) => parse_submit_response(response),
            Err(detail) => SubmitOutcome::Unknown {
                detail: detail.clone(),
            },
        };
        self.record_submit(operation_id, &outcome);
        raw
    }

    /// Starts one command on its own connection without waiting for it.
    ///
    /// The submission runs on its own thread and its own socket, so the caller
    /// stays free to kill a replica while the command is in flight.
    pub fn begin_submit(&mut self, node_id: NodeId, command: Command) -> PendingSubmit {
        let operation_id = self.allocate_operation_id();
        self.history.push(HistoryEvent::Invoked {
            operation_id,
            command,
        });
        let line = render_command(command);
        let addr = self.node_mut(node_id).client_addr();
        let handle = thread::spawn(move || {
            let mut client = LineConnection::connect(addr, CONNECTION_TIMEOUTS)
                .map_err(|error| error.to_string())?;
            client.request(&line).map_err(|error| error.to_string())
        });
        PendingSubmit {
            operation_id,
            handle,
        }
    }

    /// Waits for a started command and records its terminal history event.
    pub fn resolve_submit(&mut self, pending: PendingSubmit) -> SubmitOutcome {
        let raw = pending
            .handle
            .join()
            .unwrap_or_else(|_| Err(String::from("the submitting thread panicked")));
        let outcome = match raw {
            Ok(response) => parse_submit_response(&response),
            // A connection that died took the answer with it. The command may
            // still commit, so nothing weaker than `Unknown` is honest.
            Err(detail) => SubmitOutcome::Unknown { detail },
        };
        self.record_submit(pending.operation_id, &outcome);
        outcome
    }

    /// Runs and records one linearizable `GetLock`.
    pub fn query(&mut self, node_id: NodeId, resource: ResourceName) -> QueryOutcome {
        let operation_id = self.allocate_operation_id();
        self.history
            .push(process_history::query_invocation(operation_id, resource));
        let line = format!("QUERY LOCK {}", resource.as_str());
        let outcome = match self.node_mut(node_id).ask(&line) {
            Ok(response) => parse_query_response(&response),
            Err(detail) => QueryOutcome::Abandoned { detail },
        };
        let terminal = process_history::query_terminal(operation_id, &outcome);
        self.history.push(terminal);
        outcome
    }

    /// Reads one replica's own applied state, which may be stale.
    pub fn local_read(&mut self, node_id: NodeId, resource: ResourceName) -> QueryOutcome {
        let line = format!("LOCAL LOCK {}", resource.as_str());
        match self.node_mut(node_id).ask(&line) {
            Ok(response) => parse_query_response(&response),
            Err(detail) => QueryOutcome::Abandoned { detail },
        }
    }

    /// Waits until `node_id` has applied at least `index`.
    pub fn wait_applied_through(&mut self, node_id: NodeId, index: LogIndex) {
        let description = format!("replica {} to apply through index {}", node_id.0, index.0);
        wait_until(&description, || {
            self.status(node_id)
                .filter(|status| status.applied >= index.0)
        });
    }

    /// Stops every replica cleanly.
    pub fn shutdown(&mut self) {
        for node_id in self.live_nodes() {
            self.node_mut(node_id).shutdown();
        }
        self.nodes.clear();
    }

    fn record_submit(&mut self, operation_id: OperationId, outcome: &SubmitOutcome) {
        self.history
            .push(process_history::submit_terminal(operation_id, outcome));
    }

    fn allocate_operation_id(&mut self) -> OperationId {
        let operation_id = OperationId::new(self.next_operation_id);
        self.next_operation_id += 1;
        operation_id
    }
}

impl Drop for ProcessCluster {
    /// Replicas are killed before the scratch directory is removed, so no live
    /// process is left writing into a directory that is being deleted.
    fn drop(&mut self) {
        for (_, mut process) in std::mem::take(&mut self.nodes) {
            process.kill();
        }
    }
}

/// Renders one command as a protocol request line.
///
/// The fingerprint inside a [`Command::Submit`] is deliberately not rendered:
/// the protocol derives it from the operation on the same line, and
/// `CONTRACT.md` records what that costs.
pub(crate) fn render_command(command: Command) -> String {
    match command {
        Command::OpenSession {
            client_id,
            session_epoch,
        } => format!("OPEN_SESSION {} {}", client_id.get(), session_epoch.get()),
        Command::Submit { request, operation } => {
            let operation = match operation {
                rafter_reference_fenced_lock::Operation::Acquire { resource, lease } => {
                    format!("ACQUIRE {} {}", resource.as_str(), lease.get())
                }
                rafter_reference_fenced_lock::Operation::Renew {
                    resource,
                    token,
                    lease,
                } => format!(
                    "RENEW {} {} {}",
                    resource.as_str(),
                    token.get(),
                    lease.get()
                ),
                rafter_reference_fenced_lock::Operation::Release { resource, token } => {
                    format!("RELEASE {} {}", resource.as_str(), token.get())
                }
                rafter_reference_fenced_lock::Operation::ExpireThrough { horizon } => {
                    format!("EXPIRE_THROUGH {}", horizon.get())
                }
            };
            format!(
                "SUBMIT {} {} {} {operation}",
                request.client_id.get(),
                request.session_epoch.get(),
                request.sequence.get()
            )
        }
    }
}

pub(crate) fn parse_status(response: &str) -> Option<NodeStatus> {
    let tokens: Vec<&str> = response.split_whitespace().collect();
    if tokens.len() != 7 || tokens[0] != "STATUS" {
        return None;
    }
    Some(NodeStatus {
        ready: tokens[1] == "ready",
        role: tokens[2].to_string(),
        term: tokens[3].parse().ok()?,
        applied: tokens[4].parse().ok()?,
        committed: tokens[5].parse().ok()?,
        leader: match tokens[6] {
            "-" => None,
            leader => Some(NodeId(leader.parse().ok()?)),
        },
    })
}

#[track_caller]
pub(crate) fn parse_submit_response(response: &str) -> SubmitOutcome {
    let tokens: Vec<&str> = response.split_whitespace().collect();
    match tokens.first().copied() {
        Some("OK") => {
            let disposition = tokens
                .get(1)
                .and_then(|token| parse_disposition(token))
                .unwrap_or_else(|| panic!("unparseable disposition in {response:?}"));
            let parsed = parse_lock_response(&tokens[2..])
                .unwrap_or_else(|| panic!("unparseable response in {response:?}"));
            SubmitOutcome::Applied {
                disposition,
                response: parsed,
            }
        }
        Some("NOTCOMMITTED") => SubmitOutcome::NotCommitted {
            kind: tokens.get(1).copied().unwrap_or_default().to_string(),
            leader_hint: tokens
                .get(2)
                .filter(|token| **token != "-")
                .and_then(|token| token.parse().ok())
                .map(NodeId),
        },
        Some("NOTREADY") => SubmitOutcome::NotReady {
            applied: tokens
                .get(1)
                .and_then(|token| token.parse().ok())
                .unwrap_or(0),
            committed: tokens
                .get(2)
                .and_then(|token| token.parse().ok())
                .unwrap_or(0),
        },
        Some("UNKNOWN") => SubmitOutcome::Unknown {
            detail: tokens[1..].join(" "),
        },
        _ => panic!("unexpected submit response {response:?}"),
    }
}

#[track_caller]
pub(crate) fn parse_query_response(response: &str) -> QueryOutcome {
    let tokens: Vec<&str> = response.split_whitespace().collect();
    match (tokens.first().copied(), tokens.get(1).copied()) {
        (Some("OK"), Some("LOCK")) => {
            assert!(
                tokens.len() == 8,
                "an OK LOCK line has eight tokens, observed {response:?}"
            );
            QueryOutcome::Ready(LockView {
                resource: tokens[2].to_string(),
                owner: optional(tokens[3]),
                held_token: optional(tokens[4]),
                expiry: optional(tokens[5]),
                token_floor: optional(tokens[6]),
                logical_time: tokens[7].parse().expect("logical time parses"),
            })
        }
        (Some("NOTREADY"), _) => QueryOutcome::NotReady {
            applied: tokens
                .get(1)
                .and_then(|token| token.parse().ok())
                .unwrap_or(0),
            committed: tokens
                .get(2)
                .and_then(|token| token.parse().ok())
                .unwrap_or(0),
        },
        (Some("ABANDONED"), _) => QueryOutcome::Abandoned {
            detail: tokens[1..].join(" "),
        },
        _ => panic!("unexpected query response {response:?}"),
    }
}

fn optional<T: std::str::FromStr>(token: &str) -> Option<T> {
    (token != "-").then(|| token.parse().ok().expect("an optional field parses"))
}

fn parse_disposition(token: &str) -> Option<ApplyDisposition> {
    match token {
        "SESSION_OPENED" => Some(ApplyDisposition::SessionOpened),
        "SESSION_REPLACED" => Some(ApplyDisposition::SessionReplaced),
        "SESSION_ALREADY_OPEN" => Some(ApplyDisposition::SessionAlreadyOpen),
        "APPLIED" => Some(ApplyDisposition::Applied),
        "REPLAYED" => Some(ApplyDisposition::Replayed),
        "NOT_ADMITTED" => Some(ApplyDisposition::Rejected),
        _ => None,
    }
}

fn parse_lock_response(tokens: &[&str]) -> Option<LockResponse> {
    match tokens.first().copied()? {
        "SESSION" => Some(LockResponse::SessionOpened {
            session_epoch: SessionEpoch::new(tokens.get(1)?.parse().ok()?)?,
        }),
        "OP" => Some(LockResponse::Operation(parse_operation_result(
            &tokens[1..],
        )?)),
        "REQUEST_REJECTED" => Some(LockResponse::Rejected(parse_request_rejection(
            &tokens[1..],
        )?)),
        _ => None,
    }
}

fn parse_operation_result(tokens: &[&str]) -> Option<OperationResult> {
    match tokens.first().copied()? {
        "ACQUIRED" => Some(OperationResult::Acquired {
            token: FencingToken::new(tokens.get(1)?.parse().ok()?)?,
            expiry: LogicalTime::new(tokens.get(2)?.parse().ok()?),
        }),
        "RENEWED" => Some(OperationResult::Renewed {
            token: FencingToken::new(tokens.get(1)?.parse().ok()?)?,
            expiry: LogicalTime::new(tokens.get(2)?.parse().ok()?),
        }),
        "RELEASED" => Some(OperationResult::Released),
        "EXPIRED" => Some(OperationResult::Expired {
            released_locks: tokens.get(1)?.parse().ok()?,
            logical_time: LogicalTime::new(tokens.get(2)?.parse().ok()?),
        }),
        "LOCK_REJECTED" => Some(OperationResult::Rejected(parse_lock_rejection(
            &tokens[1..],
        )?)),
        _ => None,
    }
}

fn parse_lock_rejection(tokens: &[&str]) -> Option<LockRejection> {
    match tokens.first().copied()? {
        "LOCK_HELD" => Some(LockRejection::LockHeld {
            owner: ClientId::new(tokens.get(1)?.parse().ok()?),
            token: FencingToken::new(tokens.get(2)?.parse().ok()?)?,
            expiry: LogicalTime::new(tokens.get(3)?.parse().ok()?),
        }),
        "LOCK_NOT_HELD" => Some(LockRejection::LockNotHeld),
        "NOT_LOCK_HOLDER" => Some(LockRejection::NotLockHolder {
            owner: ClientId::new(tokens.get(1)?.parse().ok()?),
        }),
        "FENCING_TOKEN_MISMATCH" => Some(LockRejection::FencingTokenMismatch {
            current: FencingToken::new(tokens.get(1)?.parse().ok()?)?,
        }),
        "LEASE_OVERFLOW" => Some(LockRejection::LeaseOverflow),
        "TOKEN_EXHAUSTED" => Some(LockRejection::TokenExhausted),
        "RESOURCE_CAPACITY_EXCEEDED" => Some(LockRejection::ResourceCapacityExceeded),
        "LOGICAL_TIME_NOT_ADVANCED" => Some(LockRejection::LogicalTimeNotAdvanced {
            current: LogicalTime::new(tokens.get(1)?.parse().ok()?),
        }),
        _ => None,
    }
}

fn parse_request_rejection(tokens: &[&str]) -> Option<RequestRejection> {
    match tokens.first().copied()? {
        "CLIENT_OUT_OF_RANGE" => Some(RequestRejection::ClientOutOfRange),
        "SESSION_NOT_OPEN" => Some(RequestRejection::SessionNotOpen),
        "CONFLICTING_RETRY" => Some(RequestRejection::ConflictingRetry),
        "STALE_SESSION" => Some(RequestRejection::StaleSession {
            current: SessionEpoch::new(tokens.get(1)?.parse().ok()?)?,
        }),
        "FUTURE_SESSION" => Some(RequestRejection::FutureSession {
            current: SessionEpoch::new(tokens.get(1)?.parse().ok()?)?,
        }),
        "STALE_SEQUENCE" => Some(RequestRejection::StaleSequence {
            highest: Sequence::new(tokens.get(1)?.parse().ok()?)?,
        }),
        "SEQUENCE_GAP" => Some(RequestRejection::SequenceGap {
            expected: Sequence::new(tokens.get(1)?.parse().ok()?)?,
        }),
        _ => None,
    }
}
