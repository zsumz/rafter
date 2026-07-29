//! Orchestration for a cluster of real `ledger-node` processes.
//!
//! This is consumer-owned exemplar code in the same sense the deterministic
//! driver is: it spawns processes, kills them, restarts them, and talks the
//! documented client protocol. It uses no privileged observation — no internal
//! hooks, no shared memory, no reaching into a replica's state — because an
//! external user orchestrating Rafter has none of those either.
//!
//! # Where real time enters, and how far it can go wrong
//!
//! The deterministic driver decides when a node ticks. This one cannot: a real
//! process ticks on its own clock, a real socket delivers when it delivers, and
//! a real `SIGKILL` lands where it lands. Real time is therefore load bearing
//! in exactly four places, and each is bounded rather than assumed:
//!
//! 1. **Waiting for a condition.** Every wait is a predicate polled on a short
//!    interval against a deadline ([`wait_until`]). There is no "sleep and
//!    assume": a wait either observes the condition and returns, or fails the
//!    test with the condition it was waiting for. Slowness makes a test slower,
//!    never greener.
//! 2. **Election timing.** Election timeouts are distinct per node and Rafter's
//!    election jitter defaults to zero, so which replica wins a given election
//!    is determined rather than raced; only *when* it takes office is timing.
//!    Determined is not the same as obvious: a first election is won by the
//!    shortest timeout, while a failover is won by the *longest* one among the
//!    survivors, because pre-vote leader stickiness makes every other survivor
//!    refuse the shortest replica's poll until its own timeout has elapsed.
//!    Tests assert the first case exactly and assert only "a survivor leads"
//!    for the second, so no test depends on that asymmetry holding.
//! 3. **Where a `SIGKILL` lands.** A kill during an in-flight write genuinely
//!    may land before or after the commit point, and no harness can pin that
//!    down. Tests that kill mid-write assert the property that holds for both
//!    readings — the command took effect exactly once, or provably not at all —
//!    rather than asserting one reading and hoping.
//! 4. **Whether a kill leaves a restartable journal.** An append writes and
//!    syncs a frame before it seals that frame. A kill between those operations
//!    leaves a complete unsealed frame that recovery refuses because it cannot
//!    distinguish interrupted publication from damage. [`ProcessCluster::restart`]
//!    treats `NEEDS_REPAIR` as that legitimate branch: it records the refusal,
//!    repairs only after the process names the remedy, and lets ordinary
//!    replication restore any discarded suffix.
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
    io::{BufRead, BufReader, Write},
    net::{SocketAddr, TcpStream},
    path::{Path, PathBuf},
    process::{Child, ChildStdout, Command as OsCommand, Stdio},
    sync::mpsc::{self, Receiver},
    thread,
    time::{Duration, Instant},
};

use rafter::{LogIndex, NodeId};
use rafter_reference_ledger::{
    AccountId, ApplyDisposition, BusinessRejection, Command, HistoryEvent, LedgerConfig,
    LedgerQuery, LedgerQueryResult, LedgerResponse, LedgerSummary, MutationResult, OperationId,
    RequestRejection, Sequence, SessionEpoch,
};

use crate::scratch::ScratchDir;

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
/// running the whole suite in parallel. Tighter values made leadership flap
/// under exactly that load, which is a genuine property of the protocol under a
/// real clock and one no in-process driver can show. No test depends on
/// leadership *not* changing, so this only buys the suite fewer detours.
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

/// Polls `predicate` until it yields a value or `WAIT_TIMEOUT` elapses.
///
/// This is the only shape of waiting this harness performs. It never sleeps for
/// a fixed duration and then proceeds, because that turns a slow machine into a
/// wrong answer instead of a slow one.
///
/// # Panics
///
/// Panics with `description` when the deadline passes first.
pub fn wait_until<T>(description: &str, mut predicate: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + WAIT_TIMEOUT;
    loop {
        if let Some(value) = predicate() {
            return value;
        }
        assert!(
            Instant::now() < deadline,
            "timed out after {WAIT_TIMEOUT:?} waiting for {description}"
        );
        thread::sleep(POLL_INTERVAL);
    }
}

/// Terminal outcome of one submitted command, as this client observed it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubmitOutcome {
    /// The replica returned a replicated response.
    Applied {
        disposition: ApplyDisposition,
        response: LedgerResponse,
    },
    /// The replica refused the command before it entered any log.
    NotCommitted {
        reason: String,
        leader_hint: Option<NodeId>,
    },
    /// The replica was not serving; the command was never proposed.
    NotReady { applied: u64, committed: u64 },
    /// The client cannot tell whether the command committed.
    Unknown { detail: String },
}

/// Terminal outcome of one query.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QueryOutcome {
    /// The query returned a result.
    Ready(LedgerQueryResult),
    /// The query returned no value, so it constrains no ordering.
    Abandoned { detail: String },
    /// The replica was not serving.
    NotReady { applied: u64, committed: u64 },
}

impl QueryOutcome {
    /// Returns the result when the query answered.
    pub fn result(&self) -> Option<LedgerQueryResult> {
        match self {
            Self::Ready(result) => Some(*result),
            _ => None,
        }
    }

    /// Returns the summary when this was an answered summary query.
    pub fn summary(&self) -> Option<LedgerSummary> {
        match self.result() {
            Some(LedgerQueryResult::Summary(summary)) => Some(summary),
            _ => None,
        }
    }

    /// Returns the balance of an answered account query.
    ///
    /// `None` means the account is not open, which is the only absence a
    /// caller here has to reason about. A query that did not answer at all is
    /// a different kind of event and panics rather than collapsing into the
    /// same `None`: a test asserting on a balance has already decided that the
    /// query must have answered.
    #[track_caller]
    pub fn account_balance(&self) -> Option<u64> {
        match self.result() {
            Some(LedgerQueryResult::Account { balance, .. }) => balance,
            other => panic!("expected an answered account query, observed {other:?}"),
        }
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

/// How a restarted replica got its durable journal open.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Escalation {
    /// The replica whose journal refused a plain restart.
    pub node_id: NodeId,
    /// The explicit recovery mode selected after the refusal.
    pub mode: String,
    /// The lifecycle line that authorized the escalation.
    pub refusal: String,
}

/// What a freshly spawned replica settled into.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Started {
    /// Recovery completed and the replica serves from this applied floor.
    Ready { applied: LogIndex },
    /// The durable journal requires an explicit repair decision.
    NeedsRepair { line: String },
}

/// A blocking client connection speaking the documented line protocol.
///
/// The parsing here is written against the protocol document rather than shared
/// with the binary that produces it. That is deliberate: the two halves of a
/// wire contract should be independent, and a change that broke one without the
/// other is exactly what this suite should catch.
#[derive(Debug)]
struct LedgerClient {
    reader: BufReader<TcpStream>,
    writer: TcpStream,
}

impl LedgerClient {
    fn connect(addr: SocketAddr) -> std::io::Result<Self> {
        let stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))?;
        stream.set_read_timeout(Some(CLIENT_READ_TIMEOUT))?;
        stream.set_write_timeout(Some(Duration::from_secs(5)))?;
        Ok(Self {
            reader: BufReader::new(stream.try_clone()?),
            writer: stream,
        })
    }

    fn request(&mut self, line: &str) -> std::io::Result<String> {
        writeln!(self.writer, "{line}")?;
        self.writer.flush()?;
        let mut response = String::new();
        if self.reader.read_line(&mut response)? == 0 {
            return Err(std::io::Error::from(std::io::ErrorKind::UnexpectedEof));
        }
        Ok(response.trim_end().to_string())
    }
}

/// One `ledger-node` process.
#[derive(Debug)]
pub struct NodeProcess {
    node_id: NodeId,
    child: Child,
    lifecycle: Receiver<String>,
    /// Lifecycle lines already read off the channel, kept so a later question
    /// about an earlier line can still be answered.
    seen: Vec<String>,
    client_addr: SocketAddr,
    client: Option<LedgerClient>,
}

impl NodeProcess {
    /// Spawns one replica and waits for it to open its client port.
    ///
    /// Spawning deliberately does not wait for readiness. A caller that wants a
    /// serving replica asks for one with [`NodeProcess::wait_ready`]; a caller
    /// testing the readiness gate needs the window in between.
    pub fn spawn(cluster_dir: &Path, node_id: NodeId, config: LedgerConfig) -> Self {
        Self::spawn_inner(cluster_dir, node_id, config, false)
    }

    /// Spawns one replica with the destructive application-store repair on.
    ///
    /// A replica whose journal will not open refuses to serve, and this is the
    /// only way past that refusal. It is a separate constructor because it is a
    /// separate decision: the repair discards a region that may hold
    /// transactions the replica already acknowledged, and no restart ever
    /// reaches it by accident.
    pub fn spawn_repairing(cluster_dir: &Path, node_id: NodeId, config: LedgerConfig) -> Self {
        Self::spawn_inner(cluster_dir, node_id, config, true)
    }

    fn spawn_inner(
        cluster_dir: &Path,
        node_id: NodeId,
        config: LedgerConfig,
        repair_app_store: bool,
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

        let mut command = OsCommand::new(env!("CARGO_BIN_EXE_ledger-node"));
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
            .arg("--max-accounts")
            .arg(config.max_accounts().to_string())
            .arg("--repair-app-store")
            .arg(if repair_app_store { "true" } else { "false" })
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());

        let mut child = command
            .spawn()
            .unwrap_or_else(|error| panic!("could not spawn replica {}: {error}", node_id.0));
        let stdout = child.stdout.take().expect("replica stdout is piped");
        let lifecycle = spawn_lifecycle_reader(stdout);

        let mut process = Self {
            node_id,
            child,
            lifecycle,
            seen: Vec::new(),
            client_addr: "127.0.0.1:0".parse().expect("a placeholder address parses"),
            client: None,
        };
        let listening = process.wait_for_line("LISTENING");
        process.client_addr = listening
            .split_whitespace()
            .nth(2)
            .expect("a LISTENING line names an address")
            .parse()
            .expect("the announced client address parses");
        process
    }

    /// Returns this replica's client address.
    pub const fn client_addr(&self) -> SocketAddr {
        self.client_addr
    }

    /// Waits for this replica to either finish recovery or request repair.
    ///
    /// Both are terminal startup outcomes. Returning the refusal is what lets a
    /// restart harness escalate deliberately rather than panic on a legal crash
    /// window or repair without the process first authorizing it.
    fn wait_started(&mut self) -> Started {
        let node_id = self.node_id;
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            self.drain_lifecycle();
            if let Some(line) = self
                .seen
                .iter()
                .find(|line| line.starts_with("NEEDS_REPAIR"))
            {
                return Started::NeedsRepair { line: line.clone() };
            }
            if let Some(line) = self.seen.iter().find(|line| line.starts_with("READY")) {
                return Started::Ready {
                    applied: LogIndex(
                        line.split_whitespace()
                            .nth(3)
                            .expect("a READY line names an applied index")
                            .parse()
                            .expect("the announced applied index parses"),
                    ),
                };
            }
            assert!(
                Instant::now() < deadline,
                "replica {} neither served nor requested repair; it announced {:?}",
                node_id.0,
                self.seen
            );
            thread::sleep(POLL_INTERVAL);
        }
    }

    /// Waits for this replica to announce complete recovery.
    ///
    /// Returns the applied index it recovered to, which is the durable floor a
    /// restart test asserts against.
    ///
    /// # Panics
    ///
    /// Panics when the journal requests repair, because a caller that asks only
    /// for readiness has already decided that plain recovery must succeed.
    #[track_caller]
    pub fn wait_ready(&mut self) -> LogIndex {
        match self.wait_started() {
            Started::Ready { applied } => applied,
            Started::NeedsRepair { line } => {
                panic!("replica {} refused to serve: {line}", self.node_id.0)
            }
        }
    }

    /// Returns this replica's directory under the cluster root.
    ///
    /// A test that wants to reach past the process and at the artifact — to
    /// corrupt a journal, or to check what a repair left — needs the same path
    /// the replica uses.
    pub fn node_dir(cluster_dir: &Path, node_id: NodeId) -> PathBuf {
        cluster_dir.join(format!("node-{}", node_id.0))
    }

    /// Returns whether this replica has already announced readiness.
    ///
    /// Non-blocking, so a test can assert that a replica is *not* ready without
    /// waiting for it to become so.
    pub fn has_announced(&mut self, prefix: &str) -> bool {
        self.drain_lifecycle();
        self.seen.iter().any(|line| line.starts_with(prefix))
    }

    /// Waits for a lifecycle line beginning with `prefix`.
    pub fn wait_for_line(&mut self, prefix: &str) -> String {
        let node_id = self.node_id;
        // Borrowck: the predicate needs `self` mutably, so the wait is written
        // inline rather than through `wait_until`.
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            self.drain_lifecycle();
            if let Some(line) = self.seen.iter().find(|line| line.starts_with(prefix)) {
                return line.clone();
            }
            assert!(
                Instant::now() < deadline,
                "replica {} never announced {prefix}; it announced {:?}",
                node_id.0,
                self.seen
            );
            thread::sleep(POLL_INTERVAL);
        }
    }

    /// Sends one protocol line and returns the raw response.
    ///
    /// A connection failure is reported rather than panicked: a client whose
    /// replica was killed mid-request observes exactly this, and turning it
    /// into a panic would delete the unknown outcome the suite is here to test.
    pub fn ask(&mut self, line: &str) -> Result<String, String> {
        for attempt in 0..2 {
            if self.client.is_none() {
                match LedgerClient::connect(self.client_addr) {
                    Ok(client) => self.client = Some(client),
                    Err(error) => return Err(format!("connect failed: {error}")),
                }
            }
            let client = self.client.as_mut().expect("a client was just connected");
            match client.request(line) {
                Ok(response) => return Ok(response),
                Err(error) => {
                    self.client = None;
                    // One reconnect, and only on the first attempt: a cached
                    // connection may have been closed by an earlier kill, which
                    // says nothing about this request. A second failure is the
                    // replica being gone.
                    if attempt == 1 {
                        return Err(format!("request failed: {error}"));
                    }
                }
            }
        }
        Err(String::from("request failed after one reconnect"))
    }

    /// Kills this replica with `SIGKILL` and reaps it.
    ///
    /// `SIGKILL` is the harshest exit available and runs no cleanup path, which
    /// is the point: every recovery this suite asserts is recovery from a
    /// process that was given no chance to tidy up.
    pub fn kill(&mut self) {
        self.client = None;
        drop(self.child.kill());
        drop(self.child.wait());
    }

    /// Asks this replica to stop cleanly and waits for it to exit.
    pub fn shutdown(&mut self) {
        drop(self.ask("SHUTDOWN"));
        self.client = None;
        drop(self.child.wait());
    }

    fn drain_lifecycle(&mut self) {
        while let Ok(line) = self.lifecycle.try_recv() {
            self.seen.push(line);
        }
    }
}

impl Drop for NodeProcess {
    /// A failing test must not leave a replica holding a port and a directory.
    fn drop(&mut self) {
        drop(self.child.kill());
        drop(self.child.wait());
    }
}

fn spawn_lifecycle_reader(stdout: ChildStdout) -> Receiver<String> {
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        for line in BufReader::new(stdout).lines().map_while(Result::ok) {
            if tx.send(line).is_err() {
                return;
            }
        }
    });
    rx
}

/// A submission started but not yet resolved.
///
/// Its history invocation is already recorded, so the operation's real-time
/// interval is open from the moment this handle exists. That is what lets a
/// test kill a leader *during* a write and still record a history the checker
/// can reason about.
#[derive(Debug)]
pub struct PendingSubmit {
    operation_id: OperationId,
    handle: thread::JoinHandle<Result<String, String>>,
}

/// A cluster of three real replica processes.
#[derive(Debug)]
pub struct ProcessCluster {
    root: ScratchDir,
    config: LedgerConfig,
    nodes: BTreeMap<NodeId, NodeProcess>,
    history: Vec<HistoryEvent>,
    escalations: Vec<Escalation>,
    next_operation_id: u64,
}

impl ProcessCluster {
    /// Spawns three replicas and waits for every one of them to serve.
    pub fn start(label: &str, config: LedgerConfig) -> Self {
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

    /// Returns the ledger bounds every replica runs under.
    pub const fn config(&self) -> LedgerConfig {
        self.config
    }

    /// Returns the directory holding every replica's durable state.
    ///
    /// A test that wants to reach past the processes and at the artifacts — to
    /// corrupt a journal, or to see what a repair left — needs the same root
    /// the replicas were spawned with.
    pub fn root(&self) -> &Path {
        self.root.path()
    }

    /// Returns the recorded client history.
    pub fn history(&self) -> &[HistoryEvent] {
        &self.history
    }

    /// Returns every restart that required explicit journal repair.
    ///
    /// Empty is the common case, not an assumption tests may make: a kill can
    /// legitimately land after a frame sync and before its seal.
    pub fn escalations(&self) -> &[Escalation] {
        &self.escalations
    }

    /// Returns the live replicas, lowest identifier first.
    pub fn live_nodes(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    /// Returns one live replica's process handle.
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
    /// shortest timeout wins. Tests assert the first case exactly and assert
    /// only "a survivor leads" for the second.
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
    /// trails the election by a round trip. Waiting for it is how a client that
    /// routes on a leader hint behaves; asserting it the instant an election
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
    /// ledger's request identity makes a retry idempotent. Only refusals are
    /// retried: an unknown outcome is returned to the caller untouched, because
    /// deciding what to do about one is the caller's contract obligation and
    /// hiding it here would delete the case the suite exists to test.
    pub fn submit_to_leader(&mut self, command: &Command) -> SubmitOutcome {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            let leader = self.wait_for_leader();
            let outcome = self.submit(leader, command);
            match &outcome {
                SubmitOutcome::NotCommitted { .. } | SubmitOutcome::NotReady { .. } => {
                    assert!(
                        Instant::now() < deadline,
                        "no replica accepted the command within {WAIT_TIMEOUT:?}: {outcome:?}"
                    );
                    thread::sleep(POLL_INTERVAL);
                }
                _ => return outcome,
            }
        }
    }

    /// Runs one linearizable query against the current leader, retrying while
    /// it answers nothing.
    ///
    /// A barrier refused or canceled by a leadership change delivered no value,
    /// so retrying it is a client decision rather than a weakening of the
    /// check: every attempt is recorded, and an attempt that answered nothing
    /// constrains no ordering.
    pub fn query_leader(&mut self, query: LedgerQuery) -> QueryOutcome {
        let deadline = Instant::now() + WAIT_TIMEOUT;
        loop {
            let leader = self.wait_for_leader();
            let outcome = self.query(leader, query);
            if matches!(outcome, QueryOutcome::Ready(_)) {
                return outcome;
            }
            assert!(
                Instant::now() < deadline,
                "no replica answered the query within {WAIT_TIMEOUT:?}: {outcome:?}"
            );
            thread::sleep(POLL_INTERVAL);
        }
    }

    /// Kills one replica with `SIGKILL` and forgets it.
    pub fn kill(&mut self, node_id: NodeId) {
        let mut process = self
            .nodes
            .remove(&node_id)
            .unwrap_or_else(|| panic!("replica {} is not running", node_id.0));
        process.kill();
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
    /// A plain restart is always attempted first. If the journal requests
    /// repair, the harness records that refusal and retries once with the
    /// explicit repair flag. Repair is never selected speculatively.
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
            Started::NeedsRepair { line } => {
                drop(process);
                self.escalations.push(Escalation {
                    node_id,
                    mode: String::from("repair"),
                    refusal: line,
                });
                process = NodeProcess::spawn_repairing(self.root.path(), node_id, self.config);
                process.wait_ready()
            }
        };
        self.nodes.insert(node_id, process);
        applied
    }

    /// Submits one command and waits for its terminal outcome.
    pub fn submit(&mut self, node_id: NodeId, command: &Command) -> SubmitOutcome {
        let pending = self.begin_submit(node_id, command);
        self.resolve_submit(pending)
    }

    /// Starts one command on its own connection without waiting for it.
    ///
    /// The submission runs on its own thread and its own socket, so the caller
    /// stays free to kill a replica while the command is in flight.
    pub fn begin_submit(&mut self, node_id: NodeId, command: &Command) -> PendingSubmit {
        let operation_id = self.allocate_operation_id();
        self.history.push(HistoryEvent::Invoked {
            operation_id,
            command: command.clone(),
        });
        let line = render_command(command);
        let addr = self.node_mut(node_id).client_addr();
        let handle = thread::spawn(move || {
            let mut client = LedgerClient::connect(addr).map_err(|error| error.to_string())?;
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

    /// Runs one linearizable query and records it in the history.
    pub fn query(&mut self, node_id: NodeId, query: LedgerQuery) -> QueryOutcome {
        let operation_id = self.allocate_operation_id();
        self.history.push(HistoryEvent::QueryInvoked {
            operation_id,
            query,
        });
        let line = render_query("QUERY", query);
        let outcome = match self.node_mut(node_id).ask(&line) {
            Ok(response) => parse_query_response(&response),
            Err(detail) => QueryOutcome::Abandoned { detail },
        };
        match &outcome {
            QueryOutcome::Ready(result) => self.history.push(HistoryEvent::QueryCompleted {
                operation_id,
                result: *result,
            }),
            // A refused, gated, or lost query delivered no value, so it
            // constrains no ordering. It stays in the history because an issued
            // query that answered nothing is evidence about availability.
            QueryOutcome::Abandoned { .. } | QueryOutcome::NotReady { .. } => self
                .history
                .push(HistoryEvent::QueryAbandoned { operation_id }),
        }
        outcome
    }

    /// Reads one replica's own applied state, which may be stale.
    ///
    /// A local read is deliberately absent from the history: it is not a
    /// linearizable operation, and recording it as one would ask the checker to
    /// explain an answer the contract never promised.
    pub fn local_read(&mut self, node_id: NodeId, query: LedgerQuery) -> QueryOutcome {
        let line = render_query("LOCAL", query);
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
        match outcome {
            SubmitOutcome::Applied { response, .. } => self.history.push(HistoryEvent::Completed {
                operation_id,
                response: response.clone(),
            }),
            // Both refusals are provable, and for the same reason: the bytes
            // never reached a replicated log. `NOTCOMMITTED` is the app layer's
            // pre-append admission check reported across the process boundary;
            // `NOTREADY` is the replica's own readiness gate refusing before it
            // ever called into `rafter-app`, which is strictly earlier. Neither
            // leaves a copy of the attempt anywhere.
            SubmitOutcome::NotCommitted { .. } | SubmitOutcome::NotReady { .. } => self
                .history
                .push(HistoryEvent::NotCommitted { operation_id }),
            SubmitOutcome::Unknown { .. } => {
                self.history.push(HistoryEvent::Unknown { operation_id });
            }
        }
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
fn render_command(command: &Command) -> String {
    match command {
        Command::OpenSession {
            client_id,
            session_epoch,
        } => format!("OPEN_SESSION {} {}", client_id.get(), session_epoch.get()),
        Command::Execute { request, mutation } => {
            let mutation = match mutation {
                rafter_reference_ledger::Mutation::OpenAccount { account_id } => {
                    format!("OPEN {}", account_id.get())
                }
                rafter_reference_ledger::Mutation::Deposit { account_id, amount } => {
                    format!("DEPOSIT {} {}", account_id.get(), amount.get())
                }
                rafter_reference_ledger::Mutation::Transfer { from, to, amount } => {
                    format!("TRANSFER {} {} {}", from.get(), to.get(), amount.get())
                }
                rafter_reference_ledger::Mutation::CloseAccount { account_id } => {
                    format!("CLOSE {}", account_id.get())
                }
            };
            format!(
                "SUBMIT {} {} {} {mutation}",
                request.client_id.get(),
                request.session_epoch.get(),
                request.sequence.get()
            )
        }
    }
}

fn render_query(verb: &str, query: LedgerQuery) -> String {
    match query {
        LedgerQuery::GetAccount { account_id } => {
            format!("{verb} ACCOUNT {}", account_id.get())
        }
        LedgerQuery::GetLedgerSummary => format!("{verb} SUMMARY"),
    }
}

fn parse_status(response: &str) -> Option<NodeStatus> {
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

fn parse_submit_response(response: &str) -> SubmitOutcome {
    let tokens: Vec<&str> = response.split_whitespace().collect();
    match tokens.first().copied() {
        Some("OK") => {
            let disposition = tokens
                .get(1)
                .and_then(|token| parse_disposition(token))
                .unwrap_or_else(|| panic!("unparseable disposition in {response:?}"));
            let response_value = parse_ledger_response(&tokens[2..])
                .unwrap_or_else(|| panic!("unparseable response in {response:?}"));
            SubmitOutcome::Applied {
                disposition,
                response: response_value,
            }
        }
        Some("NOTCOMMITTED") => SubmitOutcome::NotCommitted {
            reason: tokens.get(1).copied().unwrap_or_default().to_string(),
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

fn parse_query_response(response: &str) -> QueryOutcome {
    let tokens: Vec<&str> = response.split_whitespace().collect();
    match (tokens.first().copied(), tokens.get(1).copied()) {
        (Some("OK"), Some("ACCOUNT")) => QueryOutcome::Ready(LedgerQueryResult::Account {
            account_id: AccountId::new(tokens[2].parse().expect("account id parses")),
            balance: (tokens[3] != "-").then(|| tokens[3].parse().expect("balance parses")),
        }),
        (Some("OK"), Some("SUMMARY")) => {
            QueryOutcome::Ready(LedgerQueryResult::Summary(LedgerSummary {
                open_accounts: tokens[2].parse().expect("open accounts parses"),
                total_balance: tokens[3].parse().expect("total balance parses"),
                successful_deposits: tokens[4].parse().expect("deposits parse"),
            }))
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

fn parse_ledger_response(tokens: &[&str]) -> Option<LedgerResponse> {
    match tokens.first().copied()? {
        "SESSION" => Some(LedgerResponse::SessionOpened {
            session_epoch: SessionEpoch::new(tokens.get(1)?.parse().ok()?)?,
        }),
        "MUTATION" => Some(LedgerResponse::Mutation(parse_mutation_result(
            &tokens[1..],
        )?)),
        "REJECTED" => Some(LedgerResponse::Rejected(parse_request_rejection(
            &tokens[1..],
        )?)),
        _ => None,
    }
}

fn parse_mutation_result(tokens: &[&str]) -> Option<MutationResult> {
    match tokens.first().copied()? {
        "ACCOUNT_OPENED" => Some(MutationResult::AccountOpened),
        "ACCOUNT_CLOSED" => Some(MutationResult::AccountClosed),
        "DEPOSITED" => Some(MutationResult::Deposited {
            balance: tokens.get(1)?.parse().ok()?,
        }),
        "TRANSFERRED" => Some(MutationResult::Transferred {
            from_balance: tokens.get(1)?.parse().ok()?,
            to_balance: tokens.get(2)?.parse().ok()?,
        }),
        "BUSINESS_REJECTED" => Some(MutationResult::Rejected(parse_business_rejection(
            tokens.get(1)?,
        )?)),
        _ => None,
    }
}

fn parse_business_rejection(token: &str) -> Option<BusinessRejection> {
    match token {
        "AccountAlreadyExists" => Some(BusinessRejection::AccountAlreadyExists),
        "AccountCapacityExceeded" => Some(BusinessRejection::AccountCapacityExceeded),
        "AccountNotFound" => Some(BusinessRejection::AccountNotFound),
        "SameAccount" => Some(BusinessRejection::SameAccount),
        "InsufficientFunds" => Some(BusinessRejection::InsufficientFunds),
        "BalanceOverflow" => Some(BusinessRejection::BalanceOverflow),
        "SupplyOverflow" => Some(BusinessRejection::SupplyOverflow),
        "AccountNotEmpty" => Some(BusinessRejection::AccountNotEmpty),
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
