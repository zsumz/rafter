//! Bounded production-composition acceptance process for the fenced lock.
//!
//! This is deliberately one fixture, not a generic Rafter daemon. It reuses the
//! integration process's proven transactional application store, durable Raft
//! stores, control-plane checkpoint, and service loop. Its production boundary
//! is caller-owned: durable monotonic replica identity, the public mutually
//! authenticated TLS peer transport, durable connection epochs, explicit
//! queue/connection limits, complete recovery readiness, and JSON Lines evidence.
//!
//! The file-backed Raft store proves publication and recovery correctness. It is
//! not described as a segmented high-throughput WAL. The committed test
//! credentials are a dedicated non-production CA and protect no external
//! secret.
//!
//! # Configuration
//!
//! Every option is an argument, and every argument has an environment fallback
//! so a supervisor can set them either way. Arguments win.
//!
//! ```text
//! --id <u64>
//! --members <initial-voter-id,...>
//! --cluster-dir <path>
//! --identity <path>
//! --tls-ca <path>
//! --tls-cert <path>
//! --tls-key <path>
//! --peer-certificates <id=path,...>
//! --client-listen <addr>          [127.0.0.1:0]
//! --peer-listen <addr>            [127.0.0.1:0]
//! --election-timeout-ticks <u64>  [8]
//! --tick-interval-ms <u64>        [20]
//! --ownership-wait-ms <u64>       [10000]
//! --request-timeout-ms <u64>      [5000]
//! --max-clients <u32>             [8]
//! --max-resources <u32>           [8]
//! --recover <open|repair|reseed>  [open]
//! --control-plane-fault-after <n> [test-only seam]
//! ```
//!
//! The client listener exists before Raft/app recovery so the readiness gate is
//! externally observable. Every service request is refused until identity, TLS,
//! transport session metadata, stores, checkpoint reconciliation, catch-up,
//! and worker startup have succeeded. Client connections, client lines, pending jobs,
//! peer connections, frame bytes, outbound queues, per-peer inbound queues, and
//! the global inbound queue all have explicit limits.

mod control_plane;
mod peer_link;
mod protocol;
#[path = "../lock-node/replica.rs"]
mod replica;

use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process::ExitCode,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, Receiver, SyncSender, TrySendError},
        Arc,
    },
    thread,
    time::{Duration, Instant},
};

use rafter::NodeId;
use rafter_app::membership::MembershipChange;
use rafter_reference_fenced_lock::{
    production::{load_active_replica, ReplicaIdentity},
    LockConfig, QueryOutcome, SubmitOutcome,
};

use peer_link::{
    max_frame_bytes, PeerLink, PeerTlsConfig, PeerTlsPaths, GLOBAL_INBOUND_QUEUE_LEN,
    MAX_PEER_CONNECTIONS, PEER_INBOUND_QUEUE_LEN, PEER_SEND_QUEUE_LEN,
};
use protocol::{
    parse_request, render_applied, render_lock, render_not_committed, render_status, Readiness,
    Request,
};
use replica::{Answer, OpenError, OpenRequest, RecoveryMode, Replica};

const CLIENT_PENDING_LIMIT: usize = 64;
const MAX_CLIENT_CONNECTIONS: usize = 16;
const MAX_CLIENT_LINE_BYTES: usize = 4 * 1024;

/// How long the loop blocks waiting for a client request before it polls the
/// peer link and the clock again.
///
/// This is the loop's idle wakeup rate, and it bounds how long an arrived peer
/// frame waits to be delivered. It is deliberately well under the tick interval
/// — a poll slower than a tick would add latency to every round — and
/// deliberately not smaller than it needs to be, because a cluster of these
/// processes pays this rate per replica.
const LOOP_POLL_INTERVAL: Duration = Duration::from_millis(2);

/// How many client requests one pass of the loop answers before it goes back to
/// the protocol.
///
/// Sixty-four is comfortably more than the requests a correct client population
/// can have outstanding at once — a connection is strictly sequential, so the
/// queue depth is bounded by live connections, and this composition's default
/// client bound is eight — while still being few enough that a whole pass of
/// them finishes far inside one tick interval.
///
/// **The bound is the fairness property, not a throughput knob.** Draining until
/// the channel is quiet reads like an optimization and is a liveness bug: a
/// stream of immediately-answered requests never leaves the drain, so the pass
/// below never runs, and `STATUS` alone is enough to hold a replica off its
/// clock indefinitely. Everything the pass owes the cluster is behind it —
/// detecting a stored control-plane failure, delivering peer frames, ticking,
/// driving reads, expiring deadlines — and so is this process's own terminal
/// exit, which is why a replica that already knew its control plane was not
/// durable would keep answering `ABANDONED` forever instead of stopping nonzero.
const MAX_JOBS_PER_PASS: usize = 64;

/// How often a replica waiting for directory ownership tries to take it.
///
/// Retrying at the loop's own rate would hammer the lock hundreds of times a
/// second to learn something that changes once. This is slow enough to be
/// polite and fast enough that a restart is not perceptibly delayed by it.
const OWNERSHIP_RETRY_INTERVAL: Duration = Duration::from_millis(20);

fn main() -> ExitCode {
    let config = match Config::from_env_and_args(std::env::args().skip(1)) {
        Ok(config) => config,
        Err(detail) => {
            emit(&format!("FATAL {detail}"));
            return ExitCode::FAILURE;
        }
    };
    match run(&config) {
        Ok(()) => ExitCode::SUCCESS,
        Err(detail) => {
            emit(&format!("FATAL {detail}"));
            ExitCode::FAILURE
        }
    }
}

/// Everything one replica process needs to know about itself.
#[derive(Clone, Debug)]
struct Config {
    node_id: NodeId,
    members: Vec<NodeId>,
    cluster_dir: PathBuf,
    identity: ReplicaIdentity,
    identity_path: PathBuf,
    tls: PeerTlsPaths,
    client_listen: String,
    peer_listen: String,
    election_timeout_ticks: u64,
    tick_interval: Duration,
    ownership_wait: Duration,
    request_timeout: Duration,
    lock: LockConfig,
    recover: RecoveryMode,
    control_plane_fault_after: Option<u64>,
}

impl Config {
    #[allow(
        clippy::too_many_lines,
        reason = "all operator configuration is parsed and cross-validated in one fail-closed boundary"
    )]
    fn from_env_and_args(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut values: BTreeMap<String, String> = BTreeMap::new();
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            let name = arg
                .strip_prefix("--")
                .ok_or_else(|| format!("unexpected argument {arg}"))?
                .to_string();
            let value = args
                .next()
                .ok_or_else(|| format!("argument --{name} needs a value"))?;
            values.insert(name, value);
        }

        let lookup = |name: &str, env: &str| -> Option<String> {
            values
                .get(name)
                .cloned()
                .or_else(|| std::env::var(env).ok())
        };
        let required = |name: &str, env: &str| -> Result<String, String> {
            lookup(name, env).ok_or_else(|| format!("--{name} or {env} is required"))
        };
        let parsed = |name: &str, env: &str, fallback: u64| -> Result<u64, String> {
            match lookup(name, env) {
                Some(value) => value
                    .parse()
                    .map_err(|_| format!("--{name} must be an integer")),
                None => Ok(fallback),
            }
        };

        let node_id = NodeId(
            required("id", "RAFTER_LOCK_NODE_ID")?
                .parse()
                .map_err(|_| String::from("--id must be an integer"))?,
        );
        let members: Vec<NodeId> = required("members", "RAFTER_LOCK_MEMBERS")?
            .split(',')
            .map(|token| {
                token
                    .trim()
                    .parse()
                    .map(NodeId)
                    .map_err(|_| String::from("--members must be a comma-separated id list"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        if members.is_empty() {
            return Err(String::from(
                "--members must name at least one initial voter",
            ));
        }
        let mut unique_members = members.clone();
        unique_members.sort_unstable();
        unique_members.dedup();
        if unique_members.len() != members.len() {
            return Err(String::from("--members must not repeat a node id"));
        }
        let cluster_dir = PathBuf::from(required("cluster-dir", "RAFTER_LOCK_CLUSTER_DIR")?);
        let identity_path = PathBuf::from(required("identity", "RAFTER_LOCK_IDENTITY")?);
        let identity = load_active_replica(&cluster_dir, &identity_path, replica::GROUP_ID.0)
            .map_err(|error| format!("replica identity refused: {error}"))?;
        if identity.node_id != node_id {
            return Err(format!(
                "--id {} disagrees with durable identity {}",
                node_id.0, identity.node_id.0
            ));
        }
        let peer_certificates = parse_peer_certificates(&required(
            "peer-certificates",
            "RAFTER_LOCK_PEER_CERTIFICATES",
        )?)?;
        if !peer_certificates.contains_key(&node_id) {
            return Err(String::from(
                "--peer-certificates must map this node's identity",
            ));
        }
        if let Some(missing) = members
            .iter()
            .find(|member| !peer_certificates.contains_key(member))
        {
            return Err(format!(
                "initial voter {} has no configured peer certificate",
                missing.0
            ));
        }
        let tls = PeerTlsPaths {
            ca: PathBuf::from(required("tls-ca", "RAFTER_LOCK_TLS_CA")?),
            certificate: PathBuf::from(required("tls-cert", "RAFTER_LOCK_TLS_CERT")?),
            private_key: PathBuf::from(required("tls-key", "RAFTER_LOCK_TLS_KEY")?),
            peer_certificates,
        };

        let max_clients = u32::try_from(parsed("max-clients", "RAFTER_LOCK_MAX_CLIENTS", 8)?)
            .map_err(|_| String::from("--max-clients must fit in a u32"))?;
        let max_resources = u32::try_from(parsed("max-resources", "RAFTER_LOCK_MAX_RESOURCES", 8)?)
            .map_err(|_| String::from("--max-resources must fit in a u32"))?;
        let lock = LockConfig::new(max_clients, max_resources)
            .map_err(|error| format!("invalid lock bounds: {error:?}"))?;

        // Anything other than one of the three names is refused rather than
        // silently falling back. A destructive option must not be reachable
        // through a typo, and neither must a typo silently disable one an
        // operator meant to set.
        let recover = match lookup("recover", "RAFTER_LOCK_RECOVER") {
            Some(value) => RecoveryMode::parse(&value)
                .ok_or_else(|| format!("--recover must be open, repair, or reseed; got {value}"))?,
            None => RecoveryMode::Open,
        };

        Ok(Self {
            node_id,
            members,
            cluster_dir,
            identity,
            identity_path,
            tls,
            client_listen: lookup("client-listen", "RAFTER_LOCK_CLIENT_LISTEN")
                .unwrap_or_else(|| String::from("127.0.0.1:0")),
            peer_listen: lookup("peer-listen", "RAFTER_LOCK_PEER_LISTEN")
                .unwrap_or_else(|| String::from("127.0.0.1:0")),
            election_timeout_ticks: parsed(
                "election-timeout-ticks",
                "RAFTER_LOCK_ELECTION_TICKS",
                8,
            )?,
            tick_interval: Duration::from_millis(parsed(
                "tick-interval-ms",
                "RAFTER_LOCK_TICK_MS",
                20,
            )?),
            ownership_wait: Duration::from_millis(parsed(
                "ownership-wait-ms",
                "RAFTER_LOCK_OWNERSHIP_WAIT_MS",
                10_000,
            )?),
            request_timeout: Duration::from_millis(parsed(
                "request-timeout-ms",
                "RAFTER_LOCK_REQUEST_MS",
                5_000,
            )?),
            lock,
            recover,
            // Absent for every replica an operator starts. See
            // `OpenRequest::control_plane_fault_after` for why the seam exists
            // and why it is addressed by client operation rather than by write.
            control_plane_fault_after: match lookup(
                "control-plane-fault-after",
                "RAFTER_LOCK_CONTROL_PLANE_FAULT_AFTER",
            ) {
                Some(value) => {
                    Some(value.parse().map_err(|_| {
                        String::from("--control-plane-fault-after must be an integer")
                    })?)
                }
                None => None,
            },
        })
    }

    fn node_dir(&self) -> PathBuf {
        self.cluster_dir.join(format!("node-{}", self.node_id.0))
    }
}

fn parse_peer_certificates(value: &str) -> Result<BTreeMap<NodeId, PathBuf>, String> {
    let mut peers = BTreeMap::new();
    for entry in value.split(',') {
        let (node, path) = entry
            .split_once('=')
            .ok_or_else(|| String::from("--peer-certificates entries must be id=path"))?;
        let node = NodeId(
            node.parse()
                .map_err(|_| String::from("peer certificate id must be an integer"))?,
        );
        if path.is_empty() {
            return Err(String::from("peer certificate path must not be empty"));
        }
        if peers.insert(node, PathBuf::from(path)).is_some() {
            return Err(format!("peer certificate id {} is repeated", node.0));
        }
    }
    if peers.is_empty() {
        return Err(String::from(
            "--peer-certificates must configure at least one identity",
        ));
    }
    Ok(peers)
}

/// One client request waiting for the loop that owns the replica.
#[derive(Debug)]
struct Job {
    request: Request,
    reply: ClientReply,
}

/// The client thread that owns a request's socket and its flush acknowledgement.
#[derive(Debug)]
struct ClientReply {
    response: mpsc::Sender<String>,
    flushed: Receiver<()>,
}

impl ClientReply {
    /// Hands the answer to the socket owner, optionally waiting until it has
    /// attempted the write before the process is allowed to exit.
    fn send(self, response: String, wait_for_flush: bool) {
        if self.response.send(response).is_ok() && wait_for_flush {
            let _ = self.flushed.recv();
        }
    }
}

/// Whether the replica exists yet, and whether it will serve again.
#[derive(Debug)]
enum State {
    /// The client port is open and every service request is refused while this
    /// process waits for exclusive ownership of its directory.
    Opening {
        deadline: Instant,
        next_attempt: Instant,
        announced: bool,
    },
    /// The replica is open; readiness is now the replica's own gate.
    Serving(Box<Replica>),
    /// The replica exists and will not serve again.
    ///
    /// **Two ways in, and both are about the record no other artifact holds.** A
    /// replica that cannot make its peer control plane durable is one whose next
    /// restart forgets a removal. A replica whose control-plane inputs contradict
    /// each other has no trustworthy statement to make about who is retired at
    /// all, and no later fact decides it. Either outranks anything the replica
    /// could still do for a client: what it would get wrong is permanent, and the
    /// client work it refuses will be retried against a replica that can still
    /// answer.
    ///
    /// It is a *state* rather than a flag checked at the next call because of
    /// where the failures come from. `submit`, `query`, `poll_pending`, and
    /// `abandon_waiters` answer their client with a value rather than a
    /// `Result`, so a persist failure inside one is stored instead of raised —
    /// and the next call with an error channel of its own is an unbounded number
    /// of accepted client requests away. Entering this at the top of the loop
    /// makes the refusal immediate.
    ///
    /// The replica is kept rather than dropped so `STATUS` still answers and the
    /// link counters still reach the `LINK` line. Everything else is refused,
    /// and `serve` returns the failure, so the process exits nonzero.
    Failed {
        replica: Box<Replica>,
        failure: TerminalFailure,
    },
}

#[derive(Debug, Default)]
struct ClientCounters {
    active_connections: AtomicUsize,
    pending: AtomicUsize,
    connection_full: AtomicU64,
    pending_full: AtomicU64,
    line_too_long: AtomicU64,
}

/// Why this process will not serve again.
///
/// **One shape for two facts that end identically and diagnose differently.**
/// Both are about the peer-control-plane record, both are unrecoverable by
/// restarting, and both must reach a supervisor as a nonzero exit rather than a
/// clean stop. What differs is the artifact an operator has to look at: one says
/// the record could not be *written*, and the other says the record and the log
/// *disagree*. Rendering them through one type keeps the ending in one place
/// while the lifecycle line stays specific.
#[derive(Clone, Debug, Eq, PartialEq)]
enum TerminalFailure {
    /// The peer control plane could not be made durable.
    Unpersisted(String),
    /// This replica's control-plane inputs contradict each other.
    Contradicted(String),
}

impl TerminalFailure {
    /// The lifecycle line this failure announces itself on.
    const fn lifecycle_tag(&self) -> &'static str {
        match self {
            Self::Unpersisted(_) => "CONTROL_PLANE_UNPERSISTED",
            Self::Contradicted(_) => "CONTROL_PLANE_CONTRADICTED",
        }
    }

    /// What this replica tells a client that asks for service.
    fn detail(&self) -> &str {
        match self {
            Self::Unpersisted(detail) | Self::Contradicted(detail) => detail,
        }
    }

    /// The reason the process ends with, which `main` renders as `FATAL`.
    fn message(&self) -> String {
        match self {
            Self::Unpersisted(detail) => {
                format!("the peer control plane could not be made durable: {detail}")
            }
            Self::Contradicted(detail) => format!(
                "this replica's peer control plane is contradicted and will not serve \
                 again: {detail}"
            ),
        }
    }
}

fn run(config: &Config) -> Result<(), String> {
    let node_dir = config.node_dir();
    std::fs::create_dir_all(&node_dir)
        .map_err(|error| format!("could not create {}: {error}", node_dir.display()))?;

    let tls = PeerTlsConfig::load(config.node_id, &config.tls)?;
    let (jobs_tx, jobs_rx) = mpsc::sync_channel(CLIENT_PENDING_LIMIT);
    let client_counters = Arc::new(ClientCounters::default());
    let client_listener = TcpListener::bind(&config.client_listen)
        .map_err(|error| format!("could not bind the client port: {error}"))?;
    let client_addr = client_listener
        .local_addr()
        .map_err(|error| format!("could not read the client address: {error}"))?;
    spawn_client_acceptor(client_listener, jobs_tx, Arc::clone(&client_counters));
    emit(&format!("LISTENING {} {client_addr}", config.node_id.0));

    let link = PeerLink::bind(
        &config.peer_listen,
        config.node_id,
        &config.cluster_dir,
        &node_dir,
        &tls,
    )
    .map_err(|error| format!("could not bind the peer port: {error}"))?;

    serve(
        config,
        &node_dir,
        &link,
        &jobs_rx,
        client_addr,
        &client_counters,
    )
}

struct LinkShutdownGuard<'a>(&'a PeerLink);

impl Drop for LinkShutdownGuard<'_> {
    fn drop(&mut self) {
        self.0.shut_down();
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "the process loop is one readable sequence; splitting it would hide the order"
)]
fn serve(
    config: &Config,
    node_dir: &std::path::Path,
    link: &PeerLink,
    jobs: &Receiver<Job>,
    client_addr: std::net::SocketAddr,
    client_counters: &ClientCounters,
) -> Result<(), String> {
    let mut state = State::Opening {
        deadline: Instant::now() + config.ownership_wait,
        next_attempt: Instant::now(),
        announced: false,
    };
    // Declared after `state`, so transport workers stop and join before the
    // recovered replica releases its filesystem ownership on every exit path.
    let _link_shutdown = LinkShutdownGuard(link);
    let mut responders: BTreeMap<u64, ClientReply> = BTreeMap::new();
    let mut next_ticket = 1_u64;
    let mut next_tick = Instant::now() + config.tick_interval;
    let mut announced_ready = false;
    let mut stopping = false;

    loop {
        if let Some(failure) = link.terminal_failure() {
            return Err(format!(
                "transport session metadata became unavailable: {failure}"
            ));
        }
        // Client requests first, so that a `SHUTDOWN` or a `STATUS` is answered
        // even when the replica does not exist yet — and at most
        // `MAX_JOBS_PER_PASS` of them, so that everything below this is reached
        // whatever the clients are doing.
        for slot in 0..MAX_JOBS_PER_PASS {
            // The first receive is this loop's idle wait and the rest are not.
            // Blocking again after a request has been answered would let a
            // momentary lull cost the protocol a full poll interval it never
            // needed to pay, and blocking on the first is what keeps an idle
            // replica off the CPU while still bounding how long an arrived peer
            // frame waits.
            let received = if slot == 0 {
                jobs.recv_timeout(LOOP_POLL_INTERVAL).ok()
            } else {
                jobs.try_recv().ok()
            };
            let Some(job) = received else {
                break;
            };
            client_counters.pending.fetch_sub(1, Ordering::Relaxed);
            let ticket = next_ticket;
            next_ticket += 1;
            match handle_job(&mut state, config, link, client_counters, &job, ticket) {
                Disposition::Answered(response) => {
                    let terminal = pass_is_terminal(&state);
                    job.reply
                        .send(harness_response(config, ticket, response), terminal);
                }
                Disposition::Pending => {
                    responders.insert(ticket, job.reply);
                }
                Disposition::Stop(response) => {
                    job.reply
                        .send(harness_response(config, ticket, response), true);
                    stopping = true;
                }
            }
            // A terminal state ends the pass rather than spending the budget on
            // it. `State::Failed` answers every request `ABANDONED`, and doing
            // that is not what the process is for once it has one: the work
            // below is its exit, and a pass that kept answering would postpone
            // the exit for exactly as long as clients kept asking.
            //
            // **A stored persistence failure counts, and counting it here is
            // what makes the refusal immediate.** `submit` and `query` answer
            // their client with a value rather than a `Result`, so a persist
            // failure inside one is *stored* and the state does not move — the
            // transition below is what moves it, and it is a whole job batch
            // away. Without this clause the remaining budget was spent
            // admitting work the process already knew it must not do: writes
            // started, reads served, and `STATUS` answered as if nothing had
            // happened, up to sixty-three times. Bounded fail-open is still
            // fail-open, and the bound is the wrong thing to be proud of.
            if stopping || pass_is_terminal(&state) {
                break;
            }
        }
        if stopping {
            break;
        }

        let now = Instant::now();
        match &mut state {
            State::Opening {
                deadline,
                next_attempt,
                announced,
            } => {
                if now < *next_attempt {
                    continue;
                }
                *next_attempt = now + OWNERSHIP_RETRY_INTERVAL;
                match Replica::open(OpenRequest {
                    node_dir,
                    node_id: config.node_id,
                    members: &config.members,
                    election_timeout_ticks: config.election_timeout_ticks,
                    lock_config: config.lock,
                    mode: config.recover,
                    transport: link.transport(),
                    validator: link.validator(),
                    control_plane_fault_after: config.control_plane_fault_after,
                }) {
                    Ok(replica) => {
                        // Recovery and directory ownership are established before
                        // any TLS worker can dial, accept, or mutate session state.
                        if let Err(error) = link.start(node_dir, config.node_id) {
                            link.shut_down();
                            return Err(format!(
                                "could not start the authenticated peer transport: {error}"
                            ));
                        }
                        // Published only after activation, so a peer never dials a
                        // process that does not own and run its recovered replica.
                        if let Err(error) = link.publish_address(node_dir) {
                            link.shut_down();
                            return Err(format!("could not publish the peer address: {error}"));
                        }
                        emit(&format!(
                            "PEER_LISTENING {} {}",
                            config.node_id.0,
                            link.local_addr()
                        ));
                        state = State::Serving(Box::new(replica));
                    }
                    Err(OpenError::DirectoryOwned { directory }) => {
                        if !*announced {
                            emit(&format!("WAITING_FOR_OWNERSHIP {}", config.node_id.0));
                            *announced = true;
                        }
                        if now >= *deadline {
                            return Err(format!(
                                "another process still owns {} after the ownership wait elapsed",
                                directory.display()
                            ));
                        }
                    }
                    // A store this mode cannot open is the one startup failure
                    // a restart cannot clear, so the readiness gate never opens
                    // and the process says why in a line a supervisor can match
                    // on rather than only inside `FATAL`.
                    Err(error @ OpenError::NeedsDecision { .. }) => {
                        emit(&format!("NEEDS_DECISION {} {error}", config.node_id.0));
                        return Err(error.to_string());
                    }
                    Err(error) => return Err(error.to_string()),
                }
            }
            // Terminal, and reached only through the arm below. The next pass's
            // job drain refuses whatever arrived in the meantime, and then this
            // breaks — so no client request is served after the failure and none
            // is silently dropped either.
            State::Failed { .. } => {
                stopping = true;
            }
            State::Serving(replica) => {
                // Before any protocol work this pass, and before the next job
                // drain. Both terminal conditions are checked here; see
                // `State::Failed` for why waiting for the next call with an
                // error channel is waiting too long.
                //
                // The persistence failure is asked about first because it is the
                // one that may already have been *stored* by an entry point with
                // no error channel, so it can be older than the contradiction
                // beside it — and the first failure is the one that explains why
                // this replica stopped.
                if let Some(failure) = terminal_failure(replica) {
                    emit(&format!(
                        "{} {} {}",
                        failure.lifecycle_tag(),
                        config.node_id.0,
                        failure.detail()
                    ));
                    replica.abandon_waiters(failure.detail());
                    for answer in replica.take_answers() {
                        let (ticket, response) = render_answer(answer);
                        if let Some(reply) = responders.remove(&ticket) {
                            reply.send(harness_response(config, ticket, response), true);
                        }
                    }
                    let State::Serving(replica) = std::mem::replace(
                        &mut state,
                        State::Opening {
                            deadline: now,
                            next_attempt: now,
                            announced: true,
                        },
                    ) else {
                        unreachable!("this arm matched `Serving`")
                    };
                    state = State::Failed { replica, failure };
                    continue;
                }
                for envelope in link.drain_inbound() {
                    replica.deliver(envelope)?;
                }
                if now >= next_tick {
                    replica.tick()?;
                    next_tick = now + config.tick_interval;
                }
                // Not optional and not merely tidy: a granted barrier is
                // consumed by a read call rather than announced, so a loop that
                // skipped this would leave every linearizable query unanswered.
                replica.drive_reads()?;
                replica.poll_pending(now);

                for answer in replica.take_answers() {
                    let (ticket, response) = render_answer(answer);
                    if let Some(reply) = responders.remove(&ticket) {
                        reply.send(harness_response(config, ticket, response), false);
                    }
                }
                if !announced_ready && replica.is_ready() {
                    announced_ready = true;
                    emit(&format!(
                        "READY {} {client_addr} {}",
                        config.node_id.0,
                        replica.applied_index().0
                    ));
                }
            }
        }
    }

    // Whatever the terminal state already knows, plus whatever the shutdown
    // flush discovers. Either one means the same thing and both end the same
    // way; they differ only in when the replica stopped serving.
    let mut terminal = match &state {
        State::Failed { failure, .. } => Some(failure.clone()),
        _ => None,
    };
    let (refused_frames, non_member_frames) = match &mut state {
        State::Serving(replica) => {
            replica.abandon_waiters("the replica is shutting down");
            for answer in replica.take_answers() {
                let (ticket, response) = render_answer(answer);
                if let Some(reply) = responders.remove(&ticket) {
                    reply.send(harness_response(config, ticket, response), true);
                }
            }
            // The shutdown flush has no later entry point to raise it, so the
            // process says so on its own channel rather than stopping quietly
            // over a control plane it could not make durable — or over one whose
            // inputs it has already declared contradictory.
            if let Some(failure) = terminal_failure(replica) {
                emit(&format!(
                    "{} {} {}",
                    failure.lifecycle_tag(),
                    config.node_id.0,
                    failure.detail()
                ));
                terminal = Some(failure);
            }
            (replica.refused_frames(), replica.non_member_frames())
        }
        State::Failed { replica, .. } => (replica.refused_frames(), replica.non_member_frames()),
        State::Opening { .. } => (0, 0),
    };
    let (dropped, unencodable, refused_chunks) = link.counts();
    emit(&format!(
        "LINK {} dropped={dropped} unencodable={unencodable} \
         refused_chunks={refused_chunks} refused_frames={refused_frames} \
         non_member_frames={non_member_frames}",
        config.node_id.0
    ));
    let diagnostics = link.diagnostics();
    let (outbound_depth, inbound_depth) = link.queue_depths();
    emit_json(&format!(
        "{{\"event\":\"transport\",\"node\":{},\"authenticated_connections\":{},\
         \"authentication_failed\":{},\"unknown_certificate\":{},\"identity_mismatch\":{},\
         \"unauthorized_peer\":{},\"replay_duplicate\":{},\"replay_stale_session\":{},\
         \"replay_outside_window\":{},\"malformed_frame\":{},\"outbound_depth\":{},\
         \"inbound_depth\":{},\"inbound_peer_full\":{},\"inbound_global_full\":{},\
         \"connection_full\":{},\"replay_peer_windows\":{}}}",
        config.node_id.0,
        diagnostics.authenticated_connections,
        diagnostics.authentication_failed,
        diagnostics.unknown_certificate,
        diagnostics.identity_mismatch,
        diagnostics.unauthorized_peer,
        diagnostics.replay_duplicate,
        diagnostics.replay_stale_session,
        diagnostics.replay_outside_window,
        diagnostics.malformed_frame,
        outbound_depth,
        inbound_depth,
        diagnostics.inbound_peer_full,
        diagnostics.inbound_global_full,
        diagnostics.connection_full,
        link.replay_peer_windows()
    ));
    // Before `STOPPED`, and instead of it: `stop_outcome` returns `Err` for
    // either terminal control-plane failure, and this only announces a clean
    // stop when there was none.
    stop_outcome(terminal)?;
    emit(&format!("STOPPED {}", config.node_id.0));
    Ok(())
}

/// Adds main-loop ordering evidence only when the deterministic fault seam is
/// active.
///
/// Connections have independent reader threads, so socket write order is not
/// main-loop admission order. The fault harness needs the latter to distinguish
/// a request legitimately served before the injected failure from one served
/// after it. Ordinary clients never see this suffix because an operator-started
/// replica does not carry the seam.
fn harness_response(config: &Config, ticket: u64, response: String) -> String {
    if config.control_plane_fault_after.is_some() {
        format!("{response} HARNESS_TICKET {ticket}")
    } else {
        response
    }
}

/// The terminal control-plane failure this replica has reached, if it has.
///
/// **Persistence first, contradiction second, and the order is the rule.** A
/// persist failure is *stored* by an entry point with no error channel, so it can
/// be older than the state beside it — and the first failure is the one that
/// explains why this replica stopped. A contradiction is read live off the
/// driver, so it is always current.
///
/// A free function for the reason [`stop_outcome`] is one: the rule is worth
/// being able to see without the loop around it.
fn terminal_failure(replica: &Replica) -> Option<TerminalFailure> {
    replica
        .control_plane_failure()
        .map(|detail| TerminalFailure::Unpersisted(detail.to_owned()))
        .or_else(|| {
            replica
                .terminal_control_plane_failure()
                .map(TerminalFailure::Contradicted)
        })
}

/// Whether this state must stop admitting client work for the rest of the pass.
///
/// **Two states and one of them does not know it yet.** `State::Failed` is the
/// terminal state and is obvious. A `State::Serving` replica that has already
/// reached a terminal control-plane failure has not been moved yet, and for a
/// stored persistence failure the reason is that the entry point which discovered
/// it had no error channel to raise it on — `submit`, `query`, `poll_pending`,
/// and `abandon_waiters` all answer with a value. The transition runs once per
/// pass, below the drain, so between the failure and the transition there is a
/// whole job budget in which this replica would otherwise keep serving.
///
/// It serves nothing in that window because there is nothing it can honestly
/// do. A replica whose control plane it cannot persist begins its next restart by
/// forgetting whatever it retired, so a write it accepts is a write whose
/// retirement record may not survive. A replica whose control-plane inputs
/// contradict each other has no trustworthy statement about who is retired at
/// all, and its driver has already stopped publishing one. The client work either
/// refuses will be retried against a replica that can still answer.
///
/// A free function taking the state for the reason [`stop_outcome`] is one: the
/// rule is the interesting part and it is worth being able to see it without
/// reading the loop around it.
fn pass_is_terminal(state: &State) -> bool {
    match state {
        State::Failed { .. } => true,
        State::Serving(replica) => terminal_failure(replica).is_some(),
        State::Opening { .. } => false,
    }
}

/// What the process reports once the loop has ended.
///
/// **`STOPPED` and a nonzero exit are the two mutually exclusive endings, and
/// which one applies turns on a single fact: whether this replica reached a
/// terminal control-plane failure.** A replica that could not make its peer
/// control plane durable did not stop cleanly: the record it failed to write is
/// the one no other artifact holds, so the next start of this replica begins by
/// forgetting whatever it retired — an identity a committed removal spent becomes
/// allocatable again. A replica whose control-plane inputs contradict each other
/// did not stop cleanly either: its driver has frozen its record and stopped
/// publishing, and only an operator's deliberate reseed clears that. A supervisor
/// reading exit 0 restarts either one straight back into the same state.
///
/// A free function for the reason `replica::combine_outcomes` is one: the rule
/// is checkable without three processes and a filesystem that can be made to
/// fail at the one moment that matters.
fn stop_outcome(terminal: Option<TerminalFailure>) -> Result<(), String> {
    match terminal {
        Some(failure) => Err(failure.message()),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::{stop_outcome, TerminalFailure};

    #[test]
    fn a_clean_stop_is_ok() {
        assert_eq!(stop_outcome(None), Ok(()));
    }

    /// An unpersisted control plane ends the process with a reason, which `main`
    /// turns into `FATAL` and a nonzero exit.
    ///
    /// The half that used to be missing: the failure was announced on
    /// `CONTROL_PLANE_UNPERSISTED` and then the process printed `STOPPED` and
    /// returned success, so every supervisor that watched the exit code — which
    /// is every supervisor — saw a clean shutdown.
    #[test]
    fn an_unpersisted_control_plane_is_a_failure_with_its_reason() {
        let outcome = stop_outcome(Some(TerminalFailure::Unpersisted(String::from(
            "no space left on device",
        ))));
        let detail = outcome.expect_err("an unpersisted control plane is not a clean stop");
        assert!(
            detail.contains("could not be made durable")
                && detail.contains("no space left on device"),
            "the reason reaches the operator: {detail}"
        );
    }

    /// A contradicted control plane ends the process too, and says which failure
    /// it was.
    ///
    /// The same defect arriving through the other door. `STATUS` rendered the
    /// application-floor readiness bit without consulting the driver at all, and
    /// the loop moved to `Failed` only for persistence failures — so a replica
    /// whose driver had declared its own inputs untrustworthy went on answering
    /// `ready`, went on accepting client work, and exited 0.
    #[test]
    fn a_contradicted_control_plane_is_a_failure_with_its_reason() {
        let outcome = stop_outcome(Some(TerminalFailure::Contradicted(String::from(
            "two observations of the committed membership at index 11 disagree",
        ))));
        let detail = outcome.expect_err("a contradicted control plane is not a clean stop");
        assert!(
            detail.contains("contradicted") && detail.contains("index 11"),
            "the reason reaches the operator: {detail}"
        );
    }

    /// The two endings are told apart on their own lifecycle lines.
    #[test]
    fn each_terminal_failure_announces_itself_on_its_own_line() {
        assert_eq!(
            TerminalFailure::Unpersisted(String::new()).lifecycle_tag(),
            "CONTROL_PLANE_UNPERSISTED"
        );
        assert_eq!(
            TerminalFailure::Contradicted(String::new()).lifecycle_tag(),
            "CONTROL_PLANE_CONTRADICTED"
        );
    }
}

/// What handling one client request decided.
#[derive(Debug)]
enum Disposition {
    /// The answer is already known.
    Answered(String),
    /// The replica owes the answer under this request's ticket.
    Pending,
    /// The answer is known and the process should stop afterwards.
    Stop(String),
}

fn handle_job(
    state: &mut State,
    config: &Config,
    link: &PeerLink,
    client_counters: &ClientCounters,
    job: &Job,
    ticket: u64,
) -> Disposition {
    let deadline = Instant::now() + config.request_timeout;
    match (&job.request, state) {
        (Request::Shutdown, _) => Disposition::Stop(String::from("BYE")),
        (Request::Status, State::Opening { .. }) => Disposition::Answered(render_status(
            Readiness::Recovering,
            rafter::Role::Follower,
            0,
            0,
            0,
            None,
        )),
        (Request::Status, State::Serving(replica)) => {
            let (role, term, leader) = replica.status();
            Disposition::Answered(render_status(
                Readiness::of_serving(replica.is_ready()),
                role,
                term,
                replica.applied_index().0,
                replica.committed_application_index().0,
                leader,
            ))
        }
        // **Not `replica.is_ready()`, and the state is what decides.** That
        // answer folds the driver's own service state in now, so it would be
        // right here too — but a `Failed` replica is one this process has already
        // decided will not serve again, which is a stronger statement than any
        // driver state and is the one a supervisor needs.
        (Request::Status, State::Failed { replica, .. }) => {
            let (role, term, leader) = replica.status();
            Disposition::Answered(render_status(
                Readiness::Abandoned,
                role,
                term,
                replica.applied_index().0,
                replica.committed_application_index().0,
                leader,
            ))
        }
        (Request::Observe, State::Opening { .. }) => Disposition::Answered(format!(
            "{{\"ready\":false,\"readiness_reason\":\"recovering\",\
             \"node\":{},\"client_pending\":{},\"client_active\":{}}}",
            config.node_id.0,
            client_counters.pending.load(Ordering::Relaxed),
            client_counters.active_connections.load(Ordering::Relaxed)
        )),
        (Request::Observe, State::Serving(replica)) => Disposition::Answered(render_observation(
            replica,
            config,
            link,
            client_counters,
            if replica.is_ready() {
                "ready"
            } else {
                "recovering"
            },
        )),
        (Request::Observe, State::Failed { replica, failure }) => Disposition::Answered(
            render_observation(replica, config, link, client_counters, failure.detail()),
        ),
        // Everything below is service, and service is what readiness gates.
        (_, State::Opening { .. }) => Disposition::Answered(String::from("NOTREADY 0 0")),
        // A terminal refusal rather than `NOTREADY`, because `NOTREADY` invites
        // a retry against this replica and nothing here will become ready.
        (_, State::Failed { failure, .. }) => {
            Disposition::Answered(format!("ABANDONED {}", failure.detail()))
        }
        (_, State::Serving(replica)) if !replica.is_ready() => Disposition::Answered(format!(
            "NOTREADY {} {}",
            replica.applied_index().0,
            replica.committed_application_index().0
        )),
        (Request::Submit(command), State::Serving(replica)) => {
            replica.submit(ticket, *command, deadline);
            Disposition::Pending
        }
        (Request::Query(resource), State::Serving(replica)) => {
            replica.query(ticket, *resource, deadline);
            Disposition::Pending
        }
        (Request::Local(resource), State::Serving(replica)) => {
            Disposition::Answered(match replica.local_lock(*resource) {
                Ok(status) => render_lock(status),
                Err(detail) => format!("ABANDONED {detail}"),
            })
        }
        (Request::Membership(change), State::Serving(replica)) => Disposition::Answered(
            match validate_membership_change(config, replica, change)
                .and_then(|()| replica.change_membership(change.clone()))
            {
                Ok(()) => String::from("OK MEMBERSHIP_ACCEPTED"),
                Err(detail) => format!("ERR MEMBERSHIP {detail}"),
            },
        ),
    }
}

fn validate_membership_change(
    config: &Config,
    replica: &Replica,
    change: &MembershipChange,
) -> Result<(), String> {
    let node_id = match change {
        MembershipChange::AddLearner { node_id, .. }
        | MembershipChange::PromoteLearner { node_id, .. } => Some(*node_id),
        _ => None,
    };
    if let Some(node_id) = node_id {
        if !config.tls.peer_certificates.contains_key(&node_id) {
            return Err(format!(
                "node {} has no configured authenticated principal",
                node_id.0
            ));
        }
        let identity_path = ReplicaIdentity::path(&config.cluster_dir, node_id);
        load_active_replica(&config.cluster_dir, &identity_path, replica::GROUP_ID.0)
            .map_err(|error| format!("node {} identity is not active: {error}", node_id.0))?;
    }
    if let MembershipChange::AddLearner { node_id, .. } = change {
        let (checkpoint, _) = replica.control_plane_snapshot();
        let high_water = checkpoint
            .committed_id_high_water
            .ok_or_else(|| String::from("no committed identity allocation floor is known"))?;
        if *node_id <= high_water {
            return Err(format!(
                "node {} is not above committed identity high-water {}",
                node_id.0, high_water.0
            ));
        }
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "the single structured observation record keeps its field-to-value mapping auditable"
)]
fn render_observation(
    replica: &Replica,
    config: &Config,
    link: &PeerLink,
    client: &ClientCounters,
    readiness_reason: &str,
) -> String {
    let metrics = replica.metrics_snapshot();
    let (checkpoint, checkpoint_epoch) = replica.control_plane_snapshot();
    let diagnostics = link.diagnostics();
    let (outbound_depth, inbound_depth) = link.queue_depths();
    let membership = metrics
        .membership
        .replica_ids()
        .iter()
        .map(|node| node.0.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let voters = metrics
        .membership
        .voter_ids()
        .iter()
        .map(|node| node.0.to_string())
        .collect::<Vec<_>>()
        .join(",");
    let membership_phase = match &metrics.membership {
        rafter::MembershipConfig::Stable(_) => "stable",
        rafter::MembershipConfig::Joint(_) => "joint",
    };
    let committed_membership_phase = match replica.committed_membership_snapshot() {
        Ok(rafter::MembershipConfig::Stable(_)) => "stable",
        Ok(rafter::MembershipConfig::Joint(_)) => "joint",
        Err(_) => "unavailable",
    };
    let replication = metrics
        .replication
        .iter()
        .map(|progress| format!("{}:{}", progress.follower_id.0, progress.match_index.0))
        .collect::<Vec<_>>()
        .join(",");
    let committed_members = checkpoint
        .current_committed
        .as_ref()
        .map(|current| {
            current
                .membership
                .iter()
                .map(|node| node.0.to_string())
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();
    let role = match metrics.role {
        rafter::Role::Leader => "leader",
        rafter::Role::Candidate => "candidate",
        rafter::Role::PreCandidate => "precandidate",
        rafter::Role::Follower => "follower",
    };
    format!(
        "{{\"ready\":{},\"readiness_reason\":\"{}\",\"group\":{},\"node\":{},\
         \"role\":\"{role}\",\"term\":{},\"leader\":{},\"commit_index\":{},\
         \"applied_index\":{},\"snapshot_index\":{},\"last_log_index\":{},\
         \"membership\":\"{}\",\"voters\":\"{}\",\"membership_phase\":\"{}\",\
         \"committed_membership_phase\":\"{}\",\"replication\":\"{}\",\
         \"pending_proposals\":{},\"pending_reads\":{},\"control_plane_epoch\":{},\
         \"committed_id_high_water\":{},\"committed_members\":\"{}\",\
         \"client_active\":{},\"client_pending\":{},\"client_connection_full\":{},\
         \"client_pending_full\":{},\"client_line_too_long\":{},\
         \"outbound_depth\":{},\"inbound_depth\":{},\"outbound_full\":{},\
         \"inbound_peer_full\":{},\"inbound_global_full\":{},\
         \"authenticated_connections\":{},\"authentication_failed\":{},\
         \"unknown_certificate\":{},\"identity_mismatch\":{},\
         \"unauthorized_peer\":{},\"replay_duplicate\":{},\
         \"replay_stale_session\":{},\"replay_outside_window\":{},\
         \"replay_peer_windows\":{},\"frame_limit\":{},\"replay_window\":{},\
         \"outbound_limit\":{},\"inbound_peer_limit\":{},\"inbound_global_limit\":{},\
         \"peer_connection_limit\":{},\"client_connection_limit\":{},\
         \"client_pending_limit\":{},\"identity_path\":\"{}\"}}",
        replica.is_ready(),
        json_escape(readiness_reason),
        replica::GROUP_ID.0,
        config.identity.node_id.0,
        metrics.term.0,
        metrics
            .leader_hint
            .map_or_else(|| "null".to_owned(), |leader| leader.0.to_string()),
        metrics.commit_index.0,
        metrics.applied_index.0,
        metrics.snapshot_index.0,
        metrics.last_log_index.0,
        membership,
        voters,
        membership_phase,
        committed_membership_phase,
        replication,
        metrics.pending_proposals,
        metrics.pending_query_reads + metrics.pending_read_barriers,
        checkpoint_epoch,
        checkpoint
            .committed_id_high_water
            .map_or_else(|| "null".to_owned(), |node| node.0.to_string()),
        committed_members,
        client.active_connections.load(Ordering::Relaxed),
        client.pending.load(Ordering::Relaxed),
        client.connection_full.load(Ordering::Relaxed),
        client.pending_full.load(Ordering::Relaxed),
        client.line_too_long.load(Ordering::Relaxed),
        outbound_depth,
        inbound_depth,
        link.counts().0,
        diagnostics.inbound_peer_full,
        diagnostics.inbound_global_full,
        diagnostics.authenticated_connections,
        diagnostics.authentication_failed,
        diagnostics.unknown_certificate,
        diagnostics.identity_mismatch,
        diagnostics.unauthorized_peer,
        diagnostics.replay_duplicate,
        diagnostics.replay_stale_session,
        diagnostics.replay_outside_window,
        link.replay_peer_windows(),
        max_frame_bytes(),
        rafter_reference_fenced_lock::production::REPLAY_WINDOW,
        PEER_SEND_QUEUE_LEN,
        PEER_INBOUND_QUEUE_LEN,
        GLOBAL_INBOUND_QUEUE_LEN,
        MAX_PEER_CONNECTIONS,
        MAX_CLIENT_CONNECTIONS,
        CLIENT_PENDING_LIMIT,
        json_escape(&config.identity_path.display().to_string())
    )
}

fn render_answer(answer: Answer) -> (u64, String) {
    match answer {
        Answer::Submit { ticket, outcome } => {
            let response = match outcome {
                SubmitOutcome::Completed { outcome, .. } => {
                    render_applied(outcome.disposition, outcome.response)
                }
                SubmitOutcome::Refused { error } => render_not_committed(&error),
                SubmitOutcome::Unknown { error } => format!("UNKNOWN {error}"),
            };
            (ticket, response)
        }
        Answer::Query { ticket, outcome } => {
            let response = match outcome {
                QueryOutcome::Answered { status, .. } => render_lock(status),
                QueryOutcome::Unavailable { error } => format!("ABANDONED {error}"),
            };
            (ticket, response)
        }
        Answer::Unknown { ticket, detail } => (ticket, format!("UNKNOWN {detail}")),
        Answer::Abandoned { ticket, detail } => (ticket, format!("ABANDONED {detail}")),
    }
}

fn spawn_client_acceptor(
    listener: TcpListener,
    jobs: SyncSender<Job>,
    counters: Arc<ClientCounters>,
) {
    thread::spawn(move || {
        while let Ok((stream, _)) = listener.accept() {
            let active = counters.active_connections.fetch_add(1, Ordering::Relaxed);
            if active >= MAX_CLIENT_CONNECTIONS {
                counters.active_connections.fetch_sub(1, Ordering::Relaxed);
                counters.connection_full.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            let jobs = jobs.clone();
            let thread_counters = Arc::clone(&counters);
            if thread::Builder::new()
                .name(String::from("production-client"))
                .spawn(move || {
                    let _guard = ClientConnectionGuard(Arc::clone(&thread_counters));
                    serve_client(stream, &jobs, &thread_counters);
                })
                .is_err()
            {
                counters.active_connections.fetch_sub(1, Ordering::Relaxed);
                return;
            }
        }
    });
}

struct ClientConnectionGuard(Arc<ClientCounters>);

impl Drop for ClientConnectionGuard {
    fn drop(&mut self) {
        self.0.active_connections.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Serves one client connection, one request at a time.
///
/// A connection is strictly sequential: the reply to a request is written
/// before the next line is read. That keeps the protocol trivially correlatable
/// without request identifiers, and a client that wants overlapping operations
/// opens a second connection — which is also what makes concurrent operations
/// in a recorded history genuinely concurrent rather than interleaved by one
/// socket.
fn serve_client(stream: TcpStream, jobs: &SyncSender<Job>, counters: &ClientCounters) {
    let Ok(read_half) = stream.try_clone() else {
        return;
    };
    let mut write_half = stream;
    let mut reader = BufReader::new(read_half);
    loop {
        let mut line = String::new();
        let read = reader
            .by_ref()
            .take((MAX_CLIENT_LINE_BYTES + 1) as u64)
            .read_line(&mut line);
        let Ok(read) = read else { return };
        if read == 0 {
            return;
        }
        if line.len() > MAX_CLIENT_LINE_BYTES {
            counters.line_too_long.fetch_add(1, Ordering::Relaxed);
            let _ = writeln!(write_half, "ERR LINE_TOO_LONG {MAX_CLIENT_LINE_BYTES}");
            let _ = write_half.flush();
            return;
        }
        if line.trim().is_empty() {
            continue;
        }
        let (response, flushed) = match parse_request(&line) {
            Ok(request) => {
                let (reply_tx, reply_rx) = mpsc::channel();
                let (flushed_tx, flushed_rx) = mpsc::channel();
                counters.pending.fetch_add(1, Ordering::Relaxed);
                let admitted = match jobs.try_send(Job {
                    request,
                    reply: ClientReply {
                        response: reply_tx,
                        flushed: flushed_rx,
                    },
                }) {
                    Ok(()) => true,
                    Err(TrySendError::Full(_) | TrySendError::Disconnected(_)) => {
                        counters.pending.fetch_sub(1, Ordering::Relaxed);
                        counters.pending_full.fetch_add(1, Ordering::Relaxed);
                        false
                    }
                };
                if admitted {
                    match reply_rx.recv() {
                        Ok(response) => (response, Some(flushed_tx)),
                        // The loop that owed this answer is gone. Saying nothing is
                        // correct: the client observes a dropped connection, which
                        // is the unknown outcome it already has to handle.
                        Err(_) => return,
                    }
                } else {
                    (String::from("BUSY CLIENT_QUEUE_FULL"), None)
                }
            }
            Err(error) => (format!("ERR {error}"), None),
        };
        let written = writeln!(write_half, "{response}").and_then(|()| write_half.flush());
        if let Some(flushed) = flushed {
            let _ = flushed.send(());
        }
        if written.is_err() {
            return;
        }
    }
}

/// Writes one lifecycle line and flushes it.
///
/// Flushing every line is load-bearing rather than defensive: stdout is block
/// buffered when it is a pipe, and a supervisor waiting for `READY` on a pipe
/// would otherwise wait for a buffer to fill.
pub(crate) fn emit(line: &str) {
    let (kind, detail) = line.split_once(' ').unwrap_or((line, ""));
    emit_json(&format!(
        "{{\"event\":\"lifecycle\",\"kind\":\"{}\",\"detail\":\"{}\"}}",
        json_escape(kind),
        json_escape(detail)
    ));
}

fn emit_json(line: &str) {
    let mut stdout = std::io::stdout().lock();
    drop(writeln!(stdout, "{line}"));
    drop(stdout.flush());
}

fn json_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        match character {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                use std::fmt::Write as _;
                write!(escaped, "\\u{:04x}", character as u32)
                    .expect("writing to a string cannot fail");
            }
            character => escaped.push(character),
        }
    }
    escaped
}
