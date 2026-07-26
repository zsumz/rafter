//! One OS process, one fenced-lock replica.
//!
//! This is the durable process composition the lock contract has been deferring
//! to: file-backed Raft stores, the durable two-slot lock store, a TCP link to
//! the other replicas, and a small consumer-owned loop driving the
//! `rafter-service` managed driver. Nothing here is a Rafter API. The loop, the
//! link, the address discovery, the client protocol, the readiness output, and
//! the shutdown discipline are all consumer-owned, which is the point: an
//! external user with the published crates can write this file.
//!
//! It is **integration composition**, not production composition. The link
//! authenticates nothing and the client protocol authenticates nothing;
//! `CONTRACT.md` says exactly what that does and does not establish.
//!
//! # Configuration
//!
//! Every option is an argument, and every argument has an environment fallback
//! so a supervisor can set them either way. Arguments win.
//!
//! ```text
//! --id <u64>                      RAFTER_LOCK_NODE_ID
//! --members <id,id,...>           RAFTER_LOCK_MEMBERS
//! --cluster-dir <path>            RAFTER_LOCK_CLUSTER_DIR
//! --client-listen <addr>          RAFTER_LOCK_CLIENT_LISTEN     [127.0.0.1:0]
//! --peer-listen <addr>            RAFTER_LOCK_PEER_LISTEN       [127.0.0.1:0]
//! --election-timeout-ticks <u64>  RAFTER_LOCK_ELECTION_TICKS    [8]
//! --tick-interval-ms <u64>        RAFTER_LOCK_TICK_MS           [20]
//! --ownership-wait-ms <u64>       RAFTER_LOCK_OWNERSHIP_WAIT_MS [10000]
//! --request-timeout-ms <u64>      RAFTER_LOCK_REQUEST_MS        [5000]
//! --max-clients <u32>             RAFTER_LOCK_MAX_CLIENTS       [8]
//! --max-resources <u32>           RAFTER_LOCK_MAX_RESOURCES     [8]
//! --recover <open|repair|reseed>  RAFTER_LOCK_RECOVER           [open]
//! ```
//!
//! `--recover` is the only way this process loses acknowledged work, and both
//! settings above `open` are deliberate operator acts that a restart never
//! reaches by itself.
//!
//! The reason it exists is that an ordinary crash of this store is not always
//! recoverable by reopening. A publication writes a whole image and then seals
//! it, and a kill between those two writes leaves a complete image whose seal
//! never landed. That is indistinguishable from a live slot whose seal byte
//! rotted, so `LockStore::open` refuses rather than guessing: refusing is
//! recoverable under both readings and adopting the partner is recoverable
//! under only one. `repair` gives up the unreadable slot, and is itself refused
//! when the slot it would give up carries a fencing high-water mark the adopted
//! slot cannot dominate — that refusal is the whole point of the design, since
//! adopting a lower mark would let a guarded resource accept two tenures under
//! one token. `reseed` deletes both slots.
//!
//! None of that is unrecoverable for a *replica*. The durable lock image is a
//! projection of the replicated log, its applied index is the join point, and a
//! reseeded replica re-applies everything above index zero from the log. What
//! is unrecoverable is the case where the group cannot supply those entries,
//! which is a fact about the cluster and not about this process.
//!
//! A replica's durable state lives under `<cluster-dir>/node-<id>/`, split into
//! `raft/` for Rafter's stores and `app/` for the two lock slots.
//!
//! # Stdout
//!
//! Lifecycle is announced on stdout, one line each, flushed as it is written so
//! a supervisor reading a pipe sees it immediately:
//!
//! ```text
//! LISTENING <id> <client_addr>     the client port is open; service is refused
//! WAITING_FOR_OWNERSHIP <id>       another process still owns this directory
//! CREATED <id> <app_dir>           there were no slot files here, so two were made
//! RECOVERED <id> live_slot=.. cross_checked_marks=.. damage=..
//! REPAIRED <id> discarded_slot=.. adopted_slot=.. ...
//! RESEEDED <id> discarded_bytes=.. discarded_applied_index=..
//! NEEDS_DECISION <id> <detail>     the store will not open under this mode
//! PEER_LISTENING <id> <peer_addr>  the peer port is published and dialable
//! READY <id> <client_addr> <applied_index>
//! LINK <id> dropped=<n> unencodable=<n> refused_chunks=<n> refused_frames=<n>
//! STOPPED <id>
//! FATAL <detail>                   followed by a nonzero exit
//! ```
//!
//! `CREATED`, `RECOVERED`, `REPAIRED`, `RESEEDED`, and `NEEDS_DECISION` are the
//! durable store's recovery report reaching somebody who can act on it. A
//! report nothing reads is a report that costs nothing to be wrong.
//!
//! `CREATED` is the one that needs reading with the supervisor's own knowledge
//! beside it. On a replica's first boot it is expected; on a restart it means
//! the slot files that were there are gone, and the replica is about to serve an
//! empty lock table from applied index zero with no fencing high-water marks at
//! all. The store cannot tell those apart — both are two absent files — so it
//! reports the fact and leaves the judgement to whoever knows whether this
//! replica has run before.
//!
//! `LISTENING` deliberately precedes recovery. A client may connect at once and
//! will be refused with `NOTREADY` until `READY`, which is what makes the
//! readiness gate an observable behavior rather than a claim.
//!
//! `LINK` is emitted once during shutdown and is diagnostic rather than
//! protocol. A nonzero `dropped` is normal under load and means Raft
//! retransmitted; a nonzero `unencodable` means the kernel produced a message
//! the peer wire format does not carry; `refused_chunks` counts leader snapshot
//! directives this link declines because the durable runtime already resolved
//! them; `refused_frames` counts inbound frames this replica's own validator
//! turned away.
//!
//! # Shutdown
//!
//! `SHUTDOWN` on the client protocol is the clean path: waiters are released
//! with unknown outcomes, the durable stores are dropped, and the process exits
//! `0`. There is no signal handler, and that is a deliberate consequence of the
//! reference workspace's zero-external-dependency rule — installing one from
//! `std` alone is not possible. `SIGTERM` and `SIGKILL` therefore both
//! terminate the process abruptly, which is the crash the store's contract
//! covers: the slot files are recoverable to the pre- or post-publication
//! state, or to the refusal described above, and to nothing else. The process
//! suite uses `SIGKILL` for that reason — it is the harsher of the two, and no
//! cleanup path can flatter it.
//!
//! A killed process is a weaker fault than the store's contract admits, and
//! saying otherwise would be the easy error. The kernel still holds the page
//! cache, so a `SIGKILL` here loses nothing that reached a `write`, while the
//! contract also has to survive a power cut that loses a sector or reorders a
//! writeback. The store's own crash suites inject at byte boundaries to cover
//! what a signal cannot.

mod peer_link;
mod protocol;
mod replica;

use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Write},
    net::{TcpListener, TcpStream},
    path::PathBuf,
    process::ExitCode,
    sync::mpsc::{self, Receiver, Sender},
    thread,
    time::{Duration, Instant},
};

use rafter::NodeId;
use rafter_reference_fenced_lock::{LockConfig, QueryOutcome, SubmitOutcome};

use peer_link::PeerLink;
use protocol::{
    parse_request, render_applied, render_lock, render_not_committed, render_status, Request,
};
use replica::{Answer, OpenError, OpenRequest, RecoveryMode, Replica};

/// How long the loop blocks waiting for a client request before it polls the
/// peer link and the clock again.
///
/// This is the loop's idle wakeup rate, and it bounds how long an arrived peer
/// frame waits to be delivered. It is deliberately well under the tick interval
/// — a poll slower than a tick would add latency to every round — and
/// deliberately not smaller than it needs to be, because a cluster of these
/// processes pays this rate per replica.
const LOOP_POLL_INTERVAL: Duration = Duration::from_millis(2);

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
    client_listen: String,
    peer_listen: String,
    election_timeout_ticks: u64,
    tick_interval: Duration,
    ownership_wait: Duration,
    request_timeout: Duration,
    lock: LockConfig,
    recover: RecoveryMode,
}

impl Config {
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
        if !members.contains(&node_id) {
            return Err(String::from("--members must contain this node's own id"));
        }

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
            cluster_dir: PathBuf::from(required("cluster-dir", "RAFTER_LOCK_CLUSTER_DIR")?),
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
        })
    }

    fn node_dir(&self) -> PathBuf {
        self.cluster_dir.join(format!("node-{}", self.node_id.0))
    }
}

/// One client request waiting for the loop that owns the replica.
#[derive(Debug)]
struct Job {
    request: Request,
    reply: Sender<String>,
}

/// Whether the replica exists yet.
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
}

fn run(config: &Config) -> Result<(), String> {
    let node_dir = config.node_dir();
    std::fs::create_dir_all(&node_dir)
        .map_err(|error| format!("could not create {}: {error}", node_dir.display()))?;

    let (jobs_tx, jobs_rx) = mpsc::channel();
    let client_listener = TcpListener::bind(&config.client_listen)
        .map_err(|error| format!("could not bind the client port: {error}"))?;
    let client_addr = client_listener
        .local_addr()
        .map_err(|error| format!("could not read the client address: {error}"))?;
    spawn_client_acceptor(client_listener, jobs_tx);
    emit(&format!("LISTENING {} {client_addr}", config.node_id.0));

    let link = PeerLink::bind(
        &config.peer_listen,
        config.node_id,
        &config.members,
        &config.cluster_dir,
    )
    .map_err(|error| format!("could not bind the peer port: {error}"))?;

    let outcome = serve(config, &node_dir, &link, &jobs_rx, client_addr);
    link.shut_down();
    outcome
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
) -> Result<(), String> {
    let mut state = State::Opening {
        deadline: Instant::now() + config.ownership_wait,
        next_attempt: Instant::now(),
        announced: false,
    };
    let mut responders: BTreeMap<u64, Sender<String>> = BTreeMap::new();
    let mut next_ticket = 1_u64;
    let mut next_tick = Instant::now() + config.tick_interval;
    let mut announced_ready = false;
    let mut stopping = false;

    loop {
        // Client requests first, so that a `SHUTDOWN` or a `STATUS` is answered
        // even when the replica does not exist yet.
        while let Ok(job) = jobs.recv_timeout(LOOP_POLL_INTERVAL) {
            let ticket = next_ticket;
            next_ticket += 1;
            match handle_job(&mut state, config, &job, ticket) {
                Disposition::Answered(response) => drop(job.reply.send(response)),
                Disposition::Pending => {
                    responders.insert(ticket, job.reply);
                }
                Disposition::Stop(response) => {
                    drop(job.reply.send(response));
                    stopping = true;
                    break;
                }
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
                }) {
                    Ok(replica) => {
                        // The address is published only now, so a peer never
                        // dials a process that does not own its directory.
                        link.publish_address(node_dir).map_err(|error| {
                            format!("could not publish the peer address: {error}")
                        })?;
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
            State::Serving(replica) => {
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
                        drop(reply.send(response));
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

    let refused_frames = if let State::Serving(replica) = &mut state {
        replica.abandon_waiters("the replica is shutting down");
        for answer in replica.take_answers() {
            let (ticket, response) = render_answer(answer);
            if let Some(reply) = responders.remove(&ticket) {
                drop(reply.send(response));
            }
        }
        replica.refused_frames()
    } else {
        0
    };
    let (dropped, unencodable, refused_chunks) = link.counts();
    emit(&format!(
        "LINK {} dropped={dropped} unencodable={unencodable} \
         refused_chunks={refused_chunks} refused_frames={refused_frames}",
        config.node_id.0
    ));
    emit(&format!("STOPPED {}", config.node_id.0));
    Ok(())
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

fn handle_job(state: &mut State, config: &Config, job: &Job, ticket: u64) -> Disposition {
    let deadline = Instant::now() + config.request_timeout;
    match (&job.request, state) {
        (Request::Shutdown, _) => Disposition::Stop(String::from("BYE")),
        (Request::Status, State::Opening { .. }) => {
            Disposition::Answered(render_status(false, rafter::Role::Follower, 0, 0, 0, None))
        }
        (Request::Status, State::Serving(replica)) => {
            let (role, term, leader) = replica.status();
            Disposition::Answered(render_status(
                replica.is_ready(),
                role,
                term,
                replica.applied_index().0,
                replica.committed_application_index().0,
                leader,
            ))
        }
        // Everything below is service, and service is what readiness gates.
        (_, State::Opening { .. }) => Disposition::Answered(String::from("NOTREADY 0 0")),
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
    }
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

fn spawn_client_acceptor(listener: TcpListener, jobs: Sender<Job>) {
    thread::spawn(move || {
        while let Ok((stream, _)) = listener.accept() {
            let jobs = jobs.clone();
            if thread::Builder::new()
                .name(String::from("client"))
                .spawn(move || serve_client(stream, &jobs))
                .is_err()
            {
                return;
            }
        }
    });
}

/// Serves one client connection, one request at a time.
///
/// A connection is strictly sequential: the reply to a request is written
/// before the next line is read. That keeps the protocol trivially correlatable
/// without request identifiers, and a client that wants overlapping operations
/// opens a second connection — which is also what makes concurrent operations
/// in a recorded history genuinely concurrent rather than interleaved by one
/// socket.
fn serve_client(stream: TcpStream, jobs: &Sender<Job>) {
    let Ok(read_half) = stream.try_clone() else {
        return;
    };
    let mut write_half = stream;
    for line in BufReader::new(read_half).lines() {
        let Ok(line) = line else { return };
        if line.trim().is_empty() {
            continue;
        }
        let response = match parse_request(&line) {
            Ok(request) => {
                let (reply_tx, reply_rx) = mpsc::channel();
                if jobs
                    .send(Job {
                        request,
                        reply: reply_tx,
                    })
                    .is_err()
                {
                    return;
                }
                match reply_rx.recv() {
                    Ok(response) => response,
                    // The loop that owed this answer is gone. Saying nothing is
                    // correct: the client observes a dropped connection, which
                    // is the unknown outcome it already has to handle.
                    Err(_) => return,
                }
            }
            Err(error) => format!("ERR {error}"),
        };
        if writeln!(write_half, "{response}").is_err() || write_half.flush().is_err() {
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
    let mut stdout = std::io::stdout().lock();
    drop(writeln!(stdout, "{line}"));
    drop(stdout.flush());
}
