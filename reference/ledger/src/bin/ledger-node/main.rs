//! One OS process, one ledger replica.
//!
//! This is the durable process composition the ledger contract has been
//! deferring to: file-backed Raft stores, the durable transactional application
//! store, a TCP link to the other replicas, and a small consumer-owned loop
//! driving the `rafter-app` group. Nothing here is a Rafter API. The loop, the
//! transport, the wire encoding, the client protocol, the readiness output, and
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
//! --id <u64>                      RAFTER_LEDGER_NODE_ID
//! --members <id,id,...>           RAFTER_LEDGER_MEMBERS
//! --cluster-dir <path>            RAFTER_LEDGER_CLUSTER_DIR
//! --client-listen <addr>          RAFTER_LEDGER_CLIENT_LISTEN     [127.0.0.1:0]
//! --peer-listen <addr>            RAFTER_LEDGER_PEER_LISTEN       [127.0.0.1:0]
//! --election-timeout-ticks <u64>  RAFTER_LEDGER_ELECTION_TICKS    [8]
//! --tick-interval-ms <u64>        RAFTER_LEDGER_TICK_MS           [20]
//! --ownership-wait-ms <u64>       RAFTER_LEDGER_OWNERSHIP_WAIT_MS [10000]
//! --request-timeout-ms <u64>      RAFTER_LEDGER_REQUEST_MS        [5000]
//! --max-clients <u32>             RAFTER_LEDGER_MAX_CLIENTS       [8]
//! --max-accounts <usize>          RAFTER_LEDGER_MAX_ACCOUNTS      [16]
//! ```
//!
//! A replica's durable state lives under `<cluster-dir>/node-<id>/`, split into
//! `raft/` for Rafter's stores and `app/` for the ledger journal.
//!
//! # Stdout
//!
//! Lifecycle is announced on stdout, one line each, flushed as it is written so
//! a supervisor reading a pipe sees it immediately:
//!
//! ```text
//! LISTENING <id> <client_addr>     the client port is open; service is refused
//! WAITING_FOR_OWNERSHIP <id>       another process still owns this directory
//! PEER_LISTENING <id> <peer_addr>  the peer port is published and dialable
//! READY <id> <client_addr> <applied_index>
//! LINK <id> dropped=<n> encode_failures=<n>
//! STOPPED <id>
//! FATAL <detail>                   followed by a nonzero exit
//! ```
//!
//! `LINK` is emitted once during shutdown and is diagnostic rather than
//! protocol: a nonzero drop count is normal under load, while a nonzero encode
//! failure means the kernel produced a frame the peer format does not carry.
//!
//! `LISTENING` deliberately precedes recovery. A client may connect at once and
//! will be refused with `NOTREADY` until `READY`, which is what makes the
//! readiness gate an observable behavior rather than a claim.
//!
//! # Shutdown
//!
//! `SHUTDOWN` on the client protocol is the clean path: waiters are released
//! with unknown outcomes, the durable stores are dropped, and the process
//! exits `0`. There is no signal handler, and that is a deliberate consequence
//! of the reference workspace's zero-external-dependency rule — installing one
//! from `std` alone is not possible. `SIGTERM` and `SIGKILL` therefore both
//! terminate the process abruptly, which is exactly the crash the store's
//! contract already covers: the journal is recoverable to the pre- or
//! post-transaction state and to nothing in between. The process suite uses
//! `SIGKILL` for that reason — it is the harsher of the two, and no cleanup
//! path can flatter it.

mod peer_codec;
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
use rafter_reference_ledger::LedgerConfig;

use peer_link::PeerLink;
use protocol::{
    parse_request, render_applied, render_not_committed, render_query_result, render_status,
    Request,
};
use replica::{Answer, OpenError, QueryOutcome, Replica, SubmitOutcome};

/// How long the loop blocks waiting for a client request before it polls the
/// peer link and the clock again.
///
/// This is the loop's idle wakeup rate, and it bounds how long an arrived peer
/// message waits to be stepped. It is deliberately well under the tick interval
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
    peers: Vec<NodeId>,
    cluster_dir: PathBuf,
    client_listen: String,
    peer_listen: String,
    election_timeout_ticks: u64,
    tick_interval: Duration,
    ownership_wait: Duration,
    request_timeout: Duration,
    ledger: LedgerConfig,
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
            required("id", "RAFTER_LEDGER_NODE_ID")?
                .parse()
                .map_err(|_| String::from("--id must be an integer"))?,
        );
        let members = required("members", "RAFTER_LEDGER_MEMBERS")?
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
        let peers = members
            .into_iter()
            .filter(|member| *member != node_id)
            .collect();

        let max_clients = u32::try_from(parsed("max-clients", "RAFTER_LEDGER_MAX_CLIENTS", 8)?)
            .map_err(|_| String::from("--max-clients must fit in a u32"))?;
        let max_accounts =
            usize::try_from(parsed("max-accounts", "RAFTER_LEDGER_MAX_ACCOUNTS", 16)?)
                .map_err(|_| String::from("--max-accounts must fit in a usize"))?;
        let ledger = LedgerConfig::new(max_clients, max_accounts)
            .map_err(|error| format!("invalid ledger bounds: {error:?}"))?;

        Ok(Self {
            node_id,
            peers,
            cluster_dir: PathBuf::from(required("cluster-dir", "RAFTER_LEDGER_CLUSTER_DIR")?),
            client_listen: lookup("client-listen", "RAFTER_LEDGER_CLIENT_LISTEN")
                .unwrap_or_else(|| String::from("127.0.0.1:0")),
            peer_listen: lookup("peer-listen", "RAFTER_LEDGER_PEER_LISTEN")
                .unwrap_or_else(|| String::from("127.0.0.1:0")),
            election_timeout_ticks: parsed(
                "election-timeout-ticks",
                "RAFTER_LEDGER_ELECTION_TICKS",
                8,
            )?,
            tick_interval: Duration::from_millis(parsed(
                "tick-interval-ms",
                "RAFTER_LEDGER_TICK_MS",
                20,
            )?),
            ownership_wait: Duration::from_millis(parsed(
                "ownership-wait-ms",
                "RAFTER_LEDGER_OWNERSHIP_WAIT_MS",
                10_000,
            )?),
            request_timeout: Duration::from_millis(parsed(
                "request-timeout-ms",
                "RAFTER_LEDGER_REQUEST_MS",
                5_000,
            )?),
            ledger,
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

    let mut link = PeerLink::bind(
        &config.peer_listen,
        config.node_id,
        &config.peers,
        &config.cluster_dir,
    )
    .map_err(|error| format!("could not bind the peer port: {error}"))?;

    let outcome = serve(config, &node_dir, &mut link, &jobs_rx, client_addr);
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
    link: &mut PeerLink,
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
    let mut scratch = Vec::new();
    let mut stopping = false;

    loop {
        // Client requests first, so that a `SHUTDOWN` or a `STATUS` is answered
        // even when the replica does not exist yet.
        while let Ok(job) = jobs.recv_timeout(LOOP_POLL_INTERVAL) {
            let ticket = next_ticket;
            next_ticket += 1;
            match handle_job(&mut state, config, &job, ticket)? {
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
                match Replica::open(
                    node_dir,
                    config.node_id,
                    &config.peers,
                    config.election_timeout_ticks,
                    config.ledger,
                ) {
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
                    Err(error) => return Err(error.to_string()),
                }
            }
            State::Serving(replica) => {
                for inbound in link.drain_inbound() {
                    replica.deliver(inbound.from, *inbound.message)?;
                }
                if now >= next_tick {
                    replica.tick()?;
                    next_tick = now + config.tick_interval;
                }
                replica.service_pending(now)?;

                for envelope in replica.take_outbound() {
                    link.send(envelope.to, &envelope.message, &mut scratch);
                }
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

    if let State::Serving(replica) = &mut state {
        replica.abandon_waiters("the replica is shutting down");
        for answer in replica.take_answers() {
            let (ticket, response) = render_answer(answer);
            if let Some(reply) = responders.remove(&ticket) {
                drop(reply.send(response));
            }
        }
    }
    // Link counters are diagnostics rather than protocol. A nonzero drop count
    // is normal under load and means Raft retransmitted; a nonzero encode
    // failure is not normal and means the kernel produced a frame this
    // consumer-owned format does not carry.
    emit(&format!(
        "LINK {} dropped={} encode_failures={}",
        config.node_id.0,
        link.dropped_frames(),
        link.encode_failures()
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

fn handle_job(
    state: &mut State,
    config: &Config,
    job: &Job,
    ticket: u64,
) -> Result<Disposition, String> {
    let deadline = Instant::now() + config.request_timeout;
    match (&job.request, state) {
        (Request::Shutdown, _) => Ok(Disposition::Stop(String::from("BYE"))),
        (Request::Status, State::Opening { .. }) => Ok(Disposition::Answered(render_status(
            false,
            rafter::Role::Follower,
            0,
            0,
            0,
            None,
        ))),
        (Request::Status, State::Serving(replica)) => {
            let (role, term, leader) = replica.status();
            Ok(Disposition::Answered(render_status(
                replica.is_ready(),
                role,
                term,
                replica.applied_index().0,
                replica.committed_application_index().0,
                leader,
            )))
        }
        // Everything below is service, and service is what readiness gates.
        (_, State::Opening { .. }) => Ok(Disposition::Answered(String::from("NOTREADY 0 0"))),
        (_, State::Serving(replica)) if !replica.is_ready() => Ok(Disposition::Answered(format!(
            "NOTREADY {} {}",
            replica.applied_index().0,
            replica.committed_application_index().0
        ))),
        (Request::Submit(command), State::Serving(replica)) => {
            replica.submit(ticket, command.clone(), deadline)?;
            Ok(Disposition::Pending)
        }
        (Request::Query(query), State::Serving(replica)) => {
            replica.query(ticket, *query, deadline)?;
            Ok(Disposition::Pending)
        }
        (Request::Local(query), State::Serving(replica)) => match replica.local_query(*query) {
            Ok(result) => Ok(Disposition::Answered(render_query_result(result))),
            Err(detail) => Ok(Disposition::Answered(format!("ABANDONED {detail}"))),
        },
    }
}

fn render_answer(answer: Answer) -> (u64, String) {
    match answer {
        Answer::Submit { ticket, outcome } => {
            let response = match outcome {
                SubmitOutcome::Applied {
                    disposition,
                    response,
                } => render_applied(disposition, &response),
                SubmitOutcome::NotCommitted {
                    reason,
                    leader_hint,
                } => render_not_committed(&reason, leader_hint.map(|hint| hint.0)),
                SubmitOutcome::Unknown { reason } => format!("UNKNOWN {reason}"),
            };
            (ticket, response)
        }
        Answer::Query { ticket, outcome } => {
            let response = match outcome {
                QueryOutcome::Ready(result) => render_query_result(result),
                QueryOutcome::Abandoned { reason } => format!("ABANDONED {reason}"),
            };
            (ticket, response)
        }
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
fn emit(line: &str) {
    let mut stdout = std::io::stdout().lock();
    drop(writeln!(stdout, "{line}"));
    drop(stdout.flush());
}
