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
//! --control-plane-fault-after <n> RAFTER_LOCK_CONTROL_PLANE_FAULT_AFTER
//! ```
//!
//! The last one is a **fault seam, not an operator control**: from the `n`-th
//! client operation onward this replica cannot make its peer control plane
//! durable. It exists because the state it produces — a replica that knows its
//! next restart would forget what it retired — is otherwise unreachable from a
//! test that spawns this process, and it is the state the shutdown discipline
//! below is entirely about. See
//! [`OpenRequest::control_plane_fault_after`](replica::OpenRequest::control_plane_fault_after).
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
//! LINK <id> dropped=<n> unencodable=<n> refused_chunks=<n> refused_frames=<n> non_member_frames=<n>
//! CONTROL_PLANE_UNPERSISTED <id> <detail>
//! STOPPED <id>                     the process stopped cleanly
//! FATAL <detail>                   followed by a nonzero exit
//! ```
//!
//! `CONTROL_PLANE_UNPERSISTED` and `STOPPED` are mutually exclusive, and that is
//! the point of the pair. A replica that could not make its peer control plane
//! durable did not stop cleanly: the record it failed to write is the one no
//! other artifact holds, so its next start begins by forgetting whatever it
//! retired. It stops serving at once, prints this line, and the process exits
//! nonzero through `FATAL` — never through `STOPPED` and exit 0, which a
//! supervisor would read as a clean stop and restart into exactly that
//! forgetting.
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
//! turned away; `non_member_frames` counts frames the validator authorized and
//! the driver's *membership* did not, which is the window between a committed
//! removal and a fence the link layer has accepted. The last two are separate
//! because a non-zero `refused_frames` says the outer admission control is
//! working, and a non-zero `non_member_frames` says the control plane is
//! running behind the cluster.
//!
//! # The loop, and what bounds it
//!
//! One pass answers at most [`MAX_JOBS_PER_PASS`] client requests and then does
//! everything else it owes: it inspects the stored control-plane failure,
//! delivers whatever the peer link received, ticks if the clock says so, drives
//! granted reads, and expires deadlines. **The budget is what makes "and then"
//! true.** Draining the client channel until it went quiet meant a stream of
//! immediately-answered requests never let the pass finish, and `STATUS` — which
//! touches nothing and answers from memory — was enough on its own to hold a
//! replica off its clock for as long as a client cared to keep asking.
//!
//! ## What is *not* bounded here, and why that is the honest answer for v1
//!
//! The channel from the client threads is unbounded and the acceptor spawns a
//! thread per connection, so this process's memory and thread count are bounded
//! by the operating system rather than by anything in this file. A bounded
//! `sync_channel` and a connection limit were considered and are deliberately
//! not here.
//!
//! The channel first: its depth is already bounded by the number of live
//! connections, because a connection is strictly sequential — the reply to a
//! request is written before the next line is read — so a client cannot queue a
//! second request until the first is answered. Swapping in a `sync_channel`
//! would therefore not bound a resource that was actually unbounded; it would
//! move where a burst blocks, from this loop to the client threads, and leave
//! the binding resource exactly where it was.
//!
//! That binding resource is threads and file descriptors, and a connection limit
//! is what would bound it. It is out of scope for a different reason: refusing an
//! accept needs an answer the client protocol does not have, and adding one is a
//! wire-contract change rather than a loop fix. `CONTRACT.md` scopes this
//! composition as integration composition over a protocol that authenticates
//! nothing — any connection may already claim any client identity — so a
//! resource bound here would not be a property this artifact can claim against
//! an adversary, only against an accident.
//!
//! The per-pass budget is a different kind of statement and that is why it is
//! here rather than deferred with the rest. It bounds a *correctness* property:
//! that Raft's clock, its inbound frames, and this process's own terminal exit
//! are reached on every pass regardless of client load. A production composition
//! would want all three bounds; this one needs the first.
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

mod control_plane;
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

/// One client request waiting for the loop that owns the replica.
#[derive(Debug)]
struct Job {
    request: Request,
    reply: Sender<String>,
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
    /// **Terminal by policy rather than by exhaustion.** A replica that cannot
    /// make its peer control plane durable is one whose next restart forgets a
    /// removal, and that outranks anything it could still do for a client: the
    /// removals it would forget are permanent, and the client work it is
    /// refusing will be retried against a replica that can still record what it
    /// retires.
    ///
    /// It is a *state* rather than a flag checked at the next call because of
    /// where the failure comes from. `submit`, `query`, `poll_pending`, and
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
        detail: String,
    },
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
                }
            }
            // A terminal state ends the pass rather than spending the budget on
            // it. `State::Failed` answers every request `ABANDONED`, and doing
            // that is not what the process is for once it has one: the work
            // below is its exit, and a pass that kept answering would postpone
            // the exit for exactly as long as clients kept asking.
            if stopping || matches!(state, State::Failed { .. }) {
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
            // Terminal, and reached only through the arm below. The next pass's
            // job drain refuses whatever arrived in the meantime, and then this
            // breaks — so no client request is served after the failure and none
            // is silently dropped either.
            State::Failed { .. } => {
                stopping = true;
            }
            State::Serving(replica) => {
                // Before any protocol work this pass, and before the next job
                // drain. A stored persistence failure is terminal; see
                // `State::Failed` for why waiting for the next call with an
                // error channel is waiting too long.
                if let Some(detail) = replica.control_plane_failure().map(str::to_owned) {
                    emit(&format!(
                        "CONTROL_PLANE_UNPERSISTED {} {detail}",
                        config.node_id.0
                    ));
                    replica.abandon_waiters("the replica cannot make its control plane durable");
                    for answer in replica.take_answers() {
                        let (ticket, response) = render_answer(answer);
                        if let Some(reply) = responders.remove(&ticket) {
                            drop(reply.send(response));
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
                    state = State::Failed { replica, detail };
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

    // Whatever the terminal state already knows, plus whatever the shutdown
    // flush discovers. Either one means the same thing and both end the same
    // way; they differ only in when the replica stopped serving.
    let mut unpersisted = match &state {
        State::Failed { detail, .. } => Some(detail.clone()),
        _ => None,
    };
    let (refused_frames, non_member_frames) = match &mut state {
        State::Serving(replica) => {
            replica.abandon_waiters("the replica is shutting down");
            for answer in replica.take_answers() {
                let (ticket, response) = render_answer(answer);
                if let Some(reply) = responders.remove(&ticket) {
                    drop(reply.send(response));
                }
            }
            // The shutdown flush has no later entry point to raise it, so the
            // process says so on its own channel rather than stopping quietly
            // over a control plane it could not make durable.
            if let Some(failure) = replica.control_plane_failure() {
                emit(&format!(
                    "CONTROL_PLANE_UNPERSISTED {} {failure}",
                    config.node_id.0
                ));
                unpersisted = Some(failure.to_owned());
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
    // Before `STOPPED`, and instead of it: `stop_outcome` returns `Err` for an
    // unpersisted control plane, and this only announces a clean stop when it
    // does not.
    stop_outcome(unpersisted)?;
    emit(&format!("STOPPED {}", config.node_id.0));
    Ok(())
}

/// What the process reports once the loop has ended.
///
/// **`STOPPED` and a nonzero exit are the two mutually exclusive endings, and
/// which one applies turns on a single fact.** A replica that could not make its
/// peer control plane durable did not stop cleanly: the record it failed to
/// write is the one no other artifact holds, so the next start of this replica
/// begins by forgetting whatever it retired — an identity a committed removal
/// spent becomes allocatable again, and a fence the link layer refused is never
/// retried. A supervisor reading exit 0 restarts it into exactly that.
///
/// A free function for the reason `replica::combine_outcomes` is one: the rule
/// is checkable without three processes and a filesystem that can be made to
/// fail at the one moment that matters.
fn stop_outcome(unpersisted: Option<String>) -> Result<(), String> {
    match unpersisted {
        Some(detail) => Err(format!(
            "the peer control plane could not be made durable: {detail}"
        )),
        None => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::stop_outcome;

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
        let outcome = stop_outcome(Some(String::from("no space left on device")));
        let detail = outcome.expect_err("an unpersisted control plane is not a clean stop");
        assert!(
            detail.contains("could not be made durable")
                && detail.contains("no space left on device"),
            "the reason reaches the operator: {detail}"
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

fn handle_job(state: &mut State, config: &Config, job: &Job, ticket: u64) -> Disposition {
    let deadline = Instant::now() + config.request_timeout;
    match (&job.request, state) {
        (Request::Shutdown, _) => Disposition::Stop(String::from("BYE")),
        (Request::Status, State::Opening { .. }) => {
            Disposition::Answered(render_status(false, rafter::Role::Follower, 0, 0, 0, None))
        }
        (Request::Status, State::Serving(replica) | State::Failed { replica, .. }) => {
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
        // A terminal refusal rather than `NOTREADY`, because `NOTREADY` invites
        // a retry against this replica and nothing here will become ready.
        (_, State::Failed { detail, .. }) => Disposition::Answered(format!("ABANDONED {detail}")),
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
