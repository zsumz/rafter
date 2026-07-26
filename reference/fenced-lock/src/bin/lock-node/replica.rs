//! One lock replica: durable stores, the managed driver, and readiness.
//!
//! The recipe here is the documented one, in the documented order:
//!
//! 1. take exclusive ownership of the replica directory;
//! 2. open the durable lock store and read its applied floor;
//! 3. recover the Raft runtime *through that floor*, so committed entries the
//!    application has already durably applied are not replayed into it;
//! 4. build the group at the same floor and hand the recovery outputs to the
//!    managed driver rather than applying them outside it; then
//! 5. serve clients only once the application has applied every command this
//!    replica knows to be committed.
//!
//! Step 5 is the readiness gate, and it is [`Replica::is_ready`]. The floor it
//! waits for is the group's committed *application* index, never the commit
//! index: elections and membership changes commit entries the state machine is
//! never told about, so a gate on the commit index would wait for an index the
//! application can never report and the replica would never serve anything.
//!
//! # Ownership order
//!
//! Ownership is acquired first, before the lock store is opened, and that
//! ordering is the only thing standing between two live processes and one
//! directory. `rafter-storage` takes a real operating-system lock on the Raft
//! store directory; the lock service's own slot files have no such lock, so
//! this process relies on holding the Raft lock for the whole life of the
//! replica. A second process is refused at step 1 rather than at the slot files
//! it would have interleaved publications into.
//!
//! # Why a driver rather than a group
//!
//! The lock's declared Rafter surface is `rafter-service`, and its whole
//! adapter — [`LockClient`], its write options, its terminal outcome
//! vocabulary — is written against the managed driver. So this replica owns
//! what a deployment owns: when it ticks, which frames it delivers, when it
//! drives its outstanding barriers, and how long a client waits. It does not
//! own waiter tables, ID allocation, or report routing, because
//! [`TransportRaftDriver`] does.
//!
//! Three entry points advance the protocol, and all three are called from the
//! one loop that owns this value: [`Replica::tick`], [`Replica::deliver`], and
//! [`Replica::drive_reads`]. The third is not optional — a granted read barrier
//! is consumed by a read call rather than announced, so a loop that skipped it
//! would leave every linearizable query unanswered.
//!
//! # Recovery is a decision, not a retry
//!
//! [`LockStore::open`] refuses a slot it cannot show was the one being written,
//! and the ordinary crash of a publication produces exactly such a slot:
//! `SlotDamage::UnsealedCompleteImage` is a whole image whose seal never landed,
//! and the store cannot tell it from a live slot whose seal byte rotted.
//! Refusing is recoverable under both readings; skipping is recoverable under
//! one. A killed process therefore does not always restart, and pretending it
//! does would be the flake this suite exists not to have.
//!
//! So the mode is an argument with three settings, each strictly more
//! destructive than the last, and none of them reachable by restarting into the
//! previous one:
//!
//! - `open` adopts what recovery can prove and refuses the rest;
//! - `repair` additionally gives up a slot this build cannot read, and is
//!   itself refused when the slot it would give up holds a fencing mark the
//!   adopted slot cannot dominate; and
//! - `reseed` deletes this replica's durable lock state outright and lets the
//!   replicated log refill it.
//!
//! Each announces what it cost. `reseed` is the one that discards acknowledged
//! fencing marks, and it is safe only because the marks are also in the
//! replicated log this replica is about to re-apply — which is a fact about the
//! cluster, not about this process. A replica reseeded while its own log had
//! been compacted past the discarded floor does not recover, and nothing here
//! pretends otherwise.

use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    task::{Context, Poll, Waker},
    time::Instant,
};

use rafter::{LocalProposalId, LogIndex, NodeConfig, NodeId, ReadId, Role};
use rafter_app::state_machine::ReplicatedStateMachine;
use rafter_reference_fenced_lock::{
    store::{LockStore, LockStoreError, RecoveryReport, SlotIndex},
    DurableLockStateMachine, LockClient, LockConfig, QueryOutcome, ResourceName, ResourceStatus,
    SubmitOutcome,
};
use rafter_runtime::DurableRaftNode;
use rafter_service::{
    InboundEnvelopeError, MetricsWatch, TransportDriverOptions, TransportRaftDriver,
};
use rafter_storage::{
    FileRaftHardStateStore, FileRaftLogSegment, FileRaftNodeStores, FileRaftSnapshotStore,
    OpenFileRaftNodeStoresError,
};

use rafter_app::group::RaftGroup;

use super::peer_link::{PeerDirectory, PeerPrincipal, TcpPeerTransport};

/// Caller-defined identity of the single lock group each replica serves.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LockGroupId(pub u64);

/// The one group every replica in this deployment serves.
pub const GROUP_ID: LockGroupId = LockGroupId(1);

/// The durable runtime a replica runs, held by its group and nothing else.
type LockRuntime =
    DurableRaftNode<FileRaftHardStateStore, FileRaftLogSegment, FileRaftSnapshotStore>;

/// One replica's managed driver over its own TCP endpoint.
pub type NodeDriver = TransportRaftDriver<
    LockGroupId,
    DurableLockStateMachine,
    LockRuntime,
    TcpPeerTransport,
    PeerDirectory,
>;

/// One inbound peer envelope, as the link hands it over.
pub type Inbound = rafter_service::AuthenticatedPeerEnvelope<LockGroupId, PeerPrincipal>;

/// How far this process may go to open a durable lock store.
///
/// Strictly increasing destructiveness. Nothing escalates on its own: a
/// restart under one mode never becomes a run under the next.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryMode {
    /// Adopt what recovery can prove, and refuse the rest.
    Open,
    /// Additionally give up a slot this build cannot read.
    Repair,
    /// Delete this replica's durable lock state and refill it from the log.
    Reseed,
}

impl RecoveryMode {
    /// Parses the `--recover` argument.
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "open" => Some(Self::Open),
            "repair" => Some(Self::Repair),
            "reseed" => Some(Self::Reseed),
            _ => None,
        }
    }
}

/// Why a replica could not be opened.
#[derive(Debug)]
pub enum OpenError {
    /// Another live process owns this replica directory.
    ///
    /// Kept separate from every other failure because it is the one a
    /// restarting replica may legitimately wait out: it races the exit of the
    /// incarnation it replaces.
    DirectoryOwned { directory: PathBuf },
    /// The durable lock store will not open under the mode it was given.
    ///
    /// Kept apart from every other store failure because it is the one a human
    /// has to answer. Restarting under the same mode will not change it, and
    /// the next mode up discards something. The message names which.
    NeedsDecision {
        detail: String,
        next_mode: &'static str,
    },
    /// A durable store could not be opened or recovered.
    Store { detail: String },
    /// The Raft runtime could not recover through the application's floor.
    Runtime { detail: String },
    /// The managed driver refused to adopt the recovered group.
    Driver { detail: String },
    /// The replica directory could not be created.
    Io {
        operation: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
    /// The static configuration this replica was given is not a valid cluster.
    Config { detail: String },
}

impl fmt::Display for OpenError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DirectoryOwned { directory } => write!(
                formatter,
                "another process owns the replica directory {}",
                directory.display()
            ),
            Self::NeedsDecision { detail, next_mode } => write!(
                formatter,
                "the durable lock store needs an operator decision and this replica will not \
                 serve until it gets one: {detail}. Restarting under the same mode will not \
                 change it. Running with --recover {next_mode} is the next step and it discards \
                 something: `repair` gives up a slot this build cannot read, and `reseed` gives \
                 up this replica's whole durable lock state, fencing high-water marks included. \
                 Both are safe only because the replicated log still holds what they discard"
            ),
            Self::Store { detail } => write!(formatter, "durable store failed: {detail}"),
            Self::Runtime { detail } => write!(formatter, "raft recovery failed: {detail}"),
            Self::Driver { detail } => write!(formatter, "managed driver refused: {detail}"),
            Self::Io {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "could not {operation} at {}: {source}",
                path.display()
            ),
            Self::Config { detail } => write!(formatter, "invalid replica configuration: {detail}"),
        }
    }
}

impl Error for OpenError {}

/// One answer the replica owes a client, addressed by the client's ticket.
#[derive(Debug)]
pub enum Answer {
    /// A replicated command reached a terminal outcome.
    Submit { ticket: u64, outcome: SubmitOutcome },
    /// A query reached a terminal outcome.
    Query {
        ticket: u64,
        outcome: QueryOutcome<LockGroupId>,
    },
    /// A command's outcome was lost to this replica's own request deadline.
    Unknown { ticket: u64, detail: String },
    /// A query returned no value, so it constrains no ordering.
    Abandoned { ticket: u64, detail: String },
}

type SubmitFuture = Pin<Box<dyn Future<Output = SubmitOutcome>>>;
type QueryFuture = Pin<Box<dyn Future<Output = QueryOutcome<LockGroupId>>>>;

/// One client request the replica has started and not yet answered.
enum Pending {
    Submit {
        ticket: u64,
        deadline: Instant,
        /// The ID the driver allocated for this write, learned before the
        /// future resolved so this replica can retire exactly its own waiter.
        local_proposal_id: Option<LocalProposalId>,
        future: SubmitFuture,
    },
    Query {
        ticket: u64,
        deadline: Instant,
        /// The ID the driver allocated for this barrier, on the same reasoning.
        read_id: Option<ReadId>,
        future: QueryFuture,
    },
}

impl fmt::Debug for Pending {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Submit { ticket, .. } => formatter
                .debug_struct("Submit")
                .field("ticket", ticket)
                .finish_non_exhaustive(),
            Self::Query { ticket, .. } => formatter
                .debug_struct("Query")
                .field("ticket", ticket)
                .finish_non_exhaustive(),
        }
    }
}

/// One lock replica driven by a consumer-owned process loop.
#[derive(Debug)]
pub struct Replica {
    driver: NodeDriver,
    client: LockClient<LockGroupId, NodeDriver>,
    metrics: MetricsWatch<LockGroupId>,
    ready: bool,
    pending: BTreeMap<u64, Pending>,
    answers: Vec<Answer>,
    refused_frames: u64,
}

/// Everything opening one replica needs to know.
///
/// A struct rather than eight positional arguments, because two of them are
/// `NodeId`-shaped and two are opaque handles from the link; a caller that
/// swapped a pair would still compile.
#[derive(Debug)]
pub struct OpenRequest<'a> {
    /// This replica's directory, holding `raft/` and `app/`.
    pub node_dir: &'a Path,
    /// This replica's identity.
    pub node_id: NodeId,
    /// Every replica in the cluster, this one included.
    pub members: &'a [NodeId],
    /// This replica's election timeout, in ticks.
    pub election_timeout_ticks: u64,
    /// The bounds the lock service runs under.
    pub lock_config: LockConfig,
    /// How far this process may go to open the durable store.
    pub mode: RecoveryMode,
    /// The outbound half of this replica's peer link.
    pub transport: TcpPeerTransport,
    /// The validator this replica refuses inbound frames with.
    pub validator: PeerDirectory,
}

impl Replica {
    /// Opens one replica's durable state, recovers it, and adopts it.
    ///
    /// # Errors
    ///
    /// Returns [`OpenError::DirectoryOwned`] when another process holds the
    /// replica directory, [`OpenError::NeedsDecision`] when the lock store will
    /// not open under the requested mode, and otherwise an error naming the
    /// store, runtime, driver, configuration, or filesystem operation that
    /// failed.
    pub fn open(request: OpenRequest<'_>) -> Result<Self, OpenError> {
        let OpenRequest {
            node_dir,
            node_id,
            members,
            election_timeout_ticks,
            lock_config,
            mode,
            transport,
            validator,
        } = request;
        let raft_dir = node_dir.join("raft");
        let app_dir = node_dir.join("app");
        for directory in [node_dir, &raft_dir, &app_dir] {
            std::fs::create_dir_all(directory).map_err(|source| OpenError::Io {
                operation: "create a replica directory",
                path: directory.to_path_buf(),
                source,
            })?;
        }

        // Ownership first. Everything below this line assumes this process is
        // the only one publishing into this directory.
        let stores = FileRaftNodeStores::open(&raft_dir).map_err(|error| match error {
            OpenFileRaftNodeStoresError::AlreadyOpen { directory } => {
                OpenError::DirectoryOwned { directory }
            }
            other => OpenError::Store {
                detail: other.to_string(),
            },
        })?;
        let (hard_state, log_segment, snapshot_store) = stores.into_parts();

        let opened = match mode {
            RecoveryMode::Open => LockStore::open(&app_dir, lock_config),
            RecoveryMode::Repair => LockStore::open_and_repair(&app_dir, lock_config),
            RecoveryMode::Reseed => LockStore::discard_and_reseed(&app_dir, lock_config),
        };
        let store = opened.map_err(|error| classify_store_error(&error, mode))?;

        announce_recovery(node_id, &app_dir, store.recovery());

        let app = DurableLockStateMachine::new(store);
        let applied_index = app.applied_index().map_err(|error| OpenError::Store {
            detail: error.to_string(),
        })?;

        let peers: Vec<NodeId> = members
            .iter()
            .copied()
            .filter(|member| *member != node_id)
            .collect();
        let config = NodeConfig::new(node_id, peers, election_timeout_ticks).map_err(|error| {
            OpenError::Config {
                detail: format!("{error:?}"),
            }
        })?;
        let recovered = DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
            config,
            hard_state,
            log_segment,
            snapshot_store,
            applied_index,
        )
        .map_err(|error| OpenError::Runtime {
            detail: format!("{error:?}"),
        })?;
        let (raft, recovery_outputs) = recovered.into_parts();

        // The driver takes the recovery *outputs* rather than an already-applied
        // group, so the recovery report's peer messages and snapshot directives
        // are routed by the driver instead of being dropped outside it.
        let group = RaftGroup::with_applied_index(GROUP_ID, node_id, raft, app, applied_index);
        let driver = NodeDriver::new(
            group,
            recovery_outputs,
            transport,
            validator,
            TransportDriverOptions::default(),
        )
        .map_err(|error| OpenError::Driver {
            detail: error.to_string(),
        })?;
        let handle = driver.handle();
        let metrics = handle.metrics().map_err(|error| OpenError::Driver {
            detail: error.to_string(),
        })?;

        let mut replica = Self {
            client: LockClient::new(handle),
            driver,
            metrics,
            ready: false,
            pending: BTreeMap::new(),
            answers: Vec::new(),
            refused_frames: 0,
        };
        replica.refresh_readiness();
        Ok(replica)
    }

    /// Whether this replica has applied everything it knows to be committed.
    pub const fn is_ready(&self) -> bool {
        self.ready
    }

    /// Returns this replica's durable applied index.
    pub fn applied_index(&self) -> LogIndex {
        self.driver
            .with_group(|group| {
                group
                    .state_machine()
                    .applied_index()
                    .unwrap_or(LogIndex::ZERO)
            })
            .unwrap_or(LogIndex::ZERO)
    }

    /// Returns the index this replica must apply through to be current.
    pub fn committed_application_index(&self) -> LogIndex {
        self.driver
            .committed_application_index()
            .unwrap_or(LogIndex::ZERO)
    }

    /// Returns this replica's role, term, and leader hint.
    pub fn status(&self) -> (Role, u64, Option<u64>) {
        let metrics = self.metrics.current();
        (
            metrics.role,
            metrics.term.0,
            metrics.leader_hint.map(|leader| leader.0),
        )
    }

    /// Returns how many inbound frames this replica's validator refused.
    pub const fn refused_frames(&self) -> u64 {
        self.refused_frames
    }

    /// Advances one tick.
    ///
    /// # Errors
    ///
    /// Returns an error when the driver refuses the step, which for this
    /// application means the durable backend could not commit a transaction.
    pub fn tick(&mut self) -> Result<(), String> {
        self.driver.tick().map_err(|error| error.to_string())?;
        self.refresh_readiness();
        Ok(())
    }

    /// Delivers one peer envelope.
    ///
    /// A frame this replica's own validator refuses is counted and dropped —
    /// that is a network event, not a fault. Anything the driver itself refuses
    /// is fatal, because a poisoned group serves nothing.
    ///
    /// # Errors
    ///
    /// As [`Replica::tick`].
    pub fn deliver(&mut self, envelope: Inbound) -> Result<(), String> {
        match self.driver.deliver(envelope) {
            Ok(()) => {}
            Err(InboundEnvelopeError::Rejected { .. }) => {
                self.refused_frames = self.refused_frames.saturating_add(1);
            }
            Err(error) => return Err(error.to_string()),
        }
        self.refresh_readiness();
        Ok(())
    }

    /// Collects every granted read barrier.
    ///
    /// The third entry point beside tick and deliver, and not optional: a
    /// granted barrier is consumed by a read call rather than announced to a
    /// client.
    ///
    /// # Errors
    ///
    /// As [`Replica::tick`].
    pub fn drive_reads(&mut self) -> Result<(), String> {
        self.driver
            .drive_pending_reads()
            .map_err(|error| error.to_string())?;
        self.refresh_readiness();
        Ok(())
    }

    /// Starts one replicated command on behalf of a client.
    pub fn submit(
        &mut self,
        ticket: u64,
        command: rafter_reference_fenced_lock::Command,
        deadline: Instant,
    ) {
        let client = self.client.clone();
        // The handle's methods are `async fn`s, so the driver's `write` — and
        // with it the waiter registration — does not run until this first poll.
        let mut future: SubmitFuture =
            Box::pin(async move { client.submit_command(command).await });
        if let Poll::Ready(outcome) = poll_once(&mut future) {
            self.answers.push(Answer::Submit { ticket, outcome });
            return;
        }
        let local_proposal_id = newest_pending_write(&self.driver);
        self.pending.insert(
            ticket,
            Pending::Submit {
                ticket,
                deadline,
                local_proposal_id,
                future,
            },
        );
    }

    /// Starts one linearizable `GetLock` on behalf of a client.
    pub fn query(&mut self, ticket: u64, resource: ResourceName, deadline: Instant) {
        let client = self.client.clone();
        let future: QueryFuture = Box::pin(async move { client.get_lock(resource).await });
        self.start_query(ticket, deadline, future);
    }

    /// Answers one `GetLock` from this replica's own applied state.
    ///
    /// Not a read through the managed read path, and it cannot be.
    /// [`TransportRaftDriver`] documents itself as
    /// [`ReadConsistency::Linearizable`](rafter_service::ReadConsistency) only
    /// and refuses every other level with `ReadError::UnsupportedConsistency`,
    /// because it owns a single replica and a local answer through a managed
    /// API would say nothing about how far behind that replica is.
    /// [`LockClient::get_lock`] narrows the same way for the lock's own reason:
    /// offering the choice would let a caller weaken the guarantee the fencing
    /// proof rests on.
    ///
    /// So a deployment that wants to know what *its own* replica holds borrows
    /// the group instead. [`TransportRaftDriver::with_group`] lends it under the
    /// driver's own lock, which is public API and is what the in-process suites
    /// already observe replicas through. What is given up by taking this route
    /// is real and is why the protocol keeps it under a separate verb: there is
    /// no barrier, no freshness claim, and no read proof. The answer is this
    /// replica's applied state and nothing more.
    ///
    /// It is synchronous because there is nothing to wait for.
    pub fn local_lock(&self, resource: ResourceName) -> Result<ResourceStatus, String> {
        self.driver
            .with_group(|group| group.state_machine().service().status(resource))
            .map_err(|error| error.to_string())
    }

    fn start_query(&mut self, ticket: u64, deadline: Instant, mut future: QueryFuture) {
        if let Poll::Ready(outcome) = poll_once(&mut future) {
            self.answers.push(Answer::Query { ticket, outcome });
            return;
        }
        let read_id = newest_pending_read(&self.driver);
        self.pending.insert(
            ticket,
            Pending::Query {
                ticket,
                deadline,
                read_id,
                future,
            },
        );
    }

    /// Polls every in-flight request and retires everything past its deadline.
    ///
    /// A caller that runs out of time hands its request back to the driver,
    /// which resolves the client with its own terminal vocabulary under the ID
    /// the driver itself allocated. That is a real client's situation rather
    /// than a shortcut: the window an abandoned write closes is genuinely
    /// unknown, because an appended entry may still commit.
    pub fn poll_pending(&mut self, now: Instant) {
        let tickets: Vec<u64> = self.pending.keys().copied().collect();
        for ticket in tickets {
            let Some(mut pending) = self.pending.remove(&ticket) else {
                continue;
            };
            match self.settle(&mut pending, now) {
                Some(answer) => self.answers.push(answer),
                None => {
                    self.pending.insert(ticket, pending);
                }
            }
        }
    }

    fn settle(&mut self, pending: &mut Pending, now: Instant) -> Option<Answer> {
        match pending {
            Pending::Submit {
                ticket,
                deadline,
                local_proposal_id,
                future,
            } => {
                if let Poll::Ready(outcome) = poll_once(future) {
                    return Some(Answer::Submit {
                        ticket: *ticket,
                        outcome,
                    });
                }
                if now < *deadline {
                    return None;
                }
                // `false` means the write resolved between the poll above and
                // this call, which the poll below then observes. Either way the
                // driver, not this replica, decides the terminal outcome.
                if let Some(id) = *local_proposal_id {
                    let _ = self.driver.abandon_write(id);
                }
                Some(match poll_once(future) {
                    Poll::Ready(outcome) => Answer::Submit {
                        ticket: *ticket,
                        outcome,
                    },
                    // The driver named no waiter for this write, so there was
                    // nothing to retire. The outcome is still unknown, and
                    // nothing weaker than that is honest.
                    Poll::Pending => Answer::Unknown {
                        ticket: *ticket,
                        detail: String::from("deadline"),
                    },
                })
            }
            Pending::Query {
                ticket,
                deadline,
                read_id,
                future,
            } => {
                if let Poll::Ready(outcome) = poll_once(future) {
                    return Some(Answer::Query {
                        ticket: *ticket,
                        outcome,
                    });
                }
                if now < *deadline {
                    return None;
                }
                if let Some(id) = *read_id {
                    let _ = self.driver.abandon_read(id);
                }
                Some(match poll_once(future) {
                    Poll::Ready(outcome) => Answer::Query {
                        ticket: *ticket,
                        outcome,
                    },
                    Poll::Pending => Answer::Abandoned {
                        ticket: *ticket,
                        detail: String::from("deadline"),
                    },
                })
            }
        }
    }

    /// Takes every answer this replica owes its clients.
    pub fn take_answers(&mut self) -> Vec<Answer> {
        std::mem::take(&mut self.answers)
    }

    /// Fails every waiting client with a terminal non-answer.
    ///
    /// A replica that is stopping has learned nothing about its in-flight
    /// proposals, so it must not claim they did not commit. A read takes no
    /// effect, so an abandoned one is terminal rather than unknown.
    pub fn abandon_waiters(&mut self, reason: &str) {
        for (ticket, pending) in std::mem::take(&mut self.pending) {
            self.answers.push(match pending {
                Pending::Submit { .. } => Answer::Unknown {
                    ticket,
                    detail: reason.to_string(),
                },
                Pending::Query { .. } => Answer::Abandoned {
                    ticket,
                    detail: reason.to_string(),
                },
            });
        }
    }

    /// Recomputes the readiness gate.
    ///
    /// Readiness is one-way. A replica that has caught up once and then falls
    /// behind a newly committed entry is an ordinary follower, not a replica in
    /// recovery, and flapping the gate would make a healthy cluster refuse
    /// service every time a write landed.
    fn refresh_readiness(&mut self) {
        if !self.ready && self.applied_index() >= self.committed_application_index() {
            self.ready = true;
        }
    }
}

/// Announces what opening the durable store found.
///
/// The recovery report is consumed rather than dropped. A report nothing reads
/// is a report that costs nothing to be wrong, so this process announces what
/// opening found — and refuses to serve, elsewhere, on the reports that need a
/// human.
///
/// Creation is announced on its own line rather than folded into the ordinary
/// one. On a replica's first boot it is expected; on a restart it means the
/// slot files that were there are gone and this replica is about to serve an
/// empty lock table from applied index zero, with no fencing high-water marks
/// at all. The store cannot tell those apart — both arrive as two absent files
/// — so it reports the fact and leaves the judgement to whoever knows whether
/// this replica has run before.
fn announce_recovery(node_id: NodeId, app_dir: &Path, recovery: &RecoveryReport) {
    if let Some(reseed) = recovery.reseed() {
        crate::emit(&format!(
            "RESEEDED {} discarded_bytes={} discarded_applied_index={}",
            node_id.0,
            reseed.discarded_bytes(),
            reseed
                .discarded_applied_index()
                .map_or_else(|| String::from("-"), |index| index.0.to_string())
        ));
    } else if let Some(repair) = recovery.repair() {
        crate::emit(&format!(
            "REPAIRED {} discarded_slot={} adopted_slot={} adopted_generation={} \
             discarded_generation={} marks_cross_checked={} damage={:?}",
            node_id.0,
            slot_token(repair.slot()),
            slot_token(repair.adopted()),
            repair.adopted_generation(),
            repair
                .discarded_generation()
                .map_or_else(|| String::from("-"), |generation| generation.to_string()),
            repair.marks_cross_checked(),
            repair.damage()
        ));
    } else if recovery.created() {
        crate::emit(&format!("CREATED {} {}", node_id.0, app_dir.display()));
    } else if !recovery.is_clean() {
        crate::emit(&format!(
            "RECOVERED {} live_slot={} cross_checked_marks={} damage={:?}",
            node_id.0,
            recovery
                .live_slot()
                .map_or_else(|| String::from("-"), slot_token),
            recovery.cross_checked_marks(),
            recovery.damaged_slot().map(|(_, damage)| damage)
        ));
    }
}

/// Renders a slot index as a stable one-character token.
fn slot_token(slot: SlotIndex) -> String {
    match slot {
        SlotIndex::Zero => String::from("0"),
        SlotIndex::One => String::from("1"),
    }
}

/// Decides whether a store failure is one an operator has to answer.
///
/// The three that are, are the three the next mode up addresses. Everything
/// else is a plain failure: a filesystem error, a configuration that does not
/// match the image, a poisoned handle. Escalating on those would discard state
/// to work around a problem escalation does not solve.
fn classify_store_error(error: &LockStoreError, mode: RecoveryMode) -> OpenError {
    let needs_repair = matches!(error, LockStoreError::UnreadableSlot { .. });
    let needs_reseed = matches!(
        error,
        LockStoreError::NoReadableImage { .. }
            | LockStoreError::MissingSlot { .. }
            | LockStoreError::AmbiguousGeneration { .. }
            | LockStoreError::DiscardWouldRegressMark { .. }
    );
    let next_mode = match (mode, needs_repair, needs_reseed) {
        (RecoveryMode::Open, true, _) => Some("repair"),
        // A repair that still cannot read a slot has nothing left but a reseed,
        // which is the same answer the mark-regression refusal earns.
        (RecoveryMode::Open | RecoveryMode::Repair, _, true) | (RecoveryMode::Repair, true, _) => {
            Some("reseed")
        }
        _ => None,
    };
    match next_mode {
        Some(next_mode) => OpenError::NeedsDecision {
            detail: error.to_string(),
            next_mode,
        },
        None => OpenError::Store {
            detail: error.to_string(),
        },
    }
}

/// Returns the ID of the write a driver most recently admitted.
///
/// Local proposal IDs are strictly increasing for a driver's lifetime, so the
/// highest unresolved one is the write that was just started. `None` means the
/// write resolved inside its first poll and has no waiter left to name.
fn newest_pending_write(driver: &NodeDriver) -> Option<LocalProposalId> {
    driver
        .pending_writes()
        .into_iter()
        .map(|write| write.local_proposal_id)
        .max()
}

/// Returns the ID of the barrier a driver most recently admitted, on the same
/// monotonicity as [`newest_pending_write`].
fn newest_pending_read(driver: &NodeDriver) -> Option<ReadId> {
    driver.pending_reads().into_iter().max()
}

fn poll_once<T>(future: &mut Pin<Box<dyn Future<Output = T>>>) -> Poll<T> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    future.as_mut().poll(&mut context)
}
