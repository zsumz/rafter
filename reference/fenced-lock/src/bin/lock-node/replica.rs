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
use rafter_app::membership::MembershipChange;
use rafter_app::state_machine::ReplicatedStateMachine;
use rafter_reference_fenced_lock::{
    store::{LockStore, LockStoreError, RecoveryReport, SlotIndex},
    write_options, DurableLockStateMachine, LockClient, LockConfig, LockQuery, QueryOutcome,
    ResourceName, ResourceStatus, SubmitOutcome,
};
use rafter_runtime::DurableRaftNode;
use rafter_service::{
    DriverServiceState, InboundEnvelopeError, MetricsWatch, ReadOptions, TransportDriverOptions,
    TransportRaftDriver,
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
    /// The peer-control-plane checkpoint could not be read or made durable.
    ///
    /// Fatal on both sides rather than counted. A checkpoint this process
    /// cannot read is one it must not silently replace with an empty one — that
    /// is exactly the forgotten removal the file exists to prevent — and one it
    /// cannot write leaves the next restart to forget it instead.
    ControlPlane { detail: String },
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
            Self::ControlPlane { detail } => {
                write!(formatter, "peer control plane checkpoint failed: {detail}")
            }
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
        /// The ID the driver allocated for this write, returned by the call
        /// that started it so this replica can retire exactly its own waiter.
        local_proposal_id: LocalProposalId,
        future: SubmitFuture,
    },
    Query {
        ticket: u64,
        deadline: Instant,
        /// The ID the driver allocated for this barrier, on the same reasoning.
        read_id: ReadId,
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
    non_member_frames: u64,
    /// Where this replica's durable state lives, so the control-plane
    /// checkpoint is published beside the log it belongs to.
    node_dir: PathBuf,
    /// The driver epoch this replica last made durable.
    ///
    /// The driver's own change signal, held here so a persist is one `u64`
    /// comparison on the passes where nothing moved.
    persisted_checkpoint_epoch: u64,
    /// Whether a checkpoint has been written by *this* incarnation.
    ///
    /// The epoch is instance-local and starts at zero, so it cannot by itself
    /// distinguish "nothing has moved" from "nothing has been written". Without
    /// this a driver whose epoch never left zero would never publish a file.
    checkpoint_written: bool,
    /// A persistence failure raised by an entry point with no error channel.
    ///
    /// `submit`, `query`, `poll_pending`, and `abandon_waiters` can move the
    /// driver's checkpoint epoch and answer their client with a value rather
    /// than a `Result`, so a persist failure inside one has nowhere to go. It is
    /// held here and raised by the next entry point that has an error channel —
    /// which is at most one pass of the process loop away, because the loop
    /// reaches `drive_reads` on every pass. Dropping it instead would let a
    /// replica keep serving with a control plane it can no longer make durable,
    /// which is the state whose next restart forgets a removal.
    control_plane_failure: Option<String>,
    /// The client operation from which this replica's control plane is
    /// undurable, if a fault is armed.
    ///
    /// See [`OpenRequest::control_plane_fault_after`].
    control_plane_fault_after: Option<u64>,
    /// How many client operations this replica has started.
    client_operations: u64,
}

/// Combines a persistence outcome, a carried persistence failure, and an
/// operation's own outcome into the one answer an entry point returns.
///
/// **The order is the rule and it has three steps.** A persist failure from
/// *this* call dominates; then a persist failure carried from an entry point
/// with no error channel of its own; then the operation's own failure; then
/// `Ok`. Persistence outranks the operation because a replica that cannot make
/// its control plane durable is one whose next restart forgets a removal, and
/// that is worse than the step that happened to fail beside it — the step will
/// be retried by the protocol, and the forgetting will not.
///
/// A free function rather than a method so the rule is checkable without a
/// driver, a directory, and three processes; see the tests beneath it.
fn combine_outcomes(
    persisted: Result<(), String>,
    carried: Option<String>,
    outcome: Result<(), String>,
) -> Result<(), String> {
    match persisted.err().or(carried) {
        Some(failure) => Err(failure),
        None => outcome,
    }
}

/// Whether a replica may report itself ready.
///
/// **Two facts, and the second one used to be missing.** `caught_up` is the
/// application-floor gate — this replica has applied everything it knows to be
/// committed — and it is deliberately one-way: catching up is not something a
/// replica un-does, and a flapping gate would refuse service every time a write
/// landed. But a driver refuses client work for reasons the applied floor knows
/// nothing about, and readiness derived from the floor alone reported `ready` for
/// every one of them. A supervisor that polls readiness rather than watching exit
/// codes saw a healthy replica refusing everything.
///
/// The conjunction can fall, and that is correct rather than a regression in the
/// gate: what it answers is "may a client use this replica now", which is not a
/// property that only ever improves.
///
/// **`DriverServiceState` is `#[non_exhaustive]`, and the match is written so a
/// variant this build does not know reads as *not* ready.** That is the
/// fail-closed direction: a new refusal this process cannot name is still a
/// refusal, and claiming readiness through it would be exactly the defect above
/// arriving by a different route.
///
/// A free function for the reason [`combine_outcomes`] is one: the rule is
/// checkable without a driver, a directory, and three processes, and the tests
/// beneath this file pin every arm of it.
fn readiness(caught_up: bool, service_state: DriverServiceState) -> bool {
    caught_up && matches!(service_state, DriverServiceState::Serving)
}

/// Creates the directories one replica's durable state lives in.
///
/// Ahead of the ownership lock, so a first boot has somewhere to take it.
fn create_replica_directories(directories: &[&Path]) -> Result<(), OpenError> {
    for directory in directories {
        std::fs::create_dir_all(directory).map_err(|source| OpenError::Io {
            operation: "create a replica directory",
            path: (*directory).to_path_buf(),
            source,
        })?;
    }
    Ok(())
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
    /// The client operation from which this replica's control plane can no
    /// longer be made durable, if a fault is armed.
    ///
    /// **A deterministic fault seam, in the same shape the durable store's
    /// [`FaultPlan`](rafter_reference_fenced_lock::store::FaultPlan) already
    /// has and for the same reason.** A test that means to observe what a
    /// replica does when it cannot record what it retired has to be able to
    /// produce that state, and the honest ways to produce it from outside — a
    /// full disk, a revoked directory permission — are not reachable from a test
    /// that spawns this process, because in a cluster whose membership never
    /// changes the driver's checkpoint epoch stands still after the first write
    /// and no later write is attempted at all.
    ///
    /// So the fault is armed at a *client operation ordinal* rather than at a
    /// write ordinal: from the `n`-th operation onward every attempt to make the
    /// control plane durable fails, and the attempt is made whether or not the
    /// epoch moved. What is being injected is an artifact this replica can no
    /// longer write, not one update lost — and a replica in that state must stop
    /// serving and exit nonzero rather than carry on toward a restart that
    /// forgets what it retired.
    ///
    /// `None` for every replica an operator starts, which is the only value
    /// [`Config`](super::Config) produces unless the fault argument is given.
    pub control_plane_fault_after: Option<u64>,
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
            control_plane_fault_after,
        } = request;
        let raft_dir = node_dir.join("raft");
        let app_dir = node_dir.join("app");
        create_replica_directories(&[node_dir, &raft_dir, &app_dir])?;

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

        // The machine is handed the same `snapshots` directory the runtime's
        // own snapshot store owns, because a Raft-driven install gives the
        // application a descriptor rather than bytes and this is where the
        // bytes have already been promoted. It is a read-only second view: the
        // runtime below still owns the writing handle.
        let app = DurableLockStateMachine::new(store, raft_dir.join("snapshots"));
        let applied_index = app.applied_index().map_err(|error| OpenError::Store {
            detail: error.to_string(),
        })?;

        let config = if members.contains(&node_id) {
            let peers = members
                .iter()
                .copied()
                .filter(|member| *member != node_id)
                .collect();
            NodeConfig::new(node_id, peers, election_timeout_ticks)
        } else {
            NodeConfig::new_non_voter(node_id, members.to_vec(), election_timeout_ticks)
        }
        .map_err(|error| OpenError::Config {
            detail: format!("{error:?}"),
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

        // What Raft cannot give back. Read before the driver is built, because
        // the driver applies it before it derives its first membership fact from
        // the recovered group — a mark of 5 has to beat a reconstructed
        // committed set of {1,2} rather than lose to it.
        //
        // The commit floor travels with the call because it is what decides
        // whether an *absent* file is a first boot or a deleted artifact. It is
        // read from the recovered runtime rather than probed off the filesystem:
        // this method creates `raft/` and `app/` before it ever writes the
        // checkpoint, so their presence proves nothing, while a commit index of
        // zero proves there is no retirement to have lost.
        let checkpoint =
            super::control_plane::load(node_dir, raft.commit_index()).map_err(|error| {
                OpenError::ControlPlane {
                    detail: error.to_string(),
                }
            })?;

        // The driver takes the recovery *outputs* rather than an already-applied
        // group, so the recovery report's peer messages and snapshot directives
        // are routed by the driver instead of being dropped outside it.
        let group = RaftGroup::with_applied_index(GROUP_ID, node_id, raft, app, applied_index);
        let driver = NodeDriver::with_control_plane_checkpoint(
            group,
            recovery_outputs,
            transport,
            validator,
            TransportDriverOptions::default(),
            checkpoint,
        )
        .map_err(|error| OpenError::Driver {
            detail: error.to_string(),
        })?;
        let handle = driver.handle();
        let metrics = handle.metrics().map_err(|error| OpenError::Driver {
            detail: error.to_string(),
        })?;

        let mut replica = Self {
            node_dir: node_dir.to_path_buf(),
            persisted_checkpoint_epoch: driver.control_plane_checkpoint_epoch(),
            checkpoint_written: false,
            client: LockClient::new(handle),
            driver,
            metrics,
            ready: false,
            pending: BTreeMap::new(),
            answers: Vec::new(),
            refused_frames: 0,
            non_member_frames: 0,
            control_plane_failure: None,
            control_plane_fault_after,
            client_operations: 0,
        };
        // The restored checkpoint is written straight back, so the file on disk
        // always describes a driver that has run rather than one that was about
        // to. A first boot writes an empty one, which is the honest statement
        // that nothing has been retired here yet.
        replica.persist_control_plane()?;
        replica.refresh_readiness();
        Ok(replica)
    }

    /// Makes the driver's peer control plane durable when it has moved.
    ///
    /// Called from every entry point of the process loop, and it costs a `u64`
    /// comparison on the passes where nothing changed. The driver's epoch moves
    /// on exactly the facts a restart cannot rebuild — a committed configuration
    /// that moves the retirement record, and the marker a contradiction sets —
    /// so writing on that signal writes what must not be lost and nothing else.
    /// It deliberately does *not* move for a policy publication: retirement is
    /// published as a floor re-derived from the mark, so what a link layer has
    /// accepted is not a fact this file has to carry.
    ///
    /// The insecure integration process keeps a static membership, so its epoch
    /// ordinarily stands still after the first write. The production fixture
    /// reconfigures through this same replica and makes the dynamic path real:
    /// each committed configuration advances and persists the checkpoint before
    /// later work is served.
    ///
    /// # Errors
    ///
    /// Returns an error when the checkpoint cannot be made durable. That is
    /// fatal by construction — a replica whose control plane it cannot persist
    /// is one whose next restart forgets a removal — so it is reported rather
    /// than counted.
    fn persist_control_plane(&mut self) -> Result<(), OpenError> {
        // Ahead of the epoch comparison, because an armed fault says the
        // artifact is unwritable rather than that an update was lost; see
        // [`OpenRequest::control_plane_fault_after`].
        if self
            .control_plane_fault_after
            .is_some_and(|ordinal| self.client_operations >= ordinal)
        {
            return Err(OpenError::ControlPlane {
                detail: String::from("an injected fault makes this checkpoint unwritable"),
            });
        }
        let epoch = self.driver.control_plane_checkpoint_epoch();
        if epoch == self.persisted_checkpoint_epoch && self.checkpoint_written {
            return Ok(());
        }
        super::control_plane::store(&self.node_dir, &self.driver.control_plane_checkpoint())
            .map_err(|error| OpenError::ControlPlane {
                detail: error.to_string(),
            })?;
        self.persisted_checkpoint_epoch = epoch;
        self.checkpoint_written = true;
        Ok(())
    }

    /// Persists the control plane whatever the operation did, and combines the
    /// two error channels.
    ///
    /// **Finally-style, and that is the whole point.** Every operation that
    /// reaches the driver can move the checkpoint epoch *before* it fails: a
    /// `deliver` whose group refuses the step has already routed the membership
    /// the runtime moved through, and a `tick` that fails has already flushed
    /// whatever the link layer accepted. An operation that returned early on its
    /// own failure left that behind unpersisted, so the next restart read a file
    /// describing a driver that had not run — which is exactly the forgetting the
    /// checkpoint exists to prevent, arriving through the error path instead of
    /// through a crash.
    ///
    /// The two channels combine in one order: **a persist failure dominates**,
    /// because a replica that cannot make its control plane durable is one whose
    /// next restart forgets a removal, and that outranks a step that failed;
    /// otherwise the operation's own failure; otherwise `Ok`. A failure carried
    /// from an entry point with no error channel is a persist failure and
    /// dominates on the same terms.
    fn finish(&mut self, outcome: Result<(), String>) -> Result<(), String> {
        let persisted = self
            .persist_control_plane()
            .map_err(|error| error.to_string());
        combine_outcomes(persisted, self.control_plane_failure.take(), outcome)
    }

    /// The same persistence for an entry point that answers its client with a
    /// value rather than a `Result`.
    ///
    /// The failure is recorded rather than dropped; see
    /// [`Replica::control_plane_failure`]. The first failure wins, because the
    /// process is going to stop on it and the first one is the one that explains
    /// why.
    fn finish_without_channel(&mut self) {
        if let Err(error) = self.persist_control_plane() {
            self.control_plane_failure
                .get_or_insert_with(|| error.to_string());
        }
    }

    /// Whether this replica has applied everything it knows to be committed
    /// **and** its driver is still willing to serve.
    ///
    /// **Two facts, and the second one used to be missing.** The gate itself is
    /// one-way and should be: catching up is not something a replica un-does, and
    /// a flapping gate would refuse service every time a write landed. But a
    /// driver refuses client work for reasons that have nothing to do with the
    /// applied floor — a committed removal spent this replica's identity, no
    /// configuration names it, its own licensing inputs contradict each other —
    /// and a readiness answer derived from the floor alone reported `ready` for
    /// every one of them. A supervisor that polls readiness rather than watching
    /// exit codes saw a healthy replica refusing everything.
    ///
    /// So the floor stays one-way and the driver is consulted on every read. The
    /// conjunction can fall, which is correct: what it answers is "may a client
    /// use this replica now", and that is not a property that only ever improves.
    pub fn is_ready(&self) -> bool {
        readiness(self.ready, self.driver.service_state())
    }

    /// The operator-facing reason this replica will never serve again, if its
    /// control plane has reached one.
    ///
    /// **Only the contradictions, and that is the whole of the predicate.** A
    /// driver refuses client work in five states and four of them are not this
    /// process's business to die over: `NotMember` and `Released` end by
    /// themselves, `Decommissioned` is a replica the cluster deliberately removed
    /// — an operator retires that process, and killing it here would turn an
    /// orderly removal into a crash — and `ShuttingDown` is this process already
    /// stopping. The two contradictory states are different in kind: the facts
    /// that license this replica's permanent statement about who is retired
    /// disagree, no later fact decides them, and the driver has already stopped
    /// publishing. A process that kept running would be a replica reporting
    /// itself alive while it cannot answer the one question it exists to answer.
    ///
    /// `DriverServiceState` is `#[non_exhaustive]`, so the wildcard is the arm a
    /// new variant lands in and it deliberately does *not* terminate: a state
    /// this build cannot name is not a state it can claim is unrecoverable.
    pub fn terminal_control_plane_failure(&self) -> Option<String> {
        match self.driver.service_state() {
            DriverServiceState::ContradictoryCurrentState { through } => Some(format!(
                "two observations of the committed membership at index {} disagree",
                through.0
            )),
            DriverServiceState::ContradictoryTransitionPredecessor { through } => Some(format!(
                "a committed transition declares a membership at index {} that this \
                 replica's own record contradicts",
                through.0
            )),
            _ => None,
        }
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

    /// Returns a control-plane persistence failure this replica recorded and has
    /// not yet raised, if any.
    ///
    /// Only the shutdown path reads this. Every other entry point either raises
    /// the failure itself or is followed within one pass of the process loop by
    /// one that does; the last flush has no successor, so the process reads it
    /// here rather than letting it disappear with the replica.
    pub fn control_plane_failure(&self) -> Option<&str> {
        self.control_plane_failure.as_deref()
    }

    /// Returns how many inbound frames this replica's validator refused.
    pub const fn refused_frames(&self) -> u64 {
        self.refused_frames
    }

    /// Returns how many inbound frames the driver's own membership refused.
    ///
    /// Counted apart from [`Replica::refused_frames`] because the two diagnose
    /// opposite things. A refusal by the validator says the outer admission
    /// control is working: a principal this deployment does not authorize, or one
    /// beneath the retirement floor its link layer holds, was turned away before
    /// Rafter saw it. A refusal by the membership says the validator and the
    /// group *disagree* — the link layer still authorizes a replica the cluster
    /// has retired, which is the window between a committed removal and the
    /// policy publication whose floor reaches it. One is a quiet network; the
    /// other is a control plane running behind.
    pub const fn non_member_frames(&self) -> u64 {
        self.non_member_frames
    }

    /// Advances one tick.
    ///
    /// # Errors
    ///
    /// Returns an error when the driver refuses the step, which for this
    /// application means the durable backend could not commit a transaction.
    pub fn tick(&mut self) -> Result<(), String> {
        let outcome = self.driver.tick().map_err(|error| error.to_string());
        let finished = self.finish(outcome);
        self.refresh_readiness();
        finished
    }

    /// Starts one deployment-planned membership change.
    ///
    /// Acceptance here is not commitment. The production fixture observes the
    /// committed membership before it retires a replica identity or allocates a
    /// replacement, keeping deployment metadata behind Raft rather than ahead
    /// of it.
    #[allow(
        dead_code,
        reason = "the shared replica module's production binary executes membership changes"
    )]
    pub fn change_membership(&mut self, change: MembershipChange) -> Result<(), String> {
        let outcome = self
            .driver
            .change_membership(change)
            .map_err(|error| error.to_string());
        let finished = self.finish(outcome);
        self.refresh_readiness();
        finished
    }

    /// Returns the complete driver metrics snapshot for structured operations
    /// output in the production fixture.
    #[allow(
        dead_code,
        reason = "the shared replica module's production binary exposes this snapshot"
    )]
    pub fn metrics_snapshot(&self) -> rafter_app::metrics::RaftGroupMetrics<LockGroupId> {
        self.metrics.current()
    }

    /// Returns the committed configuration separately from the effective one.
    #[allow(
        dead_code,
        reason = "the shared replica module's production binary exposes this snapshot"
    )]
    pub fn committed_membership_snapshot(&self) -> Result<rafter::MembershipConfig, String> {
        self.driver
            .with_group(|group| {
                rafter_runtime_api::PersistedRaftRuntime::committed_membership(group.runtime())
            })
            .map_err(|error| error.to_string())
    }

    /// Returns the durable peer-control-plane state and this instance's persist
    /// trigger epoch for production diagnostics and membership orchestration.
    #[allow(
        dead_code,
        reason = "the shared replica module's production binary exposes this snapshot"
    )]
    pub fn control_plane_snapshot(
        &self,
    ) -> (rafter_service::PeerControlPlaneCheckpoint<LockGroupId>, u64) {
        (
            self.driver.control_plane_checkpoint(),
            self.driver.control_plane_checkpoint_epoch(),
        )
    }

    /// Delivers one peer envelope.
    ///
    /// **Two of the three refusals are network events and one is a fault**, and
    /// the middle one used to be treated as the fault. `Rejected` is the
    /// validator turning a frame away and `NotInMembership` is the driver's own
    /// membership doing the same; neither touches the group, and both exist
    /// precisely to describe a late frame from a removed or stale identity — the
    /// ordinary consequence of a link layer that has not caught up with a
    /// committed removal yet. Killing the process for one would take a healthy
    /// replica down because a retired peer was still retransmitting.
    ///
    /// They are counted apart rather than together because they say opposite
    /// things about the deployment; see [`Replica::non_member_frames`]. Only
    /// `Driver` is fatal, and it is fatal for the original reason: the group
    /// failed the step, and a poisoned group serves nothing.
    ///
    /// # Errors
    ///
    /// As [`Replica::tick`].
    pub fn deliver(&mut self, envelope: Inbound) -> Result<(), String> {
        let outcome = match self.driver.deliver(envelope) {
            Ok(()) => Ok(()),
            Err(InboundEnvelopeError::NotInMembership { .. }) => {
                self.non_member_frames = self.non_member_frames.saturating_add(1);
                Ok(())
            }
            // Fatal, and still persisted: a step that poisoned the group had
            // already routed whatever membership the runtime moved through, and
            // that is precisely the fact a restart cannot rebuild.
            Err(InboundEnvelopeError::Driver { source }) => Err(source.to_string()),
            // `Rejected`, and any refusal a later version adds. Both left the
            // group untouched — a variant that had touched it would be `Driver`
            // — so both are counted as the validator's own work rather than made
            // fatal. `InboundEnvelopeError` is `#[non_exhaustive]`.
            Err(_) => {
                self.refused_frames = self.refused_frames.saturating_add(1);
                Ok(())
            }
        };
        let finished = self.finish(outcome);
        self.refresh_readiness();
        finished
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
        let outcome = self
            .driver
            .drive_pending_reads()
            .map_err(|error| error.to_string());
        let finished = self.finish(outcome);
        self.refresh_readiness();
        finished
    }

    /// Starts one replicated command on behalf of a client.
    ///
    /// Started through the driver rather than the client, so this replica holds
    /// the ID the driver allocated from the moment it is allocated rather than
    /// after a poll. The request identity and the outcome classification are
    /// still the application's, so this is the same write
    /// [`LockClient::submit_command`] would have made.
    pub fn submit(
        &mut self,
        ticket: u64,
        command: rafter_reference_fenced_lock::Command,
        deadline: Instant,
    ) {
        self.client_operations = self.client_operations.saturating_add(1);
        let started = self.driver.begin_write(command, write_options(&command));
        // Whatever the write did, and before the early return below: starting a
        // write steps the group, and a step routes every membership fact the
        // group had moved through. The refusal path is not exempt — a driver can
        // refuse a client write *because* a committed removal decommissioned this
        // replica, which is the one refusal whose cause must survive a restart.
        self.finish_without_channel();
        let (local_proposal_id, future) = match started {
            Ok(started) => started,
            Err(error) => {
                self.answers.push(Answer::Submit {
                    ticket,
                    outcome: SubmitOutcome::from_write_error(error),
                });
                return;
            }
        };
        let mut future: SubmitFuture =
            Box::pin(async move { SubmitOutcome::from_write_result(future.await) });
        if let Poll::Ready(outcome) = poll_once(&mut future) {
            self.answers.push(Answer::Submit { ticket, outcome });
            return;
        }
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
    ///
    /// Through the driver, for the reason [`Replica::submit`] gives.
    pub fn query(&mut self, ticket: u64, resource: ResourceName, deadline: Instant) {
        self.client_operations = self.client_operations.saturating_add(1);
        let started = self
            .driver
            .begin_read(LockQuery::GetLock { resource }, ReadOptions::default());
        // As [`Replica::submit`]: starting a barrier steps the group, and the
        // refusal path is where a decommissioned replica's own retirement is
        // observed.
        self.finish_without_channel();
        let (read_id, future) = match started {
            Ok(started) => started,
            Err(error) => {
                self.answers.push(Answer::Query {
                    ticket,
                    outcome: QueryOutcome::Unavailable { error },
                });
                return;
            }
        };
        let future: QueryFuture =
            Box::pin(async move { QueryOutcome::from_read_result(future.await) });
        self.start_query(ticket, deadline, read_id, future);
    }

    /// Answers one `GetLock` from this replica's own applied state.
    ///
    /// [`LockClient::local_lock`] is a weaker level on the *same* read path as a
    /// query rather than a way around it, so this replica keeps the app-layer
    /// refusals that guard a query and gives up only what a local read gives up:
    /// there is no barrier, no quorum round, and no proof. That is why the
    /// protocol keeps it under a separate verb.
    ///
    /// Synchronous because `TransportRaftDriver` answers a local read inside the
    /// call that starts it — no barrier is reserved, so there is nothing this
    /// replica could be waiting for. The client is `async` because that promise
    /// belongs to this driver rather than to every `DriverCommandSender`.
    pub fn local_lock(&self, resource: ResourceName) -> Result<ResourceStatus, String> {
        let client = self.client.clone();
        let mut future: Pin<Box<dyn Future<Output = _>>> =
            Box::pin(async move { client.local_lock(resource).await });
        let Poll::Ready(answered) = poll_once(&mut future) else {
            unreachable!("this driver answers a local read in the call that starts it")
        };
        answered.map_err(|error| error.to_string())
    }

    fn start_query(
        &mut self,
        ticket: u64,
        deadline: Instant,
        read_id: ReadId,
        mut future: QueryFuture,
    ) {
        if let Poll::Ready(outcome) = poll_once(&mut future) {
            self.answers.push(Answer::Query { ticket, outcome });
            return;
        }
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
        // Abandoning a waiter reaches the driver, and this replica does not
        // enumerate which driver calls can flush the peer control plane — it
        // persists after every one of them. The epoch comparison makes the pass
        // that moved nothing free, which is what lets the rule be "always"
        // rather than "where we worked out that it matters".
        self.finish_without_channel();
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
                // driver, not this replica, decides the terminal outcome, and
                // either way the next poll is ready: an abandoned waiter is a
                // resolved one, and a waiter this driver named is one it holds.
                let _ = self.driver.abandon_write(*local_proposal_id);
                let Poll::Ready(outcome) = poll_once(future) else {
                    unreachable!("an abandoned write resolves its client before this returns")
                };
                Some(Answer::Submit {
                    ticket: *ticket,
                    outcome,
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
                let _ = self.driver.abandon_read(*read_id);
                let Poll::Ready(outcome) = poll_once(future) else {
                    unreachable!("an abandoned barrier resolves its client before this returns")
                };
                Some(Answer::Query {
                    ticket: *ticket,
                    outcome,
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
        // The last chance this incarnation has to make its control plane
        // durable, and the process is stopping right after: an obligation left
        // unpersisted here is one the next incarnation never learns about.
        self.finish_without_channel();
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

fn poll_once<T>(future: &mut Pin<Box<dyn Future<Output = T>>>) -> Poll<T> {
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    future.as_mut().poll(&mut context)
}

#[cfg(test)]
mod tests {
    use super::{combine_outcomes, readiness, DriverServiceState, LogIndex, NodeId};

    fn failure(detail: &str) -> Result<(), String> {
        Err(detail.to_owned())
    }

    /// A replica that has caught up and whose driver serves is ready.
    ///
    /// The control. Without it every clause below would pass a readiness rule
    /// that always answered `false`.
    #[test]
    fn a_caught_up_serving_replica_is_ready() {
        assert!(readiness(true, DriverServiceState::Serving));
    }

    /// A replica that has not caught up is not ready, whatever its driver says.
    #[test]
    fn a_replica_behind_its_application_floor_is_not_ready() {
        assert!(!readiness(false, DriverServiceState::Serving));
    }

    /// **No state in which the driver refuses reports ready**, and the applied
    /// floor being reached is exactly what makes the case interesting: the gate
    /// this file keeps is one-way, so `caught_up` stays `true` through every one
    /// of these and used to be the whole answer.
    #[test]
    fn no_refusing_driver_state_reports_ready() {
        for state in [
            DriverServiceState::Decommissioned { node_id: NodeId(1) },
            DriverServiceState::NotMember { node_id: NodeId(1) },
            DriverServiceState::ContradictoryCurrentState {
                through: LogIndex(11),
            },
            DriverServiceState::ContradictoryTransitionPredecessor {
                through: LogIndex(11),
            },
            DriverServiceState::Released,
            DriverServiceState::ShuttingDown,
        ] {
            assert!(
                !readiness(true, state),
                "a replica whose driver answers {state:?} reported itself ready"
            );
        }
    }

    #[test]
    fn a_persist_failure_outranks_the_operations_own() {
        assert_eq!(
            combine_outcomes(failure("persist"), None, failure("operation")),
            failure("persist")
        );
    }

    #[test]
    fn a_carried_persist_failure_outranks_the_operations_own() {
        assert_eq!(
            combine_outcomes(Ok(()), Some("carried".to_owned()), failure("operation")),
            failure("carried")
        );
    }

    #[test]
    fn this_calls_persist_failure_outranks_a_carried_one() {
        assert_eq!(
            combine_outcomes(failure("persist"), Some("carried".to_owned()), Ok(())),
            failure("persist")
        );
    }

    #[test]
    fn the_operations_failure_survives_a_successful_persist() {
        assert_eq!(
            combine_outcomes(Ok(()), None, failure("operation")),
            failure("operation")
        );
    }

    #[test]
    fn everything_succeeding_is_ok() {
        assert_eq!(combine_outcomes(Ok(()), None, Ok(())), Ok(()));
    }
}
