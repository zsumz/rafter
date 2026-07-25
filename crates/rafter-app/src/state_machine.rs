//! Synchronous replicated state-machine traits and apply types.
//!
//! The state machine layer defines command encoding, batch apply,
//! applied-index durability, reads under barriers, and application snapshots.

use std::{error::Error, fmt};

use rafter::{LocalProposalId, LogIndex, RaftSnapshot, Term};

/// Whether a state machine implements application snapshots.
///
/// A state machine that cannot install a snapshot cannot rejoin the cluster
/// after falling behind the leader's compacted log prefix, so this is a
/// statement about replication capability rather than about an application
/// feature.
#[non_exhaustive]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SnapshotSupport {
    /// The state machine builds and installs application snapshots, and both
    /// [`ReplicatedStateMachine::build_snapshot`] and
    /// [`ReplicatedStateMachine::install_snapshot`] are implemented.
    Supported,
    /// The state machine has no snapshot representation.
    ///
    /// A group over such a state machine refuses a Raft-driven install before
    /// the state machine is touched, and poisons — a replica that cannot
    /// install the snapshot it was sent has no way forward, and pretending
    /// otherwise is how an empty payload gets reported as an applied index.
    ///
    /// This is a development state, not a deployment one. A durable
    /// application declares [`SnapshotSupport::Supported`]; nothing here makes
    /// snapshots optional for one, and a durability test that admits an
    /// `Unsupported` state machine as evidence is testing something else.
    Unsupported,
}

/// Failure of an application snapshot operation.
///
/// The refusal is part of the trait's vocabulary rather than the
/// application's, so a state machine that has no snapshot format does not have
/// to invent an error variant that reads as a fault.
#[derive(Debug)]
#[non_exhaustive]
pub enum ApplicationSnapshotError<E> {
    /// The state machine declared [`SnapshotSupport::Unsupported`].
    Unsupported,
    /// The state machine failed to build or install the snapshot.
    StateMachine(E),
}

impl<E> From<E> for ApplicationSnapshotError<E> {
    fn from(error: E) -> Self {
        Self::StateMachine(error)
    }
}

impl<E> fmt::Display for ApplicationSnapshotError<E>
where
    E: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported => {
                formatter.write_str("state machine declares no application snapshot support")
            }
            Self::StateMachine(error) => {
                write!(formatter, "application snapshot operation failed: {error}")
            }
        }
    }
}

impl<E> Error for ApplicationSnapshotError<E>
where
    E: Error + 'static,
{
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Unsupported => None,
            Self::StateMachine(error) => Some(error),
        }
    }
}

/// A synchronous replicated state machine driven by committed Raft entries.
///
/// Implementations are responsible for their own durable application storage.
/// After [`ReplicatedStateMachine::apply_batch`] returns success, the state
/// machine must be able to recover with all effects through the highest
/// returned applied index. A state machine that cannot persist application
/// effects and applied-index progress together strongly enough for that
/// guarantee should not be used with the higher-level group or service APIs.
pub trait ReplicatedStateMachine {
    type Command;
    type CommandResult;
    type Query;
    type QueryResult;
    /// Error returned when this state machine cannot encode, decode, apply,
    /// read, or snapshot.
    ///
    /// Application errors are part of the public app/service error stack, so
    /// implementations expose typed errors rather than debug-only strings —
    /// the same contract
    /// [`rafter_runtime_api::PersistedRaftRuntime::Error`] already states for
    /// the other half of that stack. Without the bound,
    /// [`crate::error::GroupError`] is a [`std::error::Error`] for some state
    /// machines and not others, and every layer above has to render rather than
    /// preserve.
    ///
    /// `Send + Sync` is required because a managed service resolves a client
    /// waiter on a different task from the one that stepped the group.
    type Error: Error + Send + Sync + 'static;

    /// Whether this state machine implements application snapshots.
    ///
    /// There is no default. Every implementor states this, because "this
    /// application has no snapshot format yet" and "this application does not
    /// need snapshots" are different claims and only the implementor can make
    /// either one — a default would make the claim on their behalf, and it
    /// would be wrong for whichever of the two they meant.
    ///
    /// A state machine that declares [`SnapshotSupport::Supported`] must
    /// implement both snapshot methods. Inheriting a provided body while
    /// declaring support is a contract violation, and a group detects it: the
    /// provided body returns [`ApplicationSnapshotError::Unsupported`], which
    /// contradicts the declaration and poisons the group with a distinct
    /// error rather than a generic install failure.
    const SNAPSHOT_SUPPORT: SnapshotSupport;

    /// Returns the highest Raft log index whose application effects are
    /// durably reflected in this state machine.
    ///
    /// # Errors
    ///
    /// Returns an application error when the state machine cannot determine
    /// its durable applied index.
    fn applied_index(&self) -> Result<LogIndex, Self::Error>;

    /// Encodes an application command into the opaque Raft log payload.
    ///
    /// # Errors
    ///
    /// Returns an application error when the command cannot be encoded into a
    /// deterministic replicated payload.
    fn encode_command(&self, command: &Self::Command) -> Result<Vec<u8>, Self::Error>;

    /// Decodes an opaque Raft log payload back into an application command.
    ///
    /// # Errors
    ///
    /// Returns an application error when the payload is malformed or otherwise
    /// cannot be decoded by this state machine.
    fn decode_command(&self, payload: &[u8]) -> Result<Self::Command, Self::Error>;

    /// Applies committed commands in order.
    ///
    /// The returned vector must have the same length and order as
    /// [`ApplyBatch::entries`]. Each returned index must represent effects
    /// that are durable together with the state machine's applied index before
    /// this method returns `Ok`.
    ///
    /// # Errors
    ///
    /// Returns an application error when any committed command cannot be
    /// applied durably. Callers must treat this as fatal for the group unless
    /// the application provides an explicit repair path.
    fn apply_batch(
        &mut self,
        batch: ApplyBatch<Self::Command>,
    ) -> Result<Vec<ApplyResult<Self::CommandResult>>, Self::Error>;

    /// Serves a query under a caller-provided read barrier.
    ///
    /// # Errors
    ///
    /// Returns an application error when the query cannot be evaluated against
    /// the current state-machine state.
    fn read(
        &self,
        query: Self::Query,
        barrier: ReadBarrier,
    ) -> Result<Self::QueryResult, Self::Error>;

    /// Builds an application snapshot at `at`.
    ///
    /// Rafter never calls this. Snapshot creation is caller-driven: an
    /// embedder decides when to compact, calls this method, and passes the
    /// result to its runtime's compaction API — `DurableRaftNode`'s
    /// `compact_log_with_snapshot` in the `rafter-runtime` crate is the shipped
    /// one. `at` must be the state machine's own applied index; compacting above it
    /// raises the group's committed application index past a value the state
    /// machine will ever report.
    ///
    /// # Errors
    ///
    /// Returns [`ApplicationSnapshotError::Unsupported`] when this state
    /// machine declared [`SnapshotSupport::Unsupported`], and
    /// [`ApplicationSnapshotError::StateMachine`] when snapshot construction
    /// or persistence fails.
    fn build_snapshot(
        &mut self,
        at: LogIndex,
    ) -> Result<ApplicationSnapshot, ApplicationSnapshotError<Self::Error>> {
        let _ = at;
        Err(ApplicationSnapshotError::Unsupported)
    }

    /// Installs an application snapshot and makes its effects durable.
    ///
    /// Rafter calls this when the local node accepts a leader's snapshot. After
    /// it returns `Ok`, the state machine must be able to recover with all
    /// snapshot effects and applied-index progress through the installed
    /// snapshot boundary; returning `Ok` without incorporating the payload
    /// reports an applied index the state machine does not reflect, and every
    /// later read and every readiness gate believes it. When
    /// [`ApplicationSnapshot::raft_snapshot`] is present, the snapshot payload
    /// may be managed by the runtime snapshot store instead of carried inline
    /// in [`ApplicationSnapshot::payload`].
    ///
    /// # Errors
    ///
    /// Returns [`ApplicationSnapshotError::Unsupported`] when this state
    /// machine declared [`SnapshotSupport::Unsupported`], and
    /// [`ApplicationSnapshotError::StateMachine`] when the snapshot is invalid
    /// for this state machine or cannot be installed durably.
    fn install_snapshot(
        &mut self,
        snapshot: ApplicationSnapshot,
    ) -> Result<(), ApplicationSnapshotError<Self::Error>> {
        let _ = snapshot;
        Err(ApplicationSnapshotError::Unsupported)
    }
}

/// A batch of committed commands ready for ordered application.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyBatch<C> {
    pub entries: Vec<ApplyEntry<C>>,
}

/// A single committed command in an application batch.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyEntry<C> {
    pub index: LogIndex,
    pub term: Term,
    pub command: C,
    pub local_proposal_id: Option<LocalProposalId>,
}

/// The application result for one applied command.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplyResult<R> {
    pub index: LogIndex,
    pub term: Term,
    pub result: R,
    pub local_proposal_id: Option<LocalProposalId>,
}

/// A barrier proving the local state machine is fresh enough for a read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReadBarrier {
    pub required_applied_index: LogIndex,
    pub local_applied_index: LogIndex,
}

/// Application snapshot data and the applied index it covers.
///
/// `payload` carries inline application bytes for snapshots built by the
/// state machine. During Raft-driven installs, the runtime may have already
/// promoted the staged snapshot bytes into its snapshot store; in that case
/// `raft_snapshot` identifies the installed Raft snapshot and `payload` may
/// be empty.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApplicationSnapshot {
    pub applied_index: LogIndex,
    pub payload: Vec<u8>,
    pub raft_snapshot: Option<RaftSnapshot>,
}
