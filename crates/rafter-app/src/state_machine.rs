//! Synchronous replicated state-machine traits and apply types.
//!
//! The state machine layer defines command encoding, batch apply,
//! applied-index durability, reads under barriers, and application snapshots.

use rafter::{LocalProposalId, LogIndex, RaftSnapshot, Term};

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
    type Error;

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
    /// # Errors
    ///
    /// Returns an application error when snapshot construction or persistence
    /// fails.
    fn build_snapshot(&mut self, at: LogIndex) -> Result<ApplicationSnapshot, Self::Error>;

    /// Installs an application snapshot and makes its effects durable.
    ///
    /// After this returns `Ok`, the state machine must be able to recover with
    /// all snapshot effects and applied-index progress through the installed
    /// snapshot boundary. When [`ApplicationSnapshot::raft_snapshot`] is
    /// present, the snapshot payload may be managed by the runtime snapshot
    /// store instead of carried inline in [`ApplicationSnapshot::payload`].
    ///
    /// # Errors
    ///
    /// Returns an application error when the snapshot is invalid for this state
    /// machine or cannot be installed durably.
    fn install_snapshot(&mut self, snapshot: ApplicationSnapshot) -> Result<(), Self::Error>;
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
