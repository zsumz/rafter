//! Typed lock-service client over a managed `rafter-service` handle.
//!
//! This is the only path the lock application offers its callers. It exists to
//! keep two promises that a bare handle cannot keep on its own:
//!
//! 1. every *query* runs under [`ReadConsistency::Linearizable`], because the
//!    contract forbids this application from making a lease-read claim; and
//! 2. every write outcome is classified into "committed", "provably not
//!    replicated", or "unknown", because a retry under the same request
//!    identity is only safe once the caller knows which of the three it holds.
//!
//! [`LockClient::local_lock`] is not an exception to the first. It is a
//! different operation with a different return type, so a caller cannot reach a
//! stale answer by passing an argument to the one that promises freshness — and
//! nothing that a fencing decision rests on can be handed one by mistake,
//! because a local read does not produce a [`QueryOutcome`] at all.

use rafter::{LogIndex, Term};
use rafter_app::{proposal::ClientRequestId, read::ReadProof};
use rafter_service::{
    DriverCommandSender, QueryReceipt, RaftHandle, ReadConsistency, ReadError,
    UnknownOutcomeReason, WriteError, WriteOptions, WriteReceipt,
};

use crate::{
    ApplyOutcome, Command, HistoryEvent, LockQuery, LockQueryResult, OperationId, ResourceName,
    ResourceStatus,
};

/// Managed handle specialized to the lock service's command and query types.
pub type LockHandle<G, S> = RaftHandle<G, Command, LockQuery, ApplyOutcome, LockQueryResult, S>;

/// Terminal outcome of one submitted lock command.
///
/// The three variants are the only distinctions a retrying client may act on.
/// A `Refused` command provably never reached the replicated log, so its
/// request identity is still unused. An `Unknown` command may or may not have
/// committed, so the caller must retry the *same* identity and let the session
/// cache decide.
#[derive(Clone, Debug)]
pub enum SubmitOutcome {
    /// The command committed and applied; this is its replicated response.
    Completed {
        index: LogIndex,
        term: Term,
        outcome: ApplyOutcome,
    },
    /// The service refused the command before it could replicate.
    Refused { error: WriteError },
    /// The outcome window closed without a proof either way.
    Unknown { error: WriteError },
}

impl SubmitOutcome {
    /// Classifies a failed write into the two terminal shapes a retrying client
    /// may act on.
    ///
    /// This is the only place the classification is made, so a caller cannot
    /// arrive at a refusal by any route other than the one the driver proved.
    #[must_use]
    pub fn from_write_error(error: WriteError) -> Self {
        if closes_outcome_window(&error) {
            Self::Unknown { error }
        } else {
            Self::Refused { error }
        }
    }

    /// Turns one resolved managed write into this application's outcome.
    ///
    /// The whole mapping, so a caller that started its write through
    /// `TransportRaftDriver::begin_write` — in order to hold the ID the driver
    /// allocated — reaches the same three shapes as one that awaited
    /// [`LockClient::submit_command`]. Two mappings would be two chances for the
    /// history to be told a refusal the cluster never proved.
    #[must_use]
    pub fn from_write_result(result: Result<WriteReceipt<ApplyOutcome>, WriteError>) -> Self {
        match result {
            Ok(receipt) => Self::Completed {
                index: receipt.index,
                term: receipt.term,
                outcome: receipt.result,
            },
            Err(error) => Self::from_write_error(error),
        }
    }

    /// Returns the terminal history event this outcome earns.
    ///
    /// [`HistoryEvent::NotCommitted`] is strictly stronger than
    /// [`HistoryEvent::Unknown`], and `CONTRACT.md` defines exactly which
    /// observations earn it: an attempt that provably never entered the
    /// replicated log. The service layer answers that question as
    /// [`rafter_service::WriteFate::NotAppended`], so this method is the single
    /// joint between the client's three-way classification and the history's
    /// terminal vocabulary. Two independent mappings would be two chances for
    /// the checker to be told a refusal the cluster never proved.
    #[must_use]
    pub const fn history_event(&self, operation_id: OperationId) -> HistoryEvent {
        match self {
            Self::Completed { outcome, .. } => HistoryEvent::Completed {
                operation_id,
                outcome: *outcome,
            },
            Self::Refused { .. } => HistoryEvent::NotCommitted { operation_id },
            Self::Unknown { .. } => HistoryEvent::Unknown { operation_id },
        }
    }

    /// Returns the replicated outcome when the command committed.
    #[must_use]
    pub const fn committed(&self) -> Option<&ApplyOutcome> {
        match self {
            Self::Completed { outcome, .. } => Some(outcome),
            Self::Refused { .. } | Self::Unknown { .. } => None,
        }
    }

    /// Returns whether the caller must retry under the same request identity.
    #[must_use]
    pub const fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown { .. })
    }
}

/// Terminal outcome of one linearizable lock query.
///
/// There is no third variant on purpose: a query either produces an answer that
/// a granted read barrier proved fresh, or it produces no answer at all. A
/// stale answer is never a legal result for this application.
#[derive(Clone, Debug)]
pub enum QueryOutcome<G> {
    /// The barrier was granted and the local replica was fresh enough.
    Answered {
        status: ResourceStatus,
        proof: Option<ReadProof<G>>,
    },
    /// The barrier produced no answer. The caller learns nothing about the
    /// lock, which is the correct result when authority cannot be proved.
    Unavailable { error: ReadError },
}

impl<G> QueryOutcome<G> {
    /// Returns the answered status, if the barrier produced one.
    #[must_use]
    pub const fn status(&self) -> Option<ResourceStatus> {
        match self {
            Self::Answered { status, .. } => Some(*status),
            Self::Unavailable { .. } => None,
        }
    }

    /// Turns one resolved managed read into this application's outcome.
    ///
    /// The query counterpart of [`SubmitOutcome::from_write_result`], and it
    /// exists for the same reason: a caller that started its barrier through
    /// `TransportRaftDriver::begin_read` reaches the same two shapes as one that
    /// awaited [`LockClient::get_lock`].
    #[must_use]
    pub fn from_read_result(result: Result<QueryReceipt<G, LockQueryResult>, ReadError>) -> Self {
        match result {
            Ok(receipt) => Self::Answered {
                status: receipt.result.status(),
                proof: receipt.proof,
            },
            Err(error) => Self::Unavailable { error },
        }
    }
}

/// Application-facing lock client over one managed group handle.
#[derive(Clone, Debug)]
pub struct LockClient<G, S> {
    handle: LockHandle<G, S>,
}

impl<G, S> LockClient<G, S> {
    /// Wraps a managed handle in the lock application's typed surface.
    #[must_use]
    pub const fn new(handle: LockHandle<G, S>) -> Self {
        Self { handle }
    }

    /// Returns the managed handle underneath, for membership and metrics.
    #[must_use]
    pub const fn handle(&self) -> &LockHandle<G, S> {
        &self.handle
    }
}

impl<G, S> LockClient<G, S>
where
    G: Clone + Send + 'static,
    S: DriverCommandSender<G, Command, LockQuery, ApplyOutcome, LockQueryResult>,
{
    /// Submits one replicated command and classifies its outcome.
    ///
    /// Commands arrive already built, envelope and all, because a client that
    /// retries after an unknown outcome has to send back the *exact* bytes it
    /// sent the first time. Rebuilding the envelope here would let a retry
    /// differ from its original in a way the session cache would then reject as
    /// a conflict.
    pub async fn submit_command(&self, command: Command) -> SubmitOutcome {
        let options = write_options(&command);
        SubmitOutcome::from_write_result(self.handle.write_with_options(command, options).await)
    }

    /// Runs `GetLock` behind an ordinary linearizable read barrier.
    ///
    /// The consistency mode is not a parameter. The contract states that this
    /// application makes no lease-read claim, so offering the choice would let
    /// a caller weaken the guarantee the fencing proof depends on.
    pub async fn get_lock(&self, resource: ResourceName) -> QueryOutcome<G> {
        QueryOutcome::from_read_result(
            self.handle
                .read(
                    LockQuery::GetLock { resource },
                    ReadConsistency::Linearizable,
                )
                .await,
        )
    }

    /// Runs `GetLock` against this replica's own applied state.
    ///
    /// A separate operation rather than a consistency argument on
    /// [`LockClient::get_lock`], and the return type is why: [`QueryOutcome`]
    /// means "an answer a granted barrier proved fresh, or no answer", and a
    /// local read proves nothing. It answers a plain status instead, so nothing
    /// routing on authority can mistake one for the other — the fencing proof
    /// rests on the barrier, and this call does not have one.
    ///
    /// It exists because an operator, or a test watching a rejoining replica,
    /// needs to ask what *this* replica holds. `rafter-service` serves it on the
    /// same read path as a linearizable read, so the app-layer refusals that
    /// guard a query — a poisoned group, a state machine below its runtime's
    /// snapshot boundary — guard this one too.
    ///
    /// # Errors
    ///
    /// Returns [`ReadError`] when the driver cannot answer from local state.
    pub async fn local_lock(&self, resource: ResourceName) -> Result<ResourceStatus, ReadError> {
        self.handle
            .read(LockQuery::GetLock { resource }, ReadConsistency::Local)
            .await
            .map(|receipt| receipt.result.status())
    }
}

/// Returns the write options this application submits `command` under.
///
/// Public because a caller that starts its write through the driver — to hold
/// the ID the driver allocated — must submit it under the same identity this
/// client would have. The derivation is here rather than duplicated there.
#[must_use]
pub fn write_options(command: &Command) -> WriteOptions {
    request_metadata(command).map_or_else(WriteOptions::default, |id| {
        WriteOptions::default().with_client_request_id(id)
    })
}

/// Maps the contract's request identity into the service's optional write
/// metadata, so an unknown-outcome error names the identity to retry.
///
/// Rafter never generates this identity and never interprets it. The service
/// layer only echoes it back, which is exactly what a client that lost its
/// outcome needs in order to reconstruct the retry.
fn request_metadata(command: &Command) -> Option<ClientRequestId> {
    match command {
        Command::Submit { request, .. } => Some(ClientRequestId {
            // Both halves of the identity that selects a deduplication slot are
            // packed, because a sequence is only meaningful under its epoch.
            client_id: (u128::from(request.client_id.get()) << 64)
                | u128::from(request.session_epoch.get()),
            sequence: request.sequence.get(),
        }),
        Command::OpenSession { .. } => None,
    }
}

/// Returns whether a write error leaves the commit outcome unknown.
///
/// The fate is the driver's own report of what it observed, and this
/// application must not second-guess it: enumerating the refusing variants here
/// would be a second classification that could disagree with the one the
/// cluster actually proved. [`WriteError`] is `#[non_exhaustive]`, and
/// [`rafter_service::WriteFate::may_commit`] is written as the negation of the
/// refusal, so a variant this build does not recognize reads as unknown — the
/// weaker and therefore safe claim.
fn closes_outcome_window(error: &WriteError) -> bool {
    error.fate().may_commit()
}

/// Returns the diagnostic reason an unknown write outcome carried, if any.
///
/// The reason explains why the service lost the outcome. It never makes the
/// outcome known, and no retry decision may branch on it.
#[must_use]
pub const fn unknown_outcome_reason(error: &WriteError) -> Option<UnknownOutcomeReason> {
    match error {
        WriteError::UnknownOutcome { reason, .. } => Some(*reason),
        _ => None,
    }
}
