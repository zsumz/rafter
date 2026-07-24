//! Typed lock-service client over a managed `rafter-service` handle.
//!
//! This is the only path the lock application offers its callers. It exists to
//! keep two promises that a bare handle cannot keep on its own:
//!
//! 1. every query runs under [`ReadConsistency::Linearizable`], because the
//!    contract forbids this application from making a lease-read claim; and
//! 2. every write outcome is classified into "committed", "provably not
//!    replicated", or "unknown", because a retry under the same request
//!    identity is only safe once the caller knows which of the three it holds.

use rafter::{LogIndex, Term};
use rafter_app::{proposal::ClientRequestId, read::ReadProof};
use rafter_service::{
    DriverCommandSender, RaftHandle, ReadConsistency, ReadError, UnknownOutcomeReason, WriteError,
    WriteOptions,
};

use crate::{ApplyOutcome, Command, LockQuery, LockQueryResult, ResourceName, ResourceStatus};

/// Managed handle specialized to the lock service's command and query types.
pub type LockHandle<G, S> = RaftHandle<G, Command, LockQuery, ApplyOutcome, LockQueryResult, S>;

/// Terminal outcome of one submitted lock command.
///
/// The three variants are the only distinctions a retrying client may act on.
/// A `Refused` command provably never reached the replicated log, so its
/// request identity is still unused. An `Unknown` command may or may not have
/// committed, so the caller must retry the *same* identity and let the session
/// cache decide.
#[derive(Clone, Debug, Eq, PartialEq)]
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
#[derive(Clone, Debug, Eq, PartialEq)]
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
        let options = WriteOptions {
            client_request_id: request_metadata(&command),
        };
        match self.handle.write_with_options(command, options).await {
            Ok(receipt) => SubmitOutcome::Completed {
                index: receipt.index,
                term: receipt.term,
                outcome: receipt.result,
            },
            Err(error) if closes_outcome_window(&error) => SubmitOutcome::Unknown { error },
            Err(error) => SubmitOutcome::Refused { error },
        }
    }

    /// Runs `GetLock` behind an ordinary linearizable read barrier.
    ///
    /// The consistency mode is not a parameter. The contract states that this
    /// application makes no lease-read claim, so offering the choice would let
    /// a caller weaken the guarantee the fencing proof depends on.
    pub async fn get_lock(&self, resource: ResourceName) -> QueryOutcome<G> {
        match self
            .handle
            .read(
                LockQuery::GetLock { resource },
                ReadConsistency::Linearizable,
            )
            .await
        {
            Ok(receipt) => QueryOutcome::Answered {
                status: receipt.result.status(),
                proof: receipt.proof,
            },
            Err(error) => QueryOutcome::Unavailable { error },
        }
    }
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
/// Only the errors that prove the command never entered the replicated log are
/// refusals; everything else has to be retried under the same request identity.
/// The listed variants are the complete set that proves non-replication today.
/// Everything else falls through, including `UnknownOutcome` and the ambiguous
/// apply/storage/transport/poison errors, which carry only a formatted `String`
/// and so cannot be inspected to narrow the window further. `WriteError` is
/// also `#[non_exhaustive]`, so a future variant must default to the safe
/// answer rather than to a refusal.
fn closes_outcome_window(error: &WriteError) -> bool {
    !matches!(
        error,
        WriteError::NotLeader { .. }
            | WriteError::Rejected { .. }
            | WriteError::PayloadTooLarge { .. }
            | WriteError::ShuttingDown
            | WriteError::LocalProposalIdExhausted
    )
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
