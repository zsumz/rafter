use crate::{ApplyOutcome, Command, LockQuery, LockQueryResult, RequestIdentity};

pub use rafter_reference_harness::OperationId;

/// A client-visible event retained for later history checking.
///
/// A history is the recorded sequence of these events. Position in that
/// sequence *is* real-time order: an operation whose terminal event appears
/// before another operation's invocation happened before it, while overlapping
/// intervals may be linearized in either order. Every operation contributes
/// exactly one invocation and one terminal event, correlated by
/// [`OperationId`].
///
/// Mutation terminals preserve both the stable response and its
/// [`crate::ApplyDisposition`]. The distinction is load-bearing for exact
/// retries: a replay must not be explained as a fresh execution. Queries use
/// their own invocation and terminal variants so a mutation can never satisfy
/// a query interval or vice versa.
///
/// The vocabulary is closed and in-memory. It is deliberately not a wire
/// format: the versioned frames this crate defines are the replicated command
/// and snapshot frames in the adapter's codec, and a history never crosses a
/// process boundary. Adding an outcome here is a contract change recorded in
/// `CONTRACT.md`, not a compatibility negotiation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryEvent {
    /// The client invoked a replicated command.
    Invoked {
        /// Operation identity unique within the history.
        operation_id: OperationId,
        /// Exact command issued by the client.
        command: Command,
    },
    /// The client observed a terminal replicated outcome, including
    /// deterministic rejection responses.
    Completed {
        /// Operation identity from the matching invocation.
        operation_id: OperationId,
        /// Exact response and apply disposition observed by the client.
        outcome: ApplyOutcome,
    },
    /// The connection or process failed without revealing whether the command
    /// committed.
    ///
    /// The command may or may not have taken effect, so the client must retry
    /// the *same* request identity and let the session cache decide.
    Unknown {
        /// Operation identity from the matching invocation.
        operation_id: OperationId,
    },
    /// The command provably never entered the replicated log.
    ///
    /// This is strictly stronger than [`HistoryEvent::Unknown`]: no copy of the
    /// attempt can commit later, so it minted no fencing token, consumed no
    /// sequence, and left its request identity free for a fresh attempt. A
    /// checker must linearize it as never having happened. `CONTRACT.md`
    /// defines exactly which observations earn it; every other lost outcome
    /// stays `Unknown`.
    NotCommitted {
        /// Operation identity from the matching invocation.
        operation_id: OperationId,
    },
    /// The client invoked a linearizable query.
    QueryInvoked {
        /// Operation identity unique within the history.
        operation_id: OperationId,
        /// Exact query issued by the client.
        query: LockQuery,
    },
    /// The client observed a query result.
    QueryCompleted {
        /// Operation identity from the matching invocation.
        operation_id: OperationId,
        /// Result observed by the client.
        result: LockQueryResult,
    },
    /// The query ended without returning a value to the client.
    ///
    /// A refused barrier, canceled barrier, connection loss, and a caller that
    /// stopped waiting all constrain no sequential value because none delivered
    /// one.
    QueryAbandoned {
        /// Operation identity from the matching invocation.
        operation_id: OperationId,
    },
}

impl HistoryEvent {
    /// Returns the operation identity this event belongs to.
    #[must_use]
    pub const fn operation_id(&self) -> OperationId {
        match *self {
            Self::Invoked { operation_id, .. }
            | Self::Completed { operation_id, .. }
            | Self::Unknown { operation_id }
            | Self::NotCommitted { operation_id }
            | Self::QueryInvoked { operation_id, .. }
            | Self::QueryCompleted { operation_id, .. }
            | Self::QueryAbandoned { operation_id } => operation_id,
        }
    }

    /// Returns the request identity an invocation carried, when it carried one.
    ///
    /// Retries share a request identity, so a checker can group every attempt
    /// at one replicated effect from the history alone.
    #[must_use]
    pub const fn request_identity(&self) -> Option<RequestIdentity> {
        match *self {
            Self::Invoked {
                command: Command::Submit { request, .. },
                ..
            } => Some(request),
            _ => None,
        }
    }
}
