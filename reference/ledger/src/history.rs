use crate::{Command, LedgerQuery, LedgerQueryResult, LedgerResponse};

/// Stable identifier for one client-visible operation in an observed history.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct OperationId(u64);

impl OperationId {
    /// Creates an operation identifier.
    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    /// Returns the numeric identifier.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// A client-visible event retained for later history checking.
///
/// A history is the recorded sequence of these events. Position in that
/// sequence *is* the real-time order: an operation whose terminal event appears
/// before another operation's invocation happened before it, and two operations
/// whose intervals overlap may be linearized in either order. Every operation
/// therefore contributes exactly one invocation event and exactly one terminal
/// event, correlated by [`OperationId`].
///
/// The vocabulary is closed and in-memory. It is deliberately not a wire
/// format: the versioned frames this crate defines are the replicated command
/// and snapshot frames in the adapter's codec, and a history never crosses a
/// process boundary. Adding an outcome here is a contract change recorded in
/// `CONTRACT.md`, not a compatibility negotiation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HistoryEvent {
    /// The client invoked a replicated command.
    Invoked {
        /// Operation identity unique within the history.
        operation_id: OperationId,
        /// Exact command issued by the client.
        command: Command,
    },
    /// The client observed a terminal response, including deterministic
    /// rejection responses.
    Completed {
        /// Operation identity from the matching invocation.
        operation_id: OperationId,
        /// Response observed by the client.
        response: LedgerResponse,
    },
    /// The connection or process failed without revealing whether the command
    /// committed.
    ///
    /// The command may or may not have taken effect, so a checker must consider
    /// both. The client must retry the same request identity.
    Unknown {
        /// Operation identity from the matching invocation.
        operation_id: OperationId,
    },
    /// The command provably never entered the replicated log.
    ///
    /// This is strictly stronger than [`HistoryEvent::Unknown`]: it asserts
    /// that no copy of this attempt can commit later, so a checker must
    /// linearize it as never having happened. `CONTRACT.md` defines exactly
    /// which observations earn it; every other lost outcome stays `Unknown`.
    NotCommitted {
        /// Operation identity from the matching invocation.
        operation_id: OperationId,
    },
    /// The client invoked a linearizable query.
    QueryInvoked {
        /// Operation identity unique within the history.
        operation_id: OperationId,
        /// Exact query issued by the client.
        query: LedgerQuery,
    },
    /// The client observed a query result.
    QueryCompleted {
        /// Operation identity from the matching invocation.
        operation_id: OperationId,
        /// Result observed by the client.
        result: LedgerQueryResult,
    },
    /// The query ended without returning a value to the client.
    ///
    /// A refused barrier, a canceled barrier, and a client that stopped waiting
    /// are indistinguishable here on purpose: none of them delivered a result,
    /// and a query that returned nothing constrains no ordering.
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
}
