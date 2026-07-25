use crate::{Command, LockResponse, RequestIdentity};

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
/// A history is the recorded sequence of these events, and every operation
/// contributes exactly one invocation event and exactly one terminal event,
/// correlated by [`OperationId`]. The three terminal outcomes differ only in
/// what the caller can prove, which is why they are distinct events rather than
/// one lost-outcome event: a checker must allow a [`HistoryEvent::Unknown`]
/// operation to have taken effect, so recording a provable refusal as unknown
/// would explain away an implementation that minted a token for a command the
/// cluster refused.
///
/// This crate records histories and asserts against them directly; it has no
/// linearizability checker of its own, and the real-time ordering property is
/// left to the process adapter that `CONTRACT.md` describes.
///
/// The vocabulary is closed and in-memory. It is deliberately not a wire
/// format: the versioned frames this crate defines are the replicated command
/// and snapshot frames in the adapter's codec, and a history never crosses a
/// process boundary. Adding an outcome here is a contract change recorded in
/// `CONTRACT.md`, not a compatibility negotiation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryEvent {
    /// The client invoked a command.
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
        response: LockResponse,
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
}

impl HistoryEvent {
    /// Returns the operation identity this event belongs to.
    #[must_use]
    pub const fn operation_id(&self) -> OperationId {
        match *self {
            Self::Invoked { operation_id, .. }
            | Self::Completed { operation_id, .. }
            | Self::Unknown { operation_id }
            | Self::NotCommitted { operation_id } => operation_id,
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
