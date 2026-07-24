use crate::{Command, LedgerResponse};

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
#[derive(Clone, Debug, Eq, PartialEq)]
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
        response: LedgerResponse,
    },
    /// The connection or process failed without revealing whether the command
    /// committed.
    Unknown {
        /// Operation identity from the matching invocation.
        operation_id: OperationId,
    },
}
