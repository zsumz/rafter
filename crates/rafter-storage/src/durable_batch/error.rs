//! Atomic batch failures preserve ambiguous I/O and require reopening.
use crate::StorageIoError;
use std::{error::Error, fmt};

/// Failure to publish an atomic Raft persistence batch.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RaftPersistenceBatchError {
    /// The handles do not belong to the same live coordinator.
    DomainMismatch,
    /// The batch cannot be applied to the acknowledged log geometry.
    InvalidBatch(&'static str),
    /// An earlier mutating I/O failure requires a fresh open.
    StoreRequiresReopen,
    /// A write or sync failed; some or all record bytes may have reached storage.
    Io {
        /// Operation attempted by the coordinator.
        operation: &'static str,
        /// Original filesystem error.
        source: StorageIoError,
    },
}
impl fmt::Display for RaftPersistenceBatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DomainMismatch => f.write_str("Raft persistence domains differ"),
            Self::InvalidBatch(reason) => write!(f, "invalid Raft persistence batch: {reason}"),
            Self::StoreRequiresReopen => {
                f.write_str("Raft WAL requires reopen after an I/O failure")
            }
            Self::Io { operation, source } => write!(f, "could not {operation}: {source}"),
        }
    }
}
impl Error for RaftPersistenceBatchError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source.as_io_error()),
            _ => None,
        }
    }
}
