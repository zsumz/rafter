//! Typed failures from journal validation and recovery.

use crate::{DecodeRaftHardStateError, StorageIoError};
use std::{error::Error, fmt, path::PathBuf};

/// Errors returned when opening an append-only hard-state journal.
///
/// This enum is exhaustive so callers can distinguish format and I/O failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OpenJournalRaftHardStateStoreError {
    /// The header is incomplete, corrupt, or belongs to another format/version.
    InvalidHeader,
    /// A complete record failed checksum or canonical-field validation.
    Record {
        /// Byte offset of the rejected record.
        offset: u64,
        /// The underlying hard-state envelope error.
        source: DecodeRaftHardStateError,
    },
    /// Opening, reading, or durably repairing the journal failed.
    Io {
        /// Stable name of the failed operation.
        operation: &'static str,
        /// Journal path.
        path: PathBuf,
        /// Preserved I/O failure.
        source: StorageIoError,
    },
}

impl fmt::Display for OpenJournalRaftHardStateStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidHeader => {
                write!(f, "invalid hard-state journal header (expected RFHJ v1)")
            }
            Self::Record { offset, source } => {
                write!(f, "invalid hard-state journal record at {offset}: {source}")
            }
            Self::Io {
                operation,
                path,
                source,
            } => write!(f, "could not {operation} at {}: {source}", path.display()),
        }
    }
}

impl Error for OpenJournalRaftHardStateStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::InvalidHeader => None,
            Self::Record { source, .. } => Some(source),
            Self::Io { source, .. } => Some(source.as_io_error()),
        }
    }
}
