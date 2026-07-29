//! Hard-state open and mutation error vocabulary.
//!
//! These errors distinguish corrupt persisted bytes, filesystem failures, and
//! handles whose post-error durable outcome must be reconstructed by reopening.

use std::{error::Error, fmt, path::PathBuf};

use crate::{DecodeRaftHardStateError, StorageIoError};

/// Errors returned while durably writing Raft hard state.
///
/// This enum is exhaustive because a write currently fails through the
/// underlying filesystem or because an earlier I/O failure made the handle's
/// cached state unsafe to reuse.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftHardStateStoreWriteError {
    /// A filesystem operation failed. The file-backed handle now requires a
    /// fresh [`super::FileRaftHardStateStore::open`] before another mutation.
    Io {
        /// Stable name of the failed filesystem operation.
        operation: &'static str,
        /// Path on which the operation failed.
        path: PathBuf,
        /// Preserved I/O failure.
        source: StorageIoError,
    },
    /// An earlier mutating I/O failure poisoned this file-backed handle.
    StoreRequiresReopen,
}

/// Errors returned while opening a Raft hard-state store.
///
/// This enum is exhaustive so callers can distinguish I/O from corrupt
/// persisted bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OpenRaftHardStateStoreError {
    /// A filesystem operation failed while opening or publishing initial state.
    Io {
        /// Stable name of the failed filesystem operation.
        operation: &'static str,
        /// Path on which the operation failed.
        path: PathBuf,
        /// Preserved I/O failure.
        source: StorageIoError,
    },
    /// The persisted hard-state envelope was corrupt or unsupported.
    Decode(DecodeRaftHardStateError),
}

impl fmt::Display for RaftHardStateStoreWriteError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "could not {operation} at {}: {source}",
                path.display()
            ),
            Self::StoreRequiresReopen => formatter
                .write_str("Raft hard-state store requires reopen after an earlier I/O failure"),
        }
    }
}

impl Error for RaftHardStateStoreWriteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source.as_io_error()),
            Self::StoreRequiresReopen => None,
        }
    }
}

impl fmt::Display for OpenRaftHardStateStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io {
                operation,
                path,
                source,
            } => write!(
                formatter,
                "could not {operation} at {}: {source}",
                path.display()
            ),
            Self::Decode(error) => {
                write!(formatter, "stored Raft hard state is corrupt: {error}")
            }
        }
    }
}

impl Error for OpenRaftHardStateStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decode(error) => Some(error),
            Self::Io { source, .. } => Some(source.as_io_error()),
        }
    }
}
