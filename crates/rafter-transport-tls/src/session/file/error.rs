//! Typed file-backed session-store failures.

use std::{error::Error, fmt, io, path::PathBuf};

use crate::{ClusterId, PeerId};

use super::super::{
    DecodeTransportSessionStateError, EncodeTransportSessionStateError, SessionStateError,
};

/// Failure while creating brand-new durable transport session state.
#[derive(Debug)]
#[non_exhaustive]
pub enum CreateTransportSessionStoreError {
    /// The state path already exists and must not be overwritten.
    AlreadyExists {
        /// Existing state path.
        path: PathBuf,
    },
    /// Another cooperating process or handle owns the state path.
    AlreadyOpen {
        /// State path whose ownership lock is held.
        path: PathBuf,
    },
    /// Initial state could not be encoded.
    Encode {
        /// Session-state encoding failure.
        source: EncodeTransportSessionStateError,
    },
    /// A filesystem operation failed.
    Io {
        /// Operation that failed.
        operation: &'static str,
        /// Path involved in the operation.
        path: PathBuf,
        /// Original filesystem error.
        source: io::Error,
    },
}

impl fmt::Display for CreateTransportSessionStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyExists { path } => write!(
                formatter,
                "transport session state {} already exists",
                path.display()
            ),
            Self::AlreadyOpen { path } => write!(
                formatter,
                "transport session state {} is already open",
                path.display()
            ),
            Self::Encode { source } => write!(
                formatter,
                "could not encode initial transport session state: {source}"
            ),
            Self::Io {
                operation,
                path,
                source,
            } => write!(formatter, "{operation} {}: {source}", path.display()),
        }
    }
}

impl Error for CreateTransportSessionStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Encode { source } => Some(source),
            Self::Io { source, .. } => Some(source),
            Self::AlreadyExists { .. } | Self::AlreadyOpen { .. } => None,
        }
    }
}

/// Failure while opening previously created durable transport session state.
#[derive(Debug)]
#[non_exhaustive]
pub enum OpenTransportSessionStoreError {
    /// The state file is absent; reopening never silently initializes it.
    Missing {
        /// Missing state path.
        path: PathBuf,
    },
    /// Another cooperating process or handle owns the state path.
    AlreadyOpen {
        /// State path whose ownership lock is held.
        path: PathBuf,
    },
    /// File size exceeded the absolute version-1 bound before allocation.
    FileTooLarge {
        /// State path.
        path: PathBuf,
        /// Bytes observed up to the bounded read limit.
        actual: usize,
        /// Largest accepted version-1 file.
        maximum: usize,
    },
    /// Existing bytes were malformed, corrupt, or noncanonical.
    Decode {
        /// Session-state decoding failure.
        source: DecodeTransportSessionStateError,
    },
    /// The file belongs to another deployment boundary.
    ClusterMismatch {
        /// Cluster expected by the caller.
        expected: ClusterId,
        /// Cluster recorded durably.
        actual: ClusterId,
    },
    /// The file belongs to another local authenticated principal.
    LocalPeerMismatch {
        /// Local peer expected by the caller.
        expected: PeerId,
        /// Local peer recorded durably.
        actual: PeerId,
    },
    /// A filesystem operation failed.
    Io {
        /// Operation that failed.
        operation: &'static str,
        /// Path involved in the operation.
        path: PathBuf,
        /// Original filesystem error.
        source: io::Error,
    },
}

impl fmt::Display for OpenTransportSessionStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Missing { path } => write!(
                formatter,
                "transport session state {} does not exist",
                path.display()
            ),
            Self::AlreadyOpen { path } => write!(
                formatter,
                "transport session state {} is already open",
                path.display()
            ),
            Self::FileTooLarge {
                path,
                actual,
                maximum,
            } => write!(
                formatter,
                "transport session state {} is at least {actual} bytes, \
                 exceeding maximum {maximum}",
                path.display()
            ),
            Self::Decode { source } => {
                write!(
                    formatter,
                    "could not decode transport session state: {source}"
                )
            }
            Self::ClusterMismatch { expected, actual } => write!(
                formatter,
                "transport session state belongs to cluster {actual}, \
                 not expected cluster {expected}"
            ),
            Self::LocalPeerMismatch { expected, actual } => write!(
                formatter,
                "transport session state belongs to local peer {actual}, \
                 not expected peer {expected}"
            ),
            Self::Io {
                operation,
                path,
                source,
            } => write!(formatter, "{operation} {}: {source}", path.display()),
        }
    }
}

impl Error for OpenTransportSessionStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decode { source } => Some(source),
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Runtime failure from an open file-backed transport session store.
#[derive(Debug)]
#[non_exhaustive]
pub enum FileTransportSessionStoreError {
    /// A prior ambiguous publication failure requires dropping and reopening.
    StoreRequiresReopen,
    /// The requested transition violated finite or monotonic session state.
    State {
        /// Pure state transition failure.
        source: SessionStateError,
    },
    /// Candidate state could not be encoded before publication began.
    Encode {
        /// Session-state encoding failure.
        source: EncodeTransportSessionStateError,
    },
    /// A mutating filesystem operation failed and latched terminal failure.
    Io {
        /// Operation that failed.
        operation: &'static str,
        /// Path involved in the operation.
        path: PathBuf,
        /// Original filesystem error.
        source: io::Error,
    },
}

impl fmt::Display for FileTransportSessionStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StoreRequiresReopen => formatter
                .write_str("transport session store failed publication and must be reopened"),
            Self::State { source } => write!(formatter, "session transition refused: {source}"),
            Self::Encode { source } => {
                write!(
                    formatter,
                    "could not encode transport session state: {source}"
                )
            }
            Self::Io {
                operation,
                path,
                source,
            } => write!(formatter, "{operation} {}: {source}", path.display()),
        }
    }
}

impl Error for FileTransportSessionStoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::State { source } => Some(source),
            Self::Encode { source } => Some(source),
            Self::Io { source, .. } => Some(source),
            Self::StoreRequiresReopen => None,
        }
    }
}
