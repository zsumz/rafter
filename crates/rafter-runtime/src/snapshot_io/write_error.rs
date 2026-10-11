//! Typed refusals from bounded application snapshot encoding.

use rafter::LogIndex;
use std::{error::Error, fmt, io, sync::Arc};

/// Why a staging writer could not complete a snapshot payload.
///
/// `Write` wraps this type in an `io::Error`. Except for an underlying
/// `Interrupted` error, every refusal fences the attempt permanently. Sink
/// errors preserve their original cause, including custom error payloads.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum SnapshotWriteError {
    /// A zero chunk size cannot make progress.
    ZeroChunkSize,
    /// Snapshot creation must capture exactly the application's durable floor.
    AppliedFloorMismatch {
        /// Index named by the requested snapshot metadata.
        snapshot: LogIndex,
        /// Durable application index supplied by the embedding.
        applied: LogIndex,
    },
    /// The complete buffer passed to `write` would exceed the total budget.
    PayloadLimit {
        /// Bytes previously accepted by the sink.
        written: u64,
        /// Bytes in the refused caller buffer.
        attempted: usize,
        /// Maximum total payload length.
        limit: u64,
    },
    /// Cancellation was requested before completion was accepted.
    Cancelled,
    /// A sink call unwound; a caught panic cannot leave a reusable writer.
    SinkInterrupted {
        /// Last known accepted payload length before the call.
        written: u64,
    },
    /// The sink refused a write or flush operation.
    Sink {
        /// Last known accepted payload length before the call.
        written: u64,
        /// Original sink error; `Interrupted` permits a retry.
        error: Arc<io::Error>,
    },
    /// The sink made no progress on a nonempty write.
    WriteZero {
        /// Last known accepted payload length.
        written: u64,
    },
    /// The sink violated `Write` by claiming more bytes than it was offered.
    InvalidWriteCount {
        /// Maximum count offered to this sink call.
        offered: usize,
        /// Count incorrectly reported as written.
        reported: usize,
    },
}

impl SnapshotWriteError {
    pub(super) fn retryable(&self) -> bool {
        matches!(self, Self::Sink { error, .. } if error.kind() == io::ErrorKind::Interrupted)
    }

    pub(super) fn into_io(self) -> io::Error {
        // Only a genuine retryable sink interruption can trigger standard
        // write_all's retry loop. Cancellation and sticky failures must stop it.
        let kind = match &self {
            Self::Sink { error, .. } => error.kind(),
            Self::WriteZero { .. } => io::ErrorKind::WriteZero,
            _ => io::ErrorKind::Other,
        };
        io::Error::new(kind, self)
    }
}

impl fmt::Display for SnapshotWriteError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroChunkSize => write!(f, "snapshot write chunk size must be nonzero"),
            Self::AppliedFloorMismatch { snapshot, applied } => write!(
                f,
                "snapshot index {snapshot} does not match application floor {applied}"
            ),
            Self::PayloadLimit {
                written,
                attempted,
                limit,
            } => write!(
                f,
                "snapshot write of {attempted} bytes after {written} exceeds limit {limit}"
            ),
            Self::Cancelled => write!(f, "snapshot payload write cancelled"),
            Self::SinkInterrupted { written } => {
                write!(f, "snapshot sink call at byte {written} did not return")
            }
            Self::Sink { written, error } => {
                write!(f, "snapshot sink failed at byte {written}: {error}")
            }
            Self::WriteZero { written } => {
                write!(f, "snapshot sink made no progress at byte {written}")
            }
            Self::InvalidWriteCount { offered, reported } => write!(
                f,
                "snapshot sink reported {reported} written bytes after receiving {offered}"
            ),
        }
    }
}

impl Error for SnapshotWriteError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Sink { error, .. } => Some(error.as_ref()),
            _ => None,
        }
    }
}
