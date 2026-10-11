//! Typed, sticky refusals while reading a snapshot payload.

use rafter::LogIndex;
use std::{error::Error, fmt};

/// Why a snapshot payload could not be accepted.
///
/// `std::io::Read` wraps this error in `std::io::Error`; `finish` returns it
/// directly. After any read refusal the reader cannot be resumed or verified.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SnapshotReadError {
    /// A chunk size of zero cannot make progress.
    ZeroChunkSize,
    /// The descriptor exceeds the embedding's total-payload admission limit.
    PayloadLimit {
        /// Length declared by the descriptor.
        declared: u64,
        /// Maximum length allowed by the embedding.
        limit: u64,
    },
    /// Installing this descriptor would move application state backwards.
    BehindAppliedFloor {
        /// Descriptor's applied index.
        snapshot: LogIndex,
        /// Application's current durable index.
        applied: LogIndex,
    },
    /// Cancellation was requested before completion was accepted.
    Cancelled,
    /// The source could not serve the exact transfer and offset.
    SourceUnavailable {
        /// First unavailable payload offset.
        offset: u64,
    },
    /// A source call unwound; this reader cannot be reused after a caught panic.
    SourceInterrupted {
        /// Offset whose source call did not return normally.
        offset: u64,
    },
    /// A source returned a buffer whose retained capacity exceeds the budget.
    ChunkCapacity {
        /// Returned allocation capacity in bytes.
        actual: usize,
        /// Maximum retained buffer capacity.
        limit: u32,
    },
    /// The source returned a chunk of the wrong length.
    ChunkLength {
        /// Requested payload offset.
        offset: u64,
        /// Exact requested byte count.
        expected: u32,
        /// Actual returned byte count.
        actual: usize,
    },
    /// The caller stopped before consuming the complete payload.
    Incomplete {
        /// Bytes consumed by the caller.
        consumed: u64,
        /// Complete declared payload length.
        expected: u64,
    },
    /// The complete payload differs from the descriptor's corruption checksum.
    ChecksumMismatch {
        /// Descriptor's CRC32.
        expected: u32,
        /// CRC32 of bytes consumed by the caller.
        actual: u32,
    },
}

impl fmt::Display for SnapshotReadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroChunkSize => write!(f, "snapshot read chunk size must be nonzero"),
            Self::PayloadLimit { declared, limit } => write!(
                f,
                "snapshot payload length {declared} exceeds limit {limit}"
            ),
            Self::BehindAppliedFloor { snapshot, applied } => write!(
                f,
                "snapshot index {snapshot} is behind application floor {applied}"
            ),
            Self::Cancelled => write!(f, "snapshot payload read cancelled"),
            Self::SourceUnavailable { offset } => {
                write!(f, "snapshot source unavailable at offset {offset}")
            }
            Self::SourceInterrupted { offset } => {
                write!(f, "snapshot source call at {offset} was interrupted")
            }
            Self::ChunkCapacity { actual, limit } => write!(
                f,
                "snapshot source buffer capacity {actual} exceeds limit {limit}"
            ),
            Self::ChunkLength {
                offset,
                expected,
                actual,
            } => write!(
                f,
                "snapshot chunk at {offset} returned {actual} bytes instead of {expected}"
            ),
            Self::Incomplete { consumed, expected } => {
                write!(f, "snapshot reader consumed {consumed} of {expected} bytes")
            }
            Self::ChecksumMismatch { expected, actual } => write!(
                f,
                "snapshot payload checksum mismatch: expected {expected:#010x}, got {actual:#010x}"
            ),
        }
    }
}
impl Error for SnapshotReadError {}
