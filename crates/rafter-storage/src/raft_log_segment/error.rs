//! Public error vocabulary for log mutation, open, replay, and repair.

use std::{error::Error, fmt, path::PathBuf};

use rafter::LogIndex;

use crate::{
    raft_log_compaction::DecodeRaftLogCompactionMarkerError, DecodeRaftLogEntryError,
    EncodeRaftLogEntryError, StorageIoError,
};

/// Errors returned while appending entries to a Raft log segment.
///
/// This enum is exhaustive so callers can distinguish caller ordering bugs,
/// entry encoding failures, filesystem errors, and a file handle that must be
/// reopened after an earlier ambiguous I/O failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftLogSegmentAppendError {
    NonContiguous {
        expected: LogIndex,
        actual: LogIndex,
    },
    /// An append naming `u64::MAX` was requested. `next_index()` is the stored
    /// index plus one, so storing that entry would leave the segment with no
    /// representable next index.
    IndexAtMaximum,
    Encode(EncodeRaftLogEntryError),
    /// The append may have changed the file. Reopen before another mutation.
    Io {
        operation: &'static str,
        source: StorageIoError,
    },
    /// An earlier mutating I/O error made this file-backed handle unsafe to use.
    StoreRequiresReopen,
}

impl fmt::Display for RaftLogSegmentAppendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonContiguous { expected, actual } => write!(
                formatter,
                "Raft log append at index {actual} is not contiguous with expected next index {expected}"
            ),
            Self::IndexAtMaximum => formatter.write_str(
                "Raft log append at the maximum log index would leave no next index for the segment to advance to",
            ),
            Self::Encode(error) => {
                write!(formatter, "Raft log entry could not be encoded: {error}")
            }
            Self::Io { operation, source } => {
                write!(formatter, "could not {operation}: {source}")
            }
            Self::StoreRequiresReopen => formatter.write_str(
                "Raft log segment requires reopen after an earlier I/O failure",
            ),
        }
    }
}

impl Error for RaftLogSegmentAppendError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Encode(error) => Some(error),
            Self::Io { source, .. } => Some(source.as_io_error()),
            Self::NonContiguous { .. } | Self::IndexAtMaximum | Self::StoreRequiresReopen => None,
        }
    }
}

/// Errors returned while truncating a Raft log suffix.
///
/// This enum is exhaustive so callers can distinguish invalid bounds from
/// encoding, filesystem, and post-error handle-state failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftLogSegmentTruncateError {
    OutOfBounds {
        next_index: LogIndex,
        actual: LogIndex,
    },
    BeforeCompactedPrefix {
        compacted_through: LogIndex,
        actual: LogIndex,
    },
    Encode(EncodeRaftLogEntryError),
    /// The replacement may have changed the stable path. Reopen before another
    /// mutation.
    Io {
        operation: &'static str,
        source: StorageIoError,
    },
    /// An earlier mutating I/O error made this file-backed handle unsafe to use.
    StoreRequiresReopen,
}

impl fmt::Display for RaftLogSegmentTruncateError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfBounds { next_index, actual } => write!(
                formatter,
                "Raft log truncate from index {actual} is beyond the next log index {next_index}"
            ),
            Self::BeforeCompactedPrefix {
                compacted_through,
                actual,
            } => write!(
                formatter,
                "Raft log truncate from index {actual} would erase the prefix already compacted through index {compacted_through}"
            ),
            Self::Encode(error) => {
                write!(formatter, "Raft log entry could not be encoded: {error}")
            }
            Self::Io { operation, source } => {
                write!(formatter, "could not {operation}: {source}")
            }
            Self::StoreRequiresReopen => formatter.write_str(
                "Raft log segment requires reopen after an earlier I/O failure",
            ),
        }
    }
}

impl Error for RaftLogSegmentTruncateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Encode(error) => Some(error),
            Self::Io { source, .. } => Some(source.as_io_error()),
            Self::OutOfBounds { .. }
            | Self::BeforeCompactedPrefix { .. }
            | Self::StoreRequiresReopen => None,
        }
    }
}

/// Errors returned while compacting a Raft log prefix.
///
/// This enum distinguishes a failure before the marker is confirmed durable
/// from a physical-rewrite failure after logical compaction has committed.
/// It is exhaustive so callers can handle those commit states explicitly.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftLogSegmentCompactError {
    OutOfBounds {
        compacted_through: LogIndex,
        next_index: LogIndex,
    },
    /// Compaction through `u64::MAX` was requested. The retained suffix starts
    /// at `through_index.next()`, so this boundary leaves nowhere for it to
    /// start and would publish a marker that reopening must reject.
    ThroughIndexAtMaximum,
    Encode(EncodeRaftLogEntryError),
    /// The compaction commit point was not confirmed. Reopen to determine which
    /// durable state won before another mutation.
    Io {
        operation: &'static str,
        source: StorageIoError,
    },
    /// The compaction marker is durable, but rewriting the log to reclaim old
    /// frame bytes failed. Reopen reconstructs the committed compacted suffix.
    CompactedButReclamationFailed {
        compacted_through: LogIndex,
        operation: &'static str,
        source: StorageIoError,
    },
    /// An earlier mutating I/O error made this file-backed handle unsafe to use.
    StoreRequiresReopen,
}

impl fmt::Display for RaftLogSegmentCompactError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutOfBounds {
                compacted_through,
                next_index,
            } => write!(
                formatter,
                "Raft log compaction is out of bounds with prefix compacted through index {compacted_through} and next index {next_index}"
            ),
            Self::ThroughIndexAtMaximum => formatter.write_str(
                "Raft log compaction through the maximum log index would leave no room for the retained suffix that must follow it",
            ),
            Self::Encode(error) => {
                write!(formatter, "Raft log entry could not be encoded: {error}")
            }
            Self::Io { operation, source } => {
                write!(formatter, "could not {operation}: {source}")
            }
            Self::CompactedButReclamationFailed {
                compacted_through,
                operation,
                source,
            } => write!(
                formatter,
                "Raft log is durably compacted through index {compacted_through}, but obsolete frame bytes could not be reclaimed while trying to {operation}: {source}; the store requires reopen"
            ),
            Self::StoreRequiresReopen => formatter.write_str(
                "Raft log segment requires reopen after an earlier I/O failure",
            ),
        }
    }
}

impl Error for RaftLogSegmentCompactError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Encode(error) => Some(error),
            Self::Io { source, .. } | Self::CompactedButReclamationFailed { source, .. } => {
                Some(source.as_io_error())
            }
            Self::OutOfBounds { .. } | Self::ThroughIndexAtMaximum | Self::StoreRequiresReopen => {
                None
            }
        }
    }
}

/// Errors returned while opening and replaying a file-backed log segment.
///
/// This enum is exhaustive so callers can distinguish I/O, corrupt entries,
/// corrupt compaction markers, and non-contiguous replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OpenRaftLogSegmentError {
    Io {
        operation: &'static str,
        path: PathBuf,
        source: StorageIoError,
    },
    Replay(RaftLogReplayError),
    CompactionMarker(DecodeRaftLogCompactionMarkerError),
    NonContiguous {
        expected: LogIndex,
        actual: LogIndex,
    },
}

impl fmt::Display for OpenRaftLogSegmentError {
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
            Self::Replay(error) => {
                write!(formatter, "stored Raft log segment is corrupt: {error}")
            }
            Self::CompactionMarker(error) => write!(
                formatter,
                "stored Raft log compaction marker is corrupt: {error}"
            ),
            Self::NonContiguous { expected, actual } => write!(
                formatter,
                "replayed Raft log entry at index {actual} is not contiguous with expected index {expected}"
            ),
        }
    }
}

impl Error for OpenRaftLogSegmentError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Replay(error) => Some(error),
            Self::CompactionMarker(error) => Some(error),
            Self::Io { source, .. } => Some(source.as_io_error()),
            Self::NonContiguous { .. } => None,
        }
    }
}

/// Errors returned while scanning persisted log frames.
///
/// This enum is exhaustive because replay failures are limited to truncated
/// frame structure and corrupt entry payloads.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftLogReplayError {
    PartialFrameHeader {
        offset: usize,
        remaining: usize,
    },
    PartialEntry {
        offset: usize,
        expected: usize,
        remaining: usize,
    },
    CorruptEntry {
        offset: usize,
        source: DecodeRaftLogEntryError,
    },
}

impl fmt::Display for RaftLogReplayError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PartialFrameHeader { offset, remaining } => write!(
                formatter,
                "Raft log frame header at offset {offset} is truncated with only {remaining} bytes remaining"
            ),
            Self::PartialEntry {
                offset,
                expected,
                remaining,
            } => write!(
                formatter,
                "Raft log frame at offset {offset} expects {expected} entry bytes but only {remaining} remain"
            ),
            Self::CorruptEntry { offset, source } => write!(
                formatter,
                "Raft log entry at offset {offset} is corrupt: {source}"
            ),
        }
    }
}

impl Error for RaftLogReplayError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::CorruptEntry { source, .. } => Some(source),
            Self::PartialFrameHeader { .. } | Self::PartialEntry { .. } => None,
        }
    }
}
