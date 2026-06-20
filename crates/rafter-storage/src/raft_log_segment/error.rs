use std::{error::Error, fmt, path::PathBuf};

use rafter::LogIndex;

use crate::{
    raft_log_compaction::DecodeRaftLogCompactionMarkerError, DecodeRaftLogEntryError,
    EncodeRaftLogEntryError,
};

/// Errors returned while appending entries to a Raft log segment.
///
/// This enum is exhaustive so callers can distinguish caller ordering bugs,
/// entry encoding failures, and filesystem errors.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftLogSegmentAppendError {
    NonContiguous {
        expected: LogIndex,
        actual: LogIndex,
    },
    Encode(EncodeRaftLogEntryError),
    Io {
        operation: &'static str,
        message: String,
    },
}

impl fmt::Display for RaftLogSegmentAppendError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NonContiguous { expected, actual } => write!(
                formatter,
                "Raft log append at index {actual} is not contiguous with expected next index {expected}"
            ),
            Self::Encode(error) => {
                write!(formatter, "Raft log entry could not be encoded: {error}")
            }
            Self::Io { operation, message } => {
                write!(formatter, "could not {operation}: {message}")
            }
        }
    }
}

impl Error for RaftLogSegmentAppendError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Encode(error) => Some(error),
            Self::NonContiguous { .. } | Self::Io { .. } => None,
        }
    }
}

/// Errors returned while truncating a Raft log suffix.
///
/// This enum is exhaustive so callers can distinguish invalid bounds from
/// rewrite and filesystem failures.
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
    Io {
        operation: &'static str,
        message: String,
    },
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
            Self::Io { operation, message } => {
                write!(formatter, "could not {operation}: {message}")
            }
        }
    }
}

impl Error for RaftLogSegmentTruncateError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Encode(error) => Some(error),
            Self::OutOfBounds { .. } | Self::BeforeCompactedPrefix { .. } | Self::Io { .. } => None,
        }
    }
}

/// Errors returned while compacting a Raft log prefix.
///
/// This enum is exhaustive so callers can distinguish invalid compaction
/// bounds from rewrite and filesystem failures.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RaftLogSegmentCompactError {
    OutOfBounds {
        compacted_through: LogIndex,
        next_index: LogIndex,
    },
    Encode(EncodeRaftLogEntryError),
    Io {
        operation: &'static str,
        message: String,
    },
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
            Self::Encode(error) => {
                write!(formatter, "Raft log entry could not be encoded: {error}")
            }
            Self::Io { operation, message } => {
                write!(formatter, "could not {operation}: {message}")
            }
        }
    }
}

impl Error for RaftLogSegmentCompactError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Encode(error) => Some(error),
            Self::OutOfBounds { .. } | Self::Io { .. } => None,
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
        message: String,
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
                message,
            } => write!(
                formatter,
                "could not {operation} at {}: {message}",
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
            Self::Io { .. } | Self::NonContiguous { .. } => None,
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
