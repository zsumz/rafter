//! Error vocabulary for opening and replaying a durable log segment.
//!
//! These enums separate filesystem failures from corrupt frames, corrupt
//! compaction markers, and a replayed suffix that is not contiguous.

use std::{error::Error, fmt, path::PathBuf};

use rafter::LogIndex;

use crate::{
    raft_log_compaction::DecodeRaftLogCompactionMarkerError, DecodeRaftLogEntryError,
    StorageIoError,
};

/// Errors returned while opening and replaying a file-backed log segment.
///
/// This enum is exhaustive so callers can distinguish I/O, corrupt entries,
/// corrupt compaction markers, and non-contiguous replay.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OpenRaftLogSegmentError {
    /// A filesystem operation failed while opening the segment.
    Io {
        /// Stable name of the failed filesystem operation.
        operation: &'static str,
        /// Path on which the operation failed.
        path: PathBuf,
        /// Preserved I/O failure.
        source: StorageIoError,
    },
    /// A retained frame was truncated or corrupt.
    Replay(RaftLogReplayError),
    /// The durable compaction marker was corrupt or unsupported.
    CompactionMarker(DecodeRaftLogCompactionMarkerError),
    /// Replayed entries did not form a contiguous retained suffix.
    NonContiguous {
        /// Log index required at this replay position.
        expected: LogIndex,
        /// Log index decoded from the stored frame.
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
    /// The file ended part-way through a frame-length prefix.
    PartialFrameHeader {
        /// Byte offset at which the frame began.
        offset: usize,
        /// Bytes remaining after the partial header.
        remaining: usize,
    },
    /// The file ended before the declared entry payload completed.
    PartialEntry {
        /// Byte offset at which the frame began.
        offset: usize,
        /// Entry bytes declared by the frame header.
        expected: usize,
        /// Entry bytes remaining in the file.
        remaining: usize,
    },
    /// A complete frame contained an invalid entry envelope.
    CorruptEntry {
        /// Byte offset at which the frame began.
        offset: usize,
        /// Exact entry decoding failure.
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
