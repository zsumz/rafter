mod continuity;
mod error;
mod frames;
mod memory;
mod open;

use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use rafter::LogIndex;

use crate::{
    durable_fs::sync_parent_directory, raft_log_compaction::encode_raft_log_compaction_marker,
    PersistedRaftLogEntry,
};

use continuity::{
    entries_by_index, reject_truncate_bounds, validate_contiguous, NonContiguousRaftEntry,
};
pub use error::{
    OpenRaftLogSegmentError, RaftLogReplayError, RaftLogSegmentAppendError,
    RaftLogSegmentCompactError, RaftLogSegmentTruncateError,
};
use frames::{append_raft_log_frames, scan_raft_log_frames};
pub use memory::InMemoryRaftLogSegment;

/// Durable append-only Raft log segment with suffix truncation and prefix
/// compaction.
///
/// Implementations must make successful mutations durable before returning.
pub trait RaftLogSegment {
    /// Appends persisted Raft log entries to the segment.
    ///
    /// # Errors
    ///
    /// Returns [`RaftLogSegmentAppendError::NonContiguous`] when the batch does
    /// not start at the segment's next expected log index.
    fn append_entries(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), RaftLogSegmentAppendError>;

    /// Removes persisted entries at `from_index` and later.
    ///
    /// # Errors
    ///
    /// Returns [`RaftLogSegmentTruncateError::OutOfBounds`] when `from_index`
    /// is greater than the segment's next expected log index. Returns
    /// [`RaftLogSegmentTruncateError::BeforeCompactedPrefix`] when the request
    /// would erase through the already-compacted durable snapshot boundary.
    fn truncate_suffix(&mut self, from_index: LogIndex) -> Result<(), RaftLogSegmentTruncateError>;

    /// Removes persisted entries through `through_index`.
    ///
    /// # Errors
    ///
    /// This may advance the compacted prefix beyond the local tail when a
    /// follower installs a leader snapshot that replaces missing local log.
    fn compact_prefix_through(
        &mut self,
        through_index: LogIndex,
    ) -> Result<(), RaftLogSegmentCompactError>;

    /// Returns every retained persisted entry in ascending log-index order.
    fn replay_entries(&self) -> Vec<PersistedRaftLogEntry>;

    /// Returns the next log index that would be assigned by a contiguous
    /// append.
    fn next_index(&self) -> LogIndex;

    /// The durable compacted-prefix boundary: entries at or below this index
    /// are covered by a snapshot and are no longer stored. When this returns
    /// Ok, a crash immediately after preserves this boundary.
    fn compacted_through(&self) -> LogIndex;
}

/// File-backed [`RaftLogSegment`] implementation.
#[derive(Debug)]
pub struct FileRaftLogSegment {
    file: File,
    path: PathBuf,
    compacted_through: LogIndex,
    entries: BTreeMap<LogIndex, PersistedRaftLogEntry>,
}

impl FileRaftLogSegment {
    /// Returns the durable compacted-prefix boundary.
    #[must_use]
    pub fn compacted_through(&self) -> LogIndex {
        self.compacted_through
    }
}

impl RaftLogSegment for FileRaftLogSegment {
    fn append_entries(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), RaftLogSegmentAppendError> {
        validate_contiguous(entries, self.next_index()).map_err(
            |NonContiguousRaftEntry { expected, actual }| {
                RaftLogSegmentAppendError::NonContiguous { expected, actual }
            },
        )?;

        let mut bytes = Vec::new();
        append_raft_log_frames(&mut bytes, entries).map_err(RaftLogSegmentAppendError::Encode)?;
        self.file
            .write_all(&bytes)
            .and_then(|()| self.file.sync_data())
            .map_err(|error| RaftLogSegmentAppendError::Io {
                operation: "append raft log entries",
                message: error.to_string(),
            })?;

        for entry in entries {
            self.entries.insert(entry.index, entry.clone());
        }
        Ok(())
    }

    fn truncate_suffix(&mut self, from_index: LogIndex) -> Result<(), RaftLogSegmentTruncateError> {
        reject_truncate_bounds(from_index, self.compacted_through, self.next_index())?;
        if from_index == self.next_index() {
            return Ok(());
        }

        let retained = self
            .entries
            .values()
            .filter(|entry| entry.index < from_index)
            .cloned()
            .collect::<Vec<_>>();
        self.rewrite_entries(&retained)?;
        self.entries = retained
            .into_iter()
            .map(|entry| (entry.index, entry))
            .collect();
        Ok(())
    }

    fn replay_entries(&self) -> Vec<PersistedRaftLogEntry> {
        self.entries.values().cloned().collect()
    }

    fn next_index(&self) -> LogIndex {
        self.entries
            .keys()
            .next_back()
            .copied()
            .map_or_else(|| self.first_available_index(), LogIndex::next)
    }

    fn compacted_through(&self) -> LogIndex {
        self.compacted_through
    }

    fn compact_prefix_through(
        &mut self,
        through_index: LogIndex,
    ) -> Result<(), RaftLogSegmentCompactError> {
        if through_index <= self.compacted_through {
            return Ok(());
        }

        self.write_compaction_marker(through_index)?;
        let retained = self
            .entries
            .values()
            .filter(|entry| entry.index > through_index)
            .cloned()
            .collect::<Vec<_>>();
        self.rewrite_entries_for_compaction(&retained)?;
        self.entries = retained
            .into_iter()
            .map(|entry| (entry.index, entry))
            .collect();
        self.compacted_through = through_index;
        Ok(())
    }
}

impl FileRaftLogSegment {
    fn rewrite_entries(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), RaftLogSegmentTruncateError> {
        let temp_path = self.temp_rewrite_path();
        let mut bytes = Vec::new();
        append_raft_log_frames(&mut bytes, entries).map_err(RaftLogSegmentTruncateError::Encode)?;

        {
            let mut temp = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temp_path)
                .map_err(|error| RaftLogSegmentTruncateError::Io {
                    operation: "open rewritten raft log segment",
                    message: error.to_string(),
                })?;
            temp.write_all(&bytes)
                .and_then(|()| temp.sync_data())
                .map_err(|error| RaftLogSegmentTruncateError::Io {
                    operation: "write rewritten raft log segment",
                    message: error.to_string(),
                })?;
        }

        fs::rename(&temp_path, &self.path).map_err(|error| RaftLogSegmentTruncateError::Io {
            operation: "replace raft log segment",
            message: error.to_string(),
        })?;
        sync_parent_directory(&self.path).map_err(|error| RaftLogSegmentTruncateError::Io {
            operation: "sync raft log segment directory",
            message: error.to_string(),
        })?;

        self.file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&self.path)
            .map_err(|error| RaftLogSegmentTruncateError::Io {
                operation: "reopen raft log segment",
                message: error.to_string(),
            })?;
        self.file
            .sync_data()
            .map_err(|error| RaftLogSegmentTruncateError::Io {
                operation: "sync rewritten raft log segment",
                message: error.to_string(),
            })
    }

    fn temp_rewrite_path(&self) -> PathBuf {
        self.path
            .with_extension(format!("rewrite-{}.tmp", std::process::id()))
    }

    fn first_available_index(&self) -> LogIndex {
        self.compacted_through.next()
    }

    fn write_compaction_marker(
        &self,
        compacted_through: LogIndex,
    ) -> Result<(), RaftLogSegmentCompactError> {
        let bytes = encode_raft_log_compaction_marker(compacted_through);
        let temp_path = self.temp_compaction_marker_path();
        let marker_path = self.compaction_marker_path();
        {
            let mut temp = OpenOptions::new()
                .create(true)
                .truncate(true)
                .write(true)
                .open(&temp_path)
                .map_err(|error| RaftLogSegmentCompactError::Io {
                    operation: "open raft log compaction marker",
                    message: error.to_string(),
                })?;
            temp.write_all(&bytes)
                .and_then(|()| temp.sync_data())
                .map_err(|error| RaftLogSegmentCompactError::Io {
                    operation: "write raft log compaction marker",
                    message: error.to_string(),
                })?;
        }

        fs::rename(&temp_path, &marker_path).map_err(|error| RaftLogSegmentCompactError::Io {
            operation: "replace raft log compaction marker",
            message: error.to_string(),
        })?;
        sync_parent_directory(&marker_path).map_err(|error| RaftLogSegmentCompactError::Io {
            operation: "sync raft log compaction marker directory",
            message: error.to_string(),
        })
    }

    fn rewrite_entries_for_compaction(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), RaftLogSegmentCompactError> {
        self.rewrite_entries(entries).map_err(|error| match error {
            RaftLogSegmentTruncateError::OutOfBounds { .. }
            | RaftLogSegmentTruncateError::BeforeCompactedPrefix { .. } => {
                unreachable!("rewrite_entries does not validate bounds")
            }
            RaftLogSegmentTruncateError::Encode(error) => RaftLogSegmentCompactError::Encode(error),
            RaftLogSegmentTruncateError::Io { operation, message } => {
                RaftLogSegmentCompactError::Io { operation, message }
            }
        })
    }

    fn compaction_marker_path(&self) -> PathBuf {
        compaction_marker_path(&self.path)
    }

    fn temp_compaction_marker_path(&self) -> PathBuf {
        let mut temp = self.compaction_marker_path().into_os_string();
        temp.push(format!(".{}.tmp", std::process::id()));
        PathBuf::from(temp)
    }
}

fn compaction_marker_path(path: &Path) -> PathBuf {
    let mut marker = path.as_os_str().to_os_string();
    marker.push(".compact");
    PathBuf::from(marker)
}

#[cfg(test)]
#[path = "raft_log_segment/compaction_test.rs"]
mod compaction_test;
#[cfg(test)]
#[path = "raft_log_segment/memory_test.rs"]
mod memory_test;
#[cfg(test)]
#[path = "raft_log_segment_test.rs"]
mod raft_log_segment_test;
#[cfg(test)]
#[path = "raft_log_segment/test_support.rs"]
mod test_support;
