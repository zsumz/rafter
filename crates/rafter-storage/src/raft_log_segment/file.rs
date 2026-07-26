//! File-backed retained-log mutation orchestration.
//!
//! This module owns append validation and the logical commit points of suffix
//! truncation and prefix compaction. Concrete state lives in `state`; streamed
//! replacement preparation and marker publication are delegated to `rewrite`.

use std::io::Write;

use rafter::LogIndex;

use crate::{BorrowedPersistedRaftLogEntry, PersistedRaftLogEntry};

use super::{
    append_borrowed_raft_log_frame, prepare_log_rewrite, reject_compact_bounds,
    reject_truncate_bounds, FileRaftLogSegment, PrepareLogRewriteError, RaftLogSegment,
    RaftLogSegmentAppendError, RaftLogSegmentCompactError, RaftLogSegmentTruncateError,
};

impl RaftLogSegment for FileRaftLogSegment {
    fn append_entries(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), RaftLogSegmentAppendError> {
        self.append_entries_borrowed(entries.iter().map(BorrowedPersistedRaftLogEntry::from))
    }

    fn append_entries_borrowed<'a, I>(
        &mut self,
        entries: I,
    ) -> Result<(), RaftLogSegmentAppendError>
    where
        I: IntoIterator<Item = BorrowedPersistedRaftLogEntry<'a>>,
    {
        if self.requires_reopen() {
            return Err(RaftLogSegmentAppendError::StoreRequiresReopen);
        }

        let mut bytes = Vec::new();
        let mut owned_entries = Vec::new();
        let mut expected_index = self.next_index();
        for entry in entries {
            if entry.index != expected_index {
                return Err(RaftLogSegmentAppendError::NonContiguous {
                    expected: expected_index,
                    actual: entry.index,
                });
            }
            append_borrowed_raft_log_frame(&mut bytes, entry)
                .map_err(RaftLogSegmentAppendError::Encode)?;
            owned_entries.push(PersistedRaftLogEntry::from(entry));
            expected_index = expected_index.next();
        }
        if owned_entries.is_empty() {
            return Ok(());
        }

        if let Err(error) = self
            .file
            .write_all(&bytes)
            .and_then(|()| self.file.sync_data())
        {
            let failure = self.record_io_failure("append raft log entries", error);
            return Err(RaftLogSegmentAppendError::Io {
                operation: failure.operation,
                source: failure.source,
            });
        }
        #[cfg(test)]
        if let Err(error) = crate::storage_failpoint_test::check(
            crate::storage_failpoint_test::DurabilityPoint::LogAppendAfterSync,
        ) {
            let failure = self.record_io_failure("append raft log entries", error);
            return Err(RaftLogSegmentAppendError::Io {
                operation: failure.operation,
                source: failure.source,
            });
        }

        self.entries.extend_owned_validated(owned_entries);
        Ok(())
    }

    fn truncate_suffix(&mut self, from_index: LogIndex) -> Result<(), RaftLogSegmentTruncateError> {
        if self.requires_reopen() {
            return Err(RaftLogSegmentTruncateError::StoreRequiresReopen);
        }

        reject_truncate_bounds(from_index, self.compacted_through, self.next_index())?;
        if from_index == self.next_index() {
            return Ok(());
        }

        let preparation = prepare_log_rewrite(
            self.temp_rewrite_path(),
            self.entries.entries_before(from_index),
        );
        let prepared = match preparation {
            Ok(prepared) => prepared,
            Err(error) => return Err(self.truncate_prepare_error(error)),
        };
        self.publish_log_rewrite(&prepared)
            .map_err(|failure| RaftLogSegmentTruncateError::Io {
                operation: failure.operation,
                source: failure.source,
            })?;
        self.entries.truncate_suffix(from_index);
        Ok(())
    }

    fn replay_entries(&self) -> Vec<PersistedRaftLogEntry> {
        self.entries.replay_entries()
    }

    fn next_index(&self) -> LogIndex {
        self.entries.next_index()
    }

    fn compacted_through(&self) -> LogIndex {
        self.compacted_through
    }

    fn compact_prefix_through(
        &mut self,
        through_index: LogIndex,
    ) -> Result<(), RaftLogSegmentCompactError> {
        if self.requires_reopen() {
            return Err(RaftLogSegmentCompactError::StoreRequiresReopen);
        }
        reject_compact_bounds(through_index)?;
        if through_index <= self.compacted_through {
            return Ok(());
        }

        // Stream and sync the replacement before publishing the marker. Once
        // the marker is durable, compaction is committed and no encoding or
        // replacement-file write failure may make that state look uncommitted.
        let preparation = prepare_log_rewrite(
            self.temp_rewrite_path(),
            self.entries.entries_after(through_index),
        );
        let prepared = match preparation {
            Ok(prepared) => prepared,
            Err(error) => return Err(self.compaction_prepare_error(error)),
        };
        self.publish_compaction_marker(through_index)
            .map_err(|failure| RaftLogSegmentCompactError::Io {
                operation: failure.operation,
                source: failure.source,
            })?;

        // The marker is the logical commit point. Reflect it in the cached view
        // before the best-effort physical rewrite so callers can distinguish a
        // committed compaction from failed byte reclamation.
        self.entries.compact_prefix_through(through_index);
        self.compacted_through = through_index;

        if let Err(failure) = self.publish_log_rewrite(&prepared) {
            return Err(RaftLogSegmentCompactError::CompactedButReclamationFailed {
                compacted_through: through_index,
                operation: failure.operation,
                source: failure.source,
            });
        }
        Ok(())
    }
}

impl FileRaftLogSegment {
    fn truncate_prepare_error(
        &mut self,
        error: PrepareLogRewriteError,
    ) -> RaftLogSegmentTruncateError {
        match error {
            PrepareLogRewriteError::Encode(error) => RaftLogSegmentTruncateError::Encode(error),
            PrepareLogRewriteError::Io { operation, source } => {
                let failure = self.record_io_failure(operation, source);
                RaftLogSegmentTruncateError::Io {
                    operation: failure.operation,
                    source: failure.source,
                }
            }
        }
    }

    fn compaction_prepare_error(
        &mut self,
        error: PrepareLogRewriteError,
    ) -> RaftLogSegmentCompactError {
        match error {
            PrepareLogRewriteError::Encode(error) => RaftLogSegmentCompactError::Encode(error),
            PrepareLogRewriteError::Io { operation, source } => {
                let failure = self.record_io_failure(operation, source);
                RaftLogSegmentCompactError::Io {
                    operation: failure.operation,
                    source: failure.source,
                }
            }
        }
    }
}
