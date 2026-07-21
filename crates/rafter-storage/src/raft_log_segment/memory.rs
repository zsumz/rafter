//! In-memory retained-log reference behavior.
//!
//! This implementation shares continuity and bounds validation with the file
//! store without owning framing, filesystem publication, or repair.

use rafter::LogIndex;

use crate::{BorrowedPersistedRaftLogEntry, PersistedRaftLogEntry};

use super::{
    reject_truncate_bounds, ContiguousLogEntries, RaftLogSegment, RaftLogSegmentAppendError,
    RaftLogSegmentCompactError, RaftLogSegmentTruncateError,
};

/// In-memory [`RaftLogSegment`] implementation for tests and volatile
/// runtimes.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InMemoryRaftLogSegment {
    compacted_through: LogIndex,
    entries: ContiguousLogEntries,
}

impl InMemoryRaftLogSegment {
    /// Creates an empty in-memory log segment.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl RaftLogSegment for InMemoryRaftLogSegment {
    fn append_entries(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), RaftLogSegmentAppendError> {
        self.entries.append(entries).map_err(
            |super::NonContiguousRaftEntry { expected, actual }| {
                RaftLogSegmentAppendError::NonContiguous { expected, actual }
            },
        )?;
        Ok(())
    }

    fn append_entries_borrowed<'a, I>(
        &mut self,
        entries: I,
    ) -> Result<(), RaftLogSegmentAppendError>
    where
        I: IntoIterator<Item = BorrowedPersistedRaftLogEntry<'a>>,
    {
        let entries = entries
            .into_iter()
            .map(PersistedRaftLogEntry::from)
            .collect::<Vec<_>>();
        self.entries.append_owned(entries).map_err(
            |super::NonContiguousRaftEntry { expected, actual }| {
                RaftLogSegmentAppendError::NonContiguous { expected, actual }
            },
        )?;
        Ok(())
    }

    fn truncate_suffix(&mut self, from_index: LogIndex) -> Result<(), RaftLogSegmentTruncateError> {
        reject_truncate_bounds(from_index, self.compacted_through, self.next_index())?;
        self.entries.truncate_suffix(from_index);
        Ok(())
    }

    fn compact_prefix_through(
        &mut self,
        through_index: LogIndex,
    ) -> Result<(), RaftLogSegmentCompactError> {
        if through_index <= self.compacted_through {
            return Ok(());
        }

        self.compacted_through = through_index;
        self.entries.compact_prefix_through(through_index);
        Ok(())
    }

    fn replay_entries(&self) -> Vec<PersistedRaftLogEntry> {
        self.entries.replay_entries()
    }

    fn compacted_through(&self) -> LogIndex {
        self.compacted_through
    }

    fn next_index(&self) -> LogIndex {
        self.entries.next_index()
    }
}
