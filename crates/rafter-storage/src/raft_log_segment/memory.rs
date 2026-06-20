use std::collections::BTreeMap;

use rafter::LogIndex;

use crate::PersistedRaftLogEntry;

use super::{
    reject_truncate_bounds, validate_contiguous, RaftLogSegment, RaftLogSegmentAppendError,
    RaftLogSegmentCompactError, RaftLogSegmentTruncateError,
};

/// In-memory [`RaftLogSegment`] implementation for tests and volatile
/// runtimes.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InMemoryRaftLogSegment {
    compacted_through: LogIndex,
    entries: BTreeMap<LogIndex, PersistedRaftLogEntry>,
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
        validate_contiguous(entries, self.next_index()).map_err(
            |super::NonContiguousRaftEntry { expected, actual }| {
                RaftLogSegmentAppendError::NonContiguous { expected, actual }
            },
        )?;
        for entry in entries {
            self.entries.insert(entry.index, entry.clone());
        }
        Ok(())
    }

    fn truncate_suffix(&mut self, from_index: LogIndex) -> Result<(), RaftLogSegmentTruncateError> {
        reject_truncate_bounds(from_index, self.compacted_through, self.next_index())?;
        if from_index == self.first_available_index() {
            self.entries.clear();
            return Ok(());
        }
        self.entries.retain(|index, _| *index < from_index);
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
        self.entries.retain(|index, _| *index > through_index);
        Ok(())
    }

    fn replay_entries(&self) -> Vec<PersistedRaftLogEntry> {
        self.entries.values().cloned().collect()
    }

    fn compacted_through(&self) -> LogIndex {
        self.compacted_through
    }

    fn next_index(&self) -> LogIndex {
        self.entries
            .keys()
            .next_back()
            .copied()
            .map_or_else(|| self.first_available_index(), LogIndex::next)
    }
}

impl InMemoryRaftLogSegment {
    fn first_available_index(&self) -> LogIndex {
        self.compacted_through.next()
    }
}
