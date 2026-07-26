//! Contiguous retained-log representation and mutation bounds.
//!
//! This module owns the proven in-memory suffix geometry shared by replay, the
//! file store, and the volatile reference implementation. Rewrite views borrow
//! that suffix so payloads are not cloned merely to reclaim file bytes.

use rafter::LogIndex;

use crate::{format::advanceable_log_index, PersistedRaftLogEntry};

use super::{RaftLogSegmentAppendError, RaftLogSegmentCompactError, RaftLogSegmentTruncateError};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct NonContiguousRaftEntry {
    pub(super) expected: LogIndex,
    pub(super) actual: LogIndex,
}

/// Retained durable log entries after replay/repair has proven they cover one
/// contiguous range.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ContiguousLogEntries {
    first_index: LogIndex,
    entries: Vec<PersistedRaftLogEntry>,
}

impl Default for ContiguousLogEntries {
    fn default() -> Self {
        Self::empty_after(LogIndex::ZERO)
    }
}

impl ContiguousLogEntries {
    pub(super) fn empty_after(compacted_through: LogIndex) -> Self {
        Self {
            first_index: compacted_through.next(),
            entries: Vec::new(),
        }
    }

    pub(super) fn from_entries(
        first_index: LogIndex,
        entries: Vec<PersistedRaftLogEntry>,
    ) -> Result<Self, NonContiguousRaftEntry> {
        validate_contiguous(&entries, first_index)?;
        Ok(Self {
            first_index,
            entries,
        })
    }

    pub(super) fn append(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), NonContiguousRaftEntry> {
        validate_contiguous(entries, self.next_index())?;
        self.extend_validated(entries);
        Ok(())
    }

    pub(super) fn append_owned(
        &mut self,
        entries: Vec<PersistedRaftLogEntry>,
    ) -> Result<(), NonContiguousRaftEntry> {
        validate_contiguous(&entries, self.next_index())?;
        self.extend_owned_validated(entries);
        Ok(())
    }

    pub(super) fn extend_validated(&mut self, entries: &[PersistedRaftLogEntry]) {
        debug_assert!(validate_contiguous(entries, self.next_index()).is_ok());
        self.entries.extend_from_slice(entries);
    }

    pub(super) fn extend_owned_validated(&mut self, entries: Vec<PersistedRaftLogEntry>) {
        debug_assert!(validate_contiguous(&entries, self.next_index()).is_ok());
        self.entries.extend(entries);
    }

    pub(super) fn truncate_suffix(&mut self, from_index: LogIndex) {
        let len = self
            .entries
            .partition_point(|entry| entry.index < from_index);
        self.entries.truncate(len);
    }

    pub(super) fn compact_prefix_through(&mut self, through_index: LogIndex) {
        let retained_start = through_index.next();
        if retained_start <= self.first_index {
            return;
        }

        let drop_count = self
            .entries
            .partition_point(|entry| entry.index < retained_start);
        self.entries.drain(..drop_count);
        self.first_index = retained_start;
    }

    pub(super) fn replay_entries(&self) -> Vec<PersistedRaftLogEntry> {
        self.entries.clone()
    }

    pub(super) fn entries_before(&self, from_index: LogIndex) -> &[PersistedRaftLogEntry] {
        let len = self
            .entries
            .partition_point(|entry| entry.index < from_index);
        &self.entries[..len]
    }

    pub(super) fn entries_after(&self, through_index: LogIndex) -> &[PersistedRaftLogEntry] {
        let start = self
            .entries
            .partition_point(|entry| entry.index <= through_index);
        &self.entries[start..]
    }

    pub(super) fn next_index(&self) -> LogIndex {
        self.entries
            .last()
            .map_or(self.first_index, |entry| entry.index.next())
    }
}

pub(super) fn validate_contiguous(
    entries: &[PersistedRaftLogEntry],
    start: LogIndex,
) -> Result<(), NonContiguousRaftEntry> {
    let mut expected = start;
    for entry in entries {
        if entry.index != expected {
            return Err(NonContiguousRaftEntry {
                expected,
                actual: entry.index,
            });
        }
        expected = expected.next();
    }
    Ok(())
}

/// Rejects an append at a log index the segment could not advance past.
///
/// `next_index()` is the last stored index plus one, and the contiguity walk
/// takes the same successor, so a stored entry at `u64::MAX` leaves the segment
/// with no representable next index: it panics in debug builds and wraps to
/// [`LogIndex::ZERO`] in release builds, which re-enters the index space at the
/// sentinel meaning "before the first entry".
///
/// This is the append half of the same successor bound
/// [`reject_compact_bounds`] applies to a compaction boundary. Both shipped
/// segments call it, so the refusal belongs to the
/// [`RaftLogSegment`](super::RaftLogSegment) contract rather than to whichever
/// implementation happens to encode: the in-memory segment encodes nothing, and
/// before this it inherited no bound at all.
pub(super) fn reject_append_bounds(index: LogIndex) -> Result<(), RaftLogSegmentAppendError> {
    if advanceable_log_index(index.0).is_none() {
        return Err(RaftLogSegmentAppendError::IndexAtMaximum);
    }
    Ok(())
}

/// Rejects a compaction boundary the retained suffix could not start after.
///
/// Every segment derives its retained-suffix floor as `through_index.next()`,
/// so `u64::MAX` is refused here rather than wrapped to [`LogIndex::ZERO`]. This
/// keeps the caller-supplied boundary inside the same bound the RFLC decoder
/// enforces on the marker this compaction would go on to publish.
pub(super) fn reject_compact_bounds(
    through_index: LogIndex,
) -> Result<(), RaftLogSegmentCompactError> {
    if advanceable_log_index(through_index.0).is_none() {
        return Err(RaftLogSegmentCompactError::ThroughIndexAtMaximum);
    }
    Ok(())
}

pub(super) fn reject_truncate_bounds(
    from_index: LogIndex,
    compacted_through: LogIndex,
    next_index: LogIndex,
) -> Result<(), RaftLogSegmentTruncateError> {
    if from_index <= compacted_through {
        return Err(RaftLogSegmentTruncateError::BeforeCompactedPrefix {
            compacted_through,
            actual: from_index,
        });
    }
    if from_index > next_index {
        return Err(RaftLogSegmentTruncateError::OutOfBounds {
            next_index,
            actual: from_index,
        });
    }
    Ok(())
}

#[cfg(test)]
#[path = "continuity_test.rs"]
mod tests;
