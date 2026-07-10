use rafter::LogIndex;

use crate::PersistedRaftLogEntry;

use super::RaftLogSegmentTruncateError;

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

    pub(super) fn entries_before(&self, from_index: LogIndex) -> Vec<PersistedRaftLogEntry> {
        let len = self
            .entries
            .partition_point(|entry| entry.index < from_index);
        self.entries[..len].to_vec()
    }

    pub(super) fn entries_after(&self, through_index: LogIndex) -> Vec<PersistedRaftLogEntry> {
        let start = self
            .entries
            .partition_point(|entry| entry.index <= through_index);
        self.entries[start..].to_vec()
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
mod tests {
    use rafter::Term;

    use super::*;

    fn entry(index: u64) -> PersistedRaftLogEntry {
        PersistedRaftLogEntry::application(LogIndex(index), Term(1), Vec::new())
    }

    #[test]
    fn contiguous_entries_track_next_index_without_tree_lookup() {
        let mut entries = ContiguousLogEntries::default();

        entries
            .append(&[entry(1), entry(2), entry(3)])
            .expect("contiguous append succeeds");

        assert_eq!(entries.next_index(), LogIndex(4));
        assert_eq!(entries.replay_entries(), vec![entry(1), entry(2), entry(3)]);
    }

    #[test]
    fn contiguous_entries_reject_gaps_at_the_boundary() {
        let mut entries = ContiguousLogEntries::default();

        assert_eq!(
            entries.append(&[entry(2)]),
            Err(NonContiguousRaftEntry {
                expected: LogIndex(1),
                actual: LogIndex(2),
            })
        );
        assert_eq!(entries.next_index(), LogIndex(1));
        assert!(entries.replay_entries().is_empty());
    }

    #[test]
    fn contiguous_entries_compact_prefix_and_past_tail() {
        let mut entries =
            ContiguousLogEntries::from_entries(LogIndex(1), vec![entry(1), entry(2), entry(3)])
                .expect("entries are contiguous");

        entries.compact_prefix_through(LogIndex(2));
        assert_eq!(entries.next_index(), LogIndex(4));
        assert_eq!(entries.replay_entries(), vec![entry(3)]);

        entries.compact_prefix_through(LogIndex(5));
        assert_eq!(entries.next_index(), LogIndex(6));
        assert!(entries.replay_entries().is_empty());
    }

    #[test]
    fn contiguous_entries_truncate_suffix() {
        let mut entries =
            ContiguousLogEntries::from_entries(LogIndex(3), vec![entry(3), entry(4), entry(5)])
                .expect("entries are contiguous");

        entries.truncate_suffix(LogIndex(4));

        assert_eq!(entries.next_index(), LogIndex(4));
        assert_eq!(entries.replay_entries(), vec![entry(3)]);
    }
}
