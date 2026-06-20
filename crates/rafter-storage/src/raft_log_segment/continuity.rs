use std::collections::BTreeMap;

use rafter::LogIndex;

use crate::PersistedRaftLogEntry;

use super::RaftLogSegmentTruncateError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct NonContiguousRaftEntry {
    pub(super) expected: LogIndex,
    pub(super) actual: LogIndex,
}

pub(super) fn entries_by_index(
    entries: &[PersistedRaftLogEntry],
    start: LogIndex,
) -> Result<BTreeMap<LogIndex, PersistedRaftLogEntry>, NonContiguousRaftEntry> {
    validate_contiguous(entries, start)?;
    Ok(entries
        .iter()
        .map(|entry| (entry.index, entry.clone()))
        .collect())
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
