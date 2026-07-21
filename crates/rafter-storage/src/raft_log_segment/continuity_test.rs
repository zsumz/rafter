//! Retained-log continuity, truncation, and compaction scenarios.

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
