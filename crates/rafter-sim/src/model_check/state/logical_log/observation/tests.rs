//! Prefix-observation storage tests.

use std::collections::BTreeMap;

use rafter::{LogEntry, LogEntryKind, LogIndex, NodeId, Term};

use super::super::{LogPrefixWitness, LogicalLogHistory, LogicalLogView};

#[test]
fn sequential_growth_allocates_one_shared_node_per_unique_extension() {
    let tracked_prefix = LogPrefixWitness::tracking_allocations();
    let mut history = LogicalLogHistory::default();
    let mut entries = BTreeMap::new();

    for (raw_index, payload) in [(1, b"one".as_slice()), (2, b"two"), (3, b"three")] {
        let index = LogIndex(raw_index);
        entries.insert(
            index,
            LogEntry {
                term: Term(raw_index),
                kind: LogEntryKind::application(payload.to_vec()),
            },
        );
        history.observe_entries(
            NodeId(1),
            &LogicalLogView {
                snapshot: None,
                entries: entries.clone(),
            },
            tracked_prefix.clone(),
        );
    }

    let through_one = retained(&history, 1);
    let through_two = retained(&history, 2);
    let through_three = retained(&history, 3);
    let cloned = history.clone();
    let cloned_through_three = retained(&cloned, 3);

    assert_eq!(tracked_prefix.allocation_count(), 3);
    assert!(through_one.shares_prefix_storage_with(through_two));
    assert!(through_two.shares_prefix_storage_with(through_three));
    assert!(through_three.shares_prefix_storage_with(cloned_through_three));
}

fn retained(history: &LogicalLogHistory, raw_index: u64) -> &LogPrefixWitness {
    history
        .prefixes_by_index_term
        .get(&(LogIndex(raw_index), Term(raw_index)))
        .expect("observed prefix is retained")
}
