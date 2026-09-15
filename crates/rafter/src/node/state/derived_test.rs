//! Synchronization and protocol queries for recomputable configuration indexes.

use super::derived::ConfigurationIndex;
use crate::{ConfigurationEntry, ConfigurationId, LogEntry, LogIndex, MembershipSet, NodeId, Term};

#[test]
fn configuration_index_owns_append_compaction_and_truncate_updates() {
    let application_one = LogEntry::application(Term(1), b"one".to_vec());
    let configuration_seven = LogEntry::configuration(Term(1), configuration(7));
    let application_three = LogEntry::application(Term(1), b"three".to_vec());
    let configuration_eight = LogEntry::configuration(Term(1), configuration(8));
    let mut index = ConfigurationIndex::default();

    index.record_append(0, &application_one);
    index.record_append(1, &configuration_seven);
    index.record_append(2, &application_three);
    index.record_append(3, &configuration_eight);

    assert!(index
        .effective_entry(&[
            application_one.clone(),
            configuration_seven,
            application_three.clone(),
            configuration_eight.clone(),
        ])
        .is_some());

    index.compact_prefix(2);

    assert_eq!(
        index
            .effective_entry(&[application_three.clone(), configuration_eight])
            .map(ConfigurationEntry::config_id),
        Some(ConfigurationId(8)),
    );

    index.truncate(1);

    assert!(index.effective_entry(&[application_three]).is_none());
}

#[test]
fn configuration_index_resolves_log_indexes_without_exposing_offsets() {
    let log = vec![
        LogEntry::application(Term(1), b"one".to_vec()),
        LogEntry::configuration(Term(1), configuration(8)),
        LogEntry::application(Term(1), b"three".to_vec()),
        LogEntry::configuration(Term(2), configuration(9)),
    ];
    let index = ConfigurationIndex::from_log(&log);
    let first_log_index = LogIndex(5);

    assert_eq!(
        index
            .entry_at_or_before(first_log_index, &log, LogIndex(7))
            .map(ConfigurationEntry::config_id),
        Some(ConfigurationId(8)),
    );
    assert_eq!(
        index
            .committed_state_at(first_log_index, &log, LogIndex(7))
            .map(|state| state.index),
        Some(LogIndex(6)),
    );
    assert_eq!(
        index.indexes_after(first_log_index, LogIndex(6)),
        vec![LogIndex(8)],
    );
}

fn configuration(config_id: u64) -> ConfigurationEntry {
    let membership =
        MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("single-voter membership is valid");
    ConfigurationEntry::stable(ConfigurationId(config_id), membership)
}
