//! Synchronization and protocol queries for recomputable configuration indexes.

use super::derived::ConfigurationIndex;
use crate::{ConfigurationEntry, ConfigurationId, LogEntry, LogIndex, MembershipSet, NodeId, Term};

#[test]
fn configuration_index_owns_append_and_truncate_updates() {
    let application = LogEntry::application(Term(1), b"one".to_vec());
    let configuration = LogEntry::configuration(Term(1), configuration(7));
    let mut index = ConfigurationIndex::default();

    index.record_append(0, &application);
    index.record_append(1, &configuration);

    assert!(index
        .effective_entry(&[application.clone(), configuration.clone()])
        .is_some());

    index.truncate(1);

    assert!(index.effective_entry(&[application]).is_none());
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
