//! Persistent-prefix representation tests.

use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

use rafter::{LogEntry, LogEntryKind, LogIndex, Term};

use super::LogPrefixWitness;

#[test]
fn equal_independent_prefixes_have_exact_equality_and_hashes() {
    let left = witness(&[b"one", b"two"]);
    let right = witness(&[b"one", b"two"]);

    assert_eq!(left, right);
    assert_eq!(hash(&left), hash(&right));
    assert!(!left.shares_prefix_storage_with(&right));
}

#[test]
fn divergent_descendants_do_not_change_shared_prefix_identity() {
    let shared = witness(&[b"one"]);
    let left = shared
        .extend(LogIndex(2), entry(b"left"))
        .expect("sequential extension");
    let right = shared
        .extend(LogIndex(2), entry(b"right"))
        .expect("sequential extension");

    assert!(shared.shares_prefix_storage_with(&left));
    assert!(shared.shares_prefix_storage_with(&right));
    assert_ne!(left, right);
}

#[test]
fn fixture_constructor_rejects_incomplete_prefixes() {
    assert!(
        LogPrefixWitness::from_entries(LogIndex(3), vec![entry(b"one"), entry(b"two")]).is_none()
    );
}

#[test]
fn unique_deep_spine_drops_iteratively() {
    let mut prefix = LogPrefixWitness::default();
    for raw_index in 1..=32_768 {
        let index = LogIndex(raw_index);
        prefix = prefix
            .extend(index, entry(&raw_index.to_le_bytes()))
            .expect("sequential extension");
    }
    drop(prefix);
}

fn witness(payloads: &[&[u8]]) -> LogPrefixWitness {
    LogPrefixWitness::from_entries(
        LogIndex(u64::try_from(payloads.len()).expect("test length fits u64")),
        payloads.iter().map(|payload| entry(payload)).collect(),
    )
    .expect("complete fixture prefix")
}

fn entry(payload: &[u8]) -> LogEntry {
    LogEntry {
        term: Term(1),
        kind: LogEntryKind::application(payload.to_vec()),
    }
}

fn hash(value: &LogPrefixWitness) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}
