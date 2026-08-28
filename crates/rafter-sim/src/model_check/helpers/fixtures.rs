//! Bootstrap and snapshot fixtures shared by the model-check legs.
//!
//! These builders produce the exact durable images explorers seed nodes from:
//! a plain log prefix, a prefix behind an installed snapshot, and the snapshot
//! descriptor plus payload bytes that must be registered together.

use rafter::{BootstrapLogEntry, BootstrapState, LogIndex, MembershipConfig, RaftSnapshot, Term};

use super::test_snapshot_metadata;

pub(in crate::model_check) fn bootstrap_state(
    current_term: Term,
    entries: &[(u64, Term, &[u8])],
) -> BootstrapState {
    BootstrapState {
        current_term,
        voted_for: None,
        commit_index: LogIndex::ZERO,
        committed_configuration: None,
        snapshot: None,
        log: entries
            .iter()
            .map(|(index, term, payload)| {
                BootstrapLogEntry::application(LogIndex(*index), *term, (*payload).to_vec())
            })
            .collect(),
    }
}

pub(in crate::model_check) fn bootstrap_with_snapshot(
    current_term: Term,
    snapshot: RaftSnapshot,
    entries: &[(u64, Term, &[u8])],
) -> BootstrapState {
    BootstrapState {
        current_term,
        voted_for: None,
        commit_index: LogIndex::ZERO,
        committed_configuration: None,
        snapshot: Some(snapshot),
        log: entries
            .iter()
            .map(|(index, term, payload)| {
                BootstrapLogEntry::application(LogIndex(*index), *term, (*payload).to_vec())
            })
            .collect(),
    }
}

/// Builds a snapshot descriptor for `payload` and returns both; the caller
/// seeds the payload into each node whose store must hold the content.
pub(in crate::model_check) fn test_snapshot(
    writer_id: u64,
    last_included_index: u64,
    last_included_term: u64,
    hard_state_term: u64,
    payload: &[u8],
) -> (RaftSnapshot, Vec<u8>) {
    let metadata = test_snapshot_metadata(
        writer_id,
        last_included_index,
        last_included_term,
        hard_state_term,
    );
    let snapshot = RaftSnapshot::from_payload(metadata, payload);
    (snapshot, payload.to_vec())
}

pub(in crate::model_check) fn test_snapshot_with_committed_membership(
    writer_id: u64,
    last_included_index: u64,
    last_included_term: u64,
    hard_state_term: u64,
    payload: &[u8],
    membership: MembershipConfig,
) -> (RaftSnapshot, Vec<u8>) {
    let metadata = test_snapshot_metadata(
        writer_id,
        last_included_index,
        last_included_term,
        hard_state_term,
    )
    .with_committed_membership(membership);
    let snapshot = RaftSnapshot::from_payload(metadata, payload);
    (snapshot, payload.to_vec())
}

pub(in crate::model_check) fn large_snapshot_payload() -> Vec<u8> {
    let mut payload = Vec::with_capacity(70 * 1024);
    while payload.len() < 70 * 1024 {
        payload.extend_from_slice(b"snapshot-model-check-payload");
    }
    payload.truncate(70 * 1024);
    payload
}
