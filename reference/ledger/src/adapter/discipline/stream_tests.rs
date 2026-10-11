//! Streamed decoding must reject bad identity, bytes, and schema before publication.

#[path = "stream_tests/support.rs"]
mod support;

use rafter::{ApplicationSnapshotKind, ApplicationSnapshotVersion};

use super::codec::snapshot_payload_limit as payload_limit;
use super::*;
use crate::{AccountId, LedgerQuery, LedgerQueryResult, SnapshotError};
use support::{descriptor, payload, promoted, Generated, Source, ACCOUNTS, FLOOR, LENGTH};

fn config() -> LedgerConfig {
    LedgerConfig::new(2, 4).unwrap()
}

fn accept(bytes: Vec<u8>, config: LedgerConfig) -> Result<Ledger, LedgerAdapterError> {
    let source = Source::new(bytes);
    admit_install(
        &promoted(descriptor(&source.bytes)),
        LogIndex(7),
        config,
        Some(&source),
    )
}

#[test]
fn large_schema_decodes_with_bounded_chunks_into_one_temporary_model() {
    let source = Generated::new();
    let ledger = admit_install(
        &promoted(source.snapshot.clone()),
        LogIndex(7),
        LedgerConfig::new(1, ACCOUNTS as usize).unwrap(),
        Some(&source),
    )
    .unwrap();
    assert_eq!(ledger.summary().open_accounts, ACCOUNTS as usize);
    assert_eq!(ledger.summary().total_balance, u128::from(ACCOUNTS));
    assert_eq!(ledger.summary().successful_deposits, u128::from(ACCOUNTS));
    let last = AccountId::new(u64::from(ACCOUNTS) - 1);
    assert_eq!(
        ledger.query(LedgerQuery::GetAccount { account_id: last }),
        LedgerQueryResult::Account {
            account_id: last,
            balance: Some(1)
        }
    );
    assert_eq!(source.calls.get(), 129);
    assert_eq!(source.largest.get(), 16 * 1024);
    assert_eq!(source.snapshot.application_payload_len, LENGTH);
}

#[test]
fn checksum_and_chunk_contract_failures_cannot_return_installable_state() {
    for mode in 0..3 {
        let mut source = Source::new(payload(&[(1, 5)], &[(0, 1)]));
        let mut snapshot = descriptor(&source.bytes);
        match mode {
            0 => snapshot.application_payload_crc32 ^= 1,
            1 => source.extra_capacity = true,
            _ => source.short = true,
        }
        assert!(matches!(
            admit_install(&promoted(snapshot), LogIndex(7), config(), Some(&source)),
            Err(LedgerAdapterError::SnapshotPayloadUnavailable {
                applied_index: FLOOR
            })
        ));
        assert_eq!(source.calls.get(), 1);
    }
}

#[test]
fn descriptor_identity_and_byte_limits_refuse_before_source_effects() {
    for mode in 0..4 {
        let source = Source::new(payload(&[(1, 5)], &[(0, 1)]));
        let mut snapshot = descriptor(&source.bytes);
        match mode {
            0 => snapshot.metadata.last_included_index = LogIndex(FLOOR.0 + 1),
            1 => {
                snapshot.metadata.application.kind = ApplicationSnapshotKind::new("other").unwrap();
            }
            2 => {
                snapshot.metadata.application.version = ApplicationSnapshotVersion::new(2).unwrap();
            }
            _ => snapshot.application_payload_len = payload_limit(config()) + 1,
        }
        let snapshot = promoted(snapshot);
        assert!(!install_needs_source(&snapshot, LogIndex(7), config()));
        assert!(matches!(
            admit_install(&snapshot, LogIndex(7), config(), Some(&source)),
            Err(LedgerAdapterError::SnapshotPayloadUnavailable { .. })
        ));
        assert_eq!(source.calls.get(), 0);
    }
}

#[test]
fn configured_counts_are_checked_before_invalid_record_fields() {
    // The duplicate account and zero epoch would fail if records were decoded.
    assert_eq!(
        accept(
            payload(&[(1, 5), (1, 5)], &[]),
            LedgerConfig::new(100, 1).unwrap()
        ),
        Err(LedgerAdapterError::Snapshot(SnapshotError::TooManyAccounts))
    );
    assert_eq!(
        accept(
            payload(&[], &[(0, 0), (0, 0)]),
            LedgerConfig::new(1, 100).unwrap()
        ),
        Err(LedgerAdapterError::Snapshot(SnapshotError::TooManySessions))
    );
}

#[test]
fn oversized_account_count_is_refused_before_reading_a_second_chunk() {
    let source = Generated::new();
    // A large session budget admits the descriptor, but the account budget
    // must stop decoding at its count field, before reading 1,025 records.
    assert!(matches!(
        admit_install(
            &promoted(source.snapshot.clone()),
            LogIndex(7),
            LedgerConfig::new(u32::MAX, 1024).unwrap(),
            Some(&source)
        ),
        Err(LedgerAdapterError::Snapshot(SnapshotError::TooManyAccounts))
    ));
    assert_eq!(source.calls.get(), 1);
}

#[test]
fn borrowed_and_streamed_frames_share_schema_and_model_refusals() {
    let good = payload(&[(1, 5)], &[(0, 1)]);
    let mut wrong_version = good.clone();
    wrong_version[0] = 9;
    let mut supply = good.clone();
    supply[24] ^= 1;
    let mut floor = good.clone();
    floor[8] ^= 1;
    let mut trailing = good.clone();
    trailing.push(0);
    for bytes in [
        wrong_version,
        supply,
        floor,
        trailing,
        good[..good.len() - 1].to_vec(),
        payload(&[(1, 5), (1, 5)], &[]),
        payload(&[], &[(0, 1), (0, 1)]),
        payload(&[], &[(2, 1)]),
        payload(&[], &[(0, 0)]),
    ] {
        let inline = admit_install(
            &ApplicationSnapshot {
                applied_index: FLOOR,
                payload: bytes.clone(),
                raft_snapshot: None,
            },
            LogIndex(7),
            config(),
            None,
        );
        assert!(inline.is_err());
        assert_eq!(accept(bytes, config()), inline);
    }
}

#[test]
fn largest_cached_record_fits_the_configured_byte_budget_and_round_trips() {
    let mut bytes = payload(&[(1, 0)], &[(0, 1)]);
    *bytes.last_mut().unwrap() = 1; // cached completion
    bytes.extend_from_slice(&1_u64.to_be_bytes()); // sequence
    bytes.push(3); // transfer
    for value in [1_u64, 2, 1] {
        bytes.extend_from_slice(&value.to_be_bytes());
    }
    bytes.push(3); // transferred result
    bytes.extend_from_slice(&0_u64.to_be_bytes());
    bytes.extend_from_slice(&0_u64.to_be_bytes());
    let config = LedgerConfig::new(1, 1).unwrap();
    assert_eq!(bytes.len() as u64, payload_limit(config));
    let ledger = accept(bytes.clone(), config).unwrap();
    assert_eq!(
        codec::encode_snapshot(FLOOR.0, &ledger.snapshot()).unwrap(),
        bytes
    );
    assert_eq!(
        payload_limit(LedgerConfig::new(u32::MAX, usize::MAX).unwrap()),
        33 + 79 * u64::from(u32::MAX)
    );
}
