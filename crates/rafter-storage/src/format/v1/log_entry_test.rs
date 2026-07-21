//! Version-1 persisted-log-entry envelope scenarios.

use super::*;
use crate::checksum::crc32;

fn entry(payload: &[u8]) -> PersistedRaftLogEntry {
    PersistedRaftLogEntry::application(LogIndex(42), Term(7), payload.to_vec())
}

fn stable_configuration_entry() -> PersistedRaftLogEntry {
    PersistedRaftLogEntry::configuration(
        LogIndex(42),
        Term(7),
        ConfigurationEntry::stable(
            ConfigurationId(3),
            MembershipSet::new(vec![NodeId(1), NodeId(2)], vec![NodeId(3)])
                .expect("membership is valid"),
        ),
    )
}

fn joint_configuration_entry() -> PersistedRaftLogEntry {
    PersistedRaftLogEntry::configuration(
        LogIndex(42),
        Term(7),
        ConfigurationEntry::joint(
            ConfigurationId(4),
            JointMembership::new(
                MembershipSet::new(vec![NodeId(1), NodeId(2)], vec![NodeId(3)])
                    .expect("old membership is valid"),
                MembershipSet::new(vec![NodeId(1), NodeId(3)], Vec::new())
                    .expect("new membership is valid"),
            ),
        ),
    )
}

fn replace_checksum(encoded: &mut [u8]) {
    let checksum_position = encoded.len() - 4;
    let checksum = crc32(&encoded[..checksum_position]);
    encoded[checksum_position..].copy_from_slice(&checksum.to_be_bytes());
}

#[test]
fn log_entry_round_trips_through_envelope() {
    let entry = entry(b"\0opaque raft payload\0");

    let encoded = encode_raft_log_entry(&entry).expect("entry encodes");

    assert_eq!(&encoded[..4], &RAFT_LOG_ENTRY_MAGIC);
    assert_eq!(encoded[4], RAFT_LOG_ENTRY_VERSION);
    assert_eq!(decode_raft_log_entry(&encoded), Ok(entry));
}

#[test]
fn stable_configuration_log_entry_round_trips_through_envelope() {
    let entry = stable_configuration_entry();

    let encoded = encode_raft_log_entry(&entry).expect("entry encodes");

    assert_eq!(decode_raft_log_entry(&encoded), Ok(entry));
}

#[test]
fn joint_configuration_log_entry_round_trips_through_envelope() {
    let entry = joint_configuration_entry();

    let encoded = encode_raft_log_entry(&entry).expect("entry encodes");

    assert_eq!(decode_raft_log_entry(&encoded), Ok(entry));
}

#[test]
fn noop_log_entry_round_trips_through_envelope() {
    let entry = PersistedRaftLogEntry::noop(LogIndex(42), Term(7));

    let encoded = encode_raft_log_entry(&entry).expect("entry encodes");

    assert_eq!(decode_raft_log_entry(&encoded), Ok(entry));
}

#[test]
fn empty_payload_log_entry_round_trips_through_envelope() {
    let entry = entry(b"");

    let encoded = encode_raft_log_entry(&entry).expect("entry encodes");

    assert_eq!(decode_raft_log_entry(&encoded), Ok(entry));
}

#[test]
fn larger_payload_log_entry_round_trips_through_envelope() {
    let payload: Vec<_> = (0..=255).cycle().take(1024).collect();
    let entry = entry(&payload);

    let encoded = encode_raft_log_entry(&entry).expect("entry encodes");

    assert_eq!(decode_raft_log_entry(&encoded), Ok(entry));
}

#[test]
fn decode_rejects_corrupt_log_entry_checksum() {
    let mut encoded = encode_raft_log_entry(&entry(b"command")).expect("entry encodes");
    let last_payload_byte = encoded.len() - 5;
    encoded[last_payload_byte] ^= 0xff;

    assert!(matches!(
        decode_raft_log_entry(&encoded),
        Err(DecodeRaftLogEntryError::ChecksumMismatch { .. })
    ));
}

#[test]
fn decode_rejects_invalid_magic_after_checksum_passes() {
    let mut encoded = encode_raft_log_entry(&entry(b"command")).expect("entry encodes");
    encoded[0] = b'X';
    replace_checksum(&mut encoded);

    assert_eq!(
        decode_raft_log_entry(&encoded),
        Err(DecodeRaftLogEntryError::InvalidMagic(*b"XFLE"))
    );
}

#[test]
fn decode_rejects_unsupported_version_after_checksum_passes() {
    let mut encoded = encode_raft_log_entry(&entry(b"command")).expect("entry encodes");
    encoded[4] = 99;
    replace_checksum(&mut encoded);

    assert_eq!(
        decode_raft_log_entry(&encoded),
        Err(DecodeRaftLogEntryError::UnsupportedVersion(99))
    );
}

#[test]
fn decode_rejects_truncated_log_entry() {
    assert_eq!(
        decode_raft_log_entry(&[]),
        Err(DecodeRaftLogEntryError::UnexpectedEof {
            needed: 4,
            remaining: 0,
        })
    );
}

#[test]
fn decode_rejects_truncated_payload_after_checksum_passes() {
    let mut encoded = encode_raft_log_entry(&entry(b"command")).expect("entry encodes");
    encoded[25] = 99;
    replace_checksum(&mut encoded);

    assert_eq!(
        decode_raft_log_entry(&encoded),
        Err(DecodeRaftLogEntryError::UnexpectedEof {
            needed: 99,
            remaining: 7,
        })
    );
}

#[test]
fn decode_rejects_trailing_bytes_after_checksum_passes() {
    let mut encoded = encode_raft_log_entry(&entry(b"command")).expect("entry encodes");
    encoded[25] = 3;
    replace_checksum(&mut encoded);

    assert_eq!(
        decode_raft_log_entry(&encoded),
        Err(DecodeRaftLogEntryError::TrailingBytes(4))
    );
}
