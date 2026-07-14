use super::*;
use rafter_invariant_test::oracle_assert;

fn snapshot(payload: &[u8]) -> PersistedRaftSnapshot {
    PersistedRaftSnapshot {
        metadata: RaftSnapshotMetadata::new(
            SnapshotGroupId::new("data-group-10").expect("valid group id"),
            NodeId(2),
            LogIndex(42),
            Term(7),
            Term(9),
            ApplicationSnapshotMetadata::new(
                ApplicationSnapshotKind::new("stream_data").expect("valid kind"),
                ApplicationSnapshotVersion::new(1).expect("valid version"),
            ),
        )
        .expect("valid metadata"),
        application_payload: payload.to_vec(),
    }
}

fn encode_snapshot(snapshot: &PersistedRaftSnapshot) -> Vec<u8> {
    encode_raft_snapshot(snapshot).expect("snapshot encodes")
}

fn snapshot_with_membership(payload: &[u8]) -> PersistedRaftSnapshot {
    PersistedRaftSnapshot {
        metadata: snapshot(payload)
            .metadata
            .with_committed_membership(MembershipConfig::joint(
                MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], vec![NodeId(4)])
                    .expect("old membership is valid"),
                MembershipSet::new(vec![NodeId(1), NodeId(3), NodeId(4)], Vec::new())
                    .expect("new membership is valid"),
            )),
        application_payload: payload.to_vec(),
    }
}

fn snapshot_with_committed_configuration(payload: &[u8]) -> PersistedRaftSnapshot {
    PersistedRaftSnapshot {
        metadata: snapshot(payload).metadata.with_committed_configuration(
            SnapshotCommittedConfiguration::new(
                Some(CommittedConfiguration {
                    index: LogIndex(40),
                    config_id: ConfigurationId(12),
                }),
                MembershipConfig::joint(
                    MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], vec![NodeId(4)])
                        .expect("old membership is valid"),
                    MembershipSet::new(vec![NodeId(1), NodeId(3), NodeId(4)], Vec::new())
                        .expect("new membership is valid"),
                ),
            ),
        ),
        application_payload: payload.to_vec(),
    }
}

fn replace_envelope_checksum(encoded: &mut [u8]) {
    let checksum_position = encoded.len() - 4;
    let checksum = crc32(&encoded[..checksum_position]);
    encoded[checksum_position..].copy_from_slice(&checksum.to_be_bytes());
}

fn payload_len_position(encoded: &[u8]) -> usize {
    let group_len = u16::from_be_bytes([encoded[5], encoded[6]]) as usize;
    let app_len_position = 7 + group_len + 8 + 8 + 8 + 8;
    let app_len =
        u16::from_be_bytes([encoded[app_len_position], encoded[app_len_position + 1]]) as usize;
    app_len_position + 2 + app_len + 2 + 1
}

fn payload_position(encoded: &[u8]) -> usize {
    payload_len_position(encoded) + 8
}

fn payload_checksum_position(encoded: &[u8]) -> usize {
    let len_position = payload_len_position(encoded);
    let payload_len = u64::from_be_bytes(
        encoded[len_position..len_position + 8]
            .try_into()
            .expect("payload length field is eight bytes"),
    );
    len_position + 8 + usize::try_from(payload_len).expect("test payloads fit usize")
}

#[test]
fn snapshot_round_trips_through_envelope() {
    let snapshot = snapshot(b"\0opaque application snapshot\0");

    let encoded = encode_snapshot(&snapshot);

    assert_eq!(&encoded[..4], &RAFT_SNAPSHOT_MAGIC);
    assert_eq!(encoded[4], RAFT_SNAPSHOT_VERSION);
    assert_eq!(decode_raft_snapshot(&encoded), Ok(snapshot));
}

#[test]
fn snapshot_with_committed_membership_round_trips_through_envelope() {
    let snapshot = snapshot_with_membership(b"\0membership snapshot\0");

    let encoded = encode_snapshot(&snapshot);

    assert_eq!(decode_raft_snapshot(&encoded), Ok(snapshot));
}

#[test]
fn snapshot_with_committed_configuration_identity_round_trips_through_envelope() {
    let snapshot = snapshot_with_committed_configuration(b"\0configuration snapshot\0");

    let encoded = encode_snapshot(&snapshot);

    assert_eq!(decode_raft_snapshot(&encoded), Ok(snapshot));
}

#[test]
fn encode_rejects_snapshot_membership_with_too_many_voters() {
    let voters = (1..=(u64::from(u16::MAX) + 1))
        .map(NodeId)
        .collect::<Vec<_>>();
    let membership = MembershipSet::new(voters, Vec::new()).expect("membership is valid");
    let snapshot = PersistedRaftSnapshot {
        metadata: snapshot(b"")
            .metadata
            .with_committed_membership(MembershipConfig::stable(membership)),
        application_payload: Vec::new(),
    };

    assert_eq!(
        encode_raft_snapshot(&snapshot),
        Err(EncodeRaftSnapshotError::TooManyMembers {
            member_kind: "voters",
            len: usize::from(u16::MAX) + 1,
        })
    );
}

#[test]
fn encode_rejects_snapshot_membership_with_too_many_learners() {
    let learners = (2..=(u64::from(u16::MAX) + 2))
        .map(NodeId)
        .collect::<Vec<_>>();
    let membership = MembershipSet::new(vec![NodeId(1)], learners).expect("membership is valid");
    let snapshot = PersistedRaftSnapshot {
        metadata: snapshot(b"")
            .metadata
            .with_committed_membership(MembershipConfig::stable(membership)),
        application_payload: Vec::new(),
    };

    assert_eq!(
        encode_raft_snapshot(&snapshot),
        Err(EncodeRaftSnapshotError::TooManyMembers {
            member_kind: "learners",
            len: usize::from(u16::MAX) + 1,
        })
    );
}

#[test]
fn empty_snapshot_payload_round_trips_through_envelope() {
    let snapshot = snapshot(b"");

    let encoded = encode_snapshot(&snapshot);

    assert_eq!(decode_raft_snapshot(&encoded), Ok(snapshot));
}

#[test]
fn decode_rejects_corrupt_snapshot_envelope_checksum() {
    let mut encoded = encode_snapshot(&snapshot(b"payload"));
    encoded[8] ^= 0xff;

    assert!(matches!(
        decode_raft_snapshot(&encoded),
        Err(DecodeRaftSnapshotError::EnvelopeChecksumMismatch { .. })
    ));
}

#[test]
fn decode_rejects_corrupt_snapshot_payload_checksum() {
    let mut encoded = encode_snapshot(&snapshot(b"payload"));
    let checksum_position = payload_checksum_position(&encoded);
    encoded[checksum_position] ^= 0xff;
    replace_envelope_checksum(&mut encoded);

    oracle_assert!(matches!(
        decode_raft_snapshot(&encoded),
        Err(DecodeRaftSnapshotError::PayloadChecksumMismatch { .. })
    ));
}

#[test]
fn decode_rejects_invalid_snapshot_magic_after_checksum_passes() {
    let mut encoded = encode_snapshot(&snapshot(b"payload"));
    encoded[0] = b'X';
    replace_envelope_checksum(&mut encoded);

    assert_eq!(
        decode_raft_snapshot(&encoded),
        Err(DecodeRaftSnapshotError::InvalidMagic(*b"XFSN"))
    );
}

#[test]
fn decode_rejects_unsupported_snapshot_version_after_checksum_passes() {
    let mut encoded = encode_snapshot(&snapshot(b"payload"));
    encoded[4] = RAFT_SNAPSHOT_VERSION + 1;
    replace_envelope_checksum(&mut encoded);

    assert_eq!(
        decode_raft_snapshot(&encoded),
        Err(DecodeRaftSnapshotError::UnsupportedVersion(
            RAFT_SNAPSHOT_VERSION + 1
        ))
    );
}

#[test]
fn decode_rejects_truncated_snapshot_payload_after_checksum_passes() {
    let mut encoded = encode_snapshot(&snapshot(b"payload"));
    let len_position = payload_len_position(&encoded);
    encoded[len_position..len_position + 8].copy_from_slice(&99_u64.to_be_bytes());
    replace_envelope_checksum(&mut encoded);

    assert_eq!(
        decode_raft_snapshot(&encoded),
        Err(DecodeRaftSnapshotError::UnexpectedEof {
            needed: 99,
            remaining: 11
        })
    );
}

#[test]
fn decode_rejects_trailing_snapshot_bytes_after_checksum_passes() {
    let mut encoded = encode_snapshot(&snapshot(b"payload"));
    let checksum_position = encoded.len() - 4;
    encoded.insert(checksum_position, 0xff);
    replace_envelope_checksum(&mut encoded);

    assert_eq!(
        decode_raft_snapshot(&encoded),
        Err(DecodeRaftSnapshotError::TrailingBytes(1))
    );
}

#[test]
fn decode_rejects_invalid_snapshot_metadata_after_checksum_passes() {
    let mut encoded = encode_snapshot(&snapshot(b"payload"));
    let payload_position = payload_position(&encoded);
    let last_included_index_position = 7 + "data-group-10".len() + 8;
    encoded[last_included_index_position..last_included_index_position + 8]
        .copy_from_slice(&0_u64.to_be_bytes());
    assert!(payload_position > last_included_index_position);
    replace_envelope_checksum(&mut encoded);

    assert_eq!(
        decode_raft_snapshot(&encoded),
        Err(DecodeRaftSnapshotError::InvalidMetadata(
            SnapshotMetadataError::ZeroLastIncludedIndex
        ))
    );
}

#[test]
fn snapshot_header_round_trips_multi_gigabyte_payload_length() {
    let metadata = snapshot(b"").metadata;
    let payload_len = 5 * 1024 * 1024 * 1024_u64 + 7;

    let header = encode_raft_snapshot_header(&metadata, payload_len).expect("header encodes");
    let decoded = decode_raft_snapshot_header(&header).expect("header decodes");

    assert_eq!(
        decoded,
        SnapshotEnvelopeHeader {
            metadata,
            payload_len,
            payload_crc32: 0,
            header_len: header.len() as u64,
        }
    );
}
