//! Domain-validation and canonical-reconstruction rejection contracts.

use rafter::{
    ConfigurationEntry, ConfigurationId, LogEntry, MembershipSet, MembershipValidationError,
    NodeId, SnapshotIdError, SnapshotMetadataError, Term,
};

use super::support::{
    append_entries_with, first_stable_membership_offset, rewrite_frame_checksum, snapshot_chunk,
    snapshot_chunk_hard_state_term_offset, snapshot_chunk_last_included_index_offset,
    snapshot_chunk_last_included_term_offset, snapshot_group_bytes_offset,
    snapshot_kind_length_offset, snapshot_metadata, snapshot_metadata_offset,
    snapshot_version_offset, stable_configuration_entry,
};
use crate::{decode_message, encode_message, DecodePeerMessageError};

const MAX_SNAPSHOT_ID_BYTES: usize = 128;

#[test]
fn decode_reports_every_snapshot_group_id_validation_class() {
    let mut empty = encoded_snapshot_chunk();
    let length_offset = snapshot_metadata_offset();
    write_u16(&mut empty, length_offset, 0);
    rewrite_frame_checksum(&mut empty);
    assert_eq!(
        decode_message(&empty),
        Err(DecodePeerMessageError::InvalidSnapshotGroupId(
            SnapshotIdError::Empty {
                field: "snapshot group id",
            },
        ))
    );

    let mut invalid = encoded_snapshot_chunk();
    invalid[snapshot_group_bytes_offset()] = b'!';
    rewrite_frame_checksum(&mut invalid);
    assert_eq!(
        decode_message(&invalid),
        Err(DecodePeerMessageError::InvalidSnapshotGroupId(
            SnapshotIdError::InvalidCharacter {
                field: "snapshot group id",
                character: '!',
            },
        ))
    );

    let mut too_long = encoded_snapshot_chunk();
    lengthen_string(&mut too_long, length_offset, MAX_SNAPSHOT_ID_BYTES + 1);
    assert_eq!(
        decode_message(&too_long),
        Err(DecodePeerMessageError::InvalidSnapshotGroupId(
            SnapshotIdError::TooLong {
                field: "snapshot group id",
                len: MAX_SNAPSHOT_ID_BYTES + 1,
            },
        ))
    );
}

#[test]
fn decode_reports_every_application_snapshot_kind_validation_class() {
    let mut empty = encoded_snapshot_chunk();
    let length_offset = snapshot_kind_length_offset(&empty);
    write_u16(&mut empty, length_offset, 0);
    rewrite_frame_checksum(&mut empty);
    assert_eq!(
        decode_message(&empty),
        Err(DecodePeerMessageError::InvalidApplicationSnapshotKind(
            SnapshotIdError::Empty {
                field: "application snapshot kind",
            },
        ))
    );

    let mut invalid = encoded_snapshot_chunk();
    let first_byte = snapshot_kind_length_offset(&invalid) + 2;
    invalid[first_byte] = b'!';
    rewrite_frame_checksum(&mut invalid);
    assert_eq!(
        decode_message(&invalid),
        Err(DecodePeerMessageError::InvalidApplicationSnapshotKind(
            SnapshotIdError::InvalidCharacter {
                field: "application snapshot kind",
                character: '!',
            },
        ))
    );

    let mut too_long = encoded_snapshot_chunk();
    let length_offset = snapshot_kind_length_offset(&too_long);
    lengthen_string(&mut too_long, length_offset, MAX_SNAPSHOT_ID_BYTES + 1);
    assert_eq!(
        decode_message(&too_long),
        Err(DecodePeerMessageError::InvalidApplicationSnapshotKind(
            SnapshotIdError::TooLong {
                field: "application snapshot kind",
                len: MAX_SNAPSHOT_ID_BYTES + 1,
            },
        ))
    );
}

#[test]
fn decode_reports_zero_application_snapshot_version() {
    let mut encoded = encoded_snapshot_chunk();
    let version_offset = snapshot_version_offset(&encoded);
    write_u16(&mut encoded, version_offset, 0);
    rewrite_frame_checksum(&mut encoded);
    assert_eq!(
        decode_message(&encoded),
        Err(DecodePeerMessageError::InvalidApplicationSnapshotVersion(
            SnapshotMetadataError::ZeroApplicationSnapshotVersion,
        ))
    );
}

#[test]
fn decode_reports_every_snapshot_boundary_validation_class() {
    let mut zero_index = encoded_snapshot_chunk();
    let index_offset = snapshot_chunk_last_included_index_offset(&zero_index);
    write_u64(&mut zero_index, index_offset, 0);
    rewrite_frame_checksum(&mut zero_index);
    assert_eq!(
        decode_message(&zero_index),
        Err(DecodePeerMessageError::InvalidSnapshotMetadata(
            SnapshotMetadataError::ZeroLastIncludedIndex,
        ))
    );

    let mut maximum_index = encoded_snapshot_chunk();
    let index_offset = snapshot_chunk_last_included_index_offset(&maximum_index);
    write_u64(&mut maximum_index, index_offset, u64::MAX);
    rewrite_frame_checksum(&mut maximum_index);
    assert_eq!(
        decode_message(&maximum_index),
        Err(DecodePeerMessageError::InvalidSnapshotMetadata(
            SnapshotMetadataError::LastIncludedIndexAtMaximum,
        ))
    );

    let mut zero_term = encoded_snapshot_chunk();
    let term_offset = snapshot_chunk_last_included_term_offset(&zero_term);
    write_u64(&mut zero_term, term_offset, 0);
    rewrite_frame_checksum(&mut zero_term);
    assert_eq!(
        decode_message(&zero_term),
        Err(DecodePeerMessageError::InvalidSnapshotMetadata(
            SnapshotMetadataError::ZeroLastIncludedTerm {
                last_included_index: rafter::LogIndex(42),
            },
        ))
    );

    let mut term_ahead = encoded_snapshot_chunk();
    let hard_state_offset = snapshot_chunk_hard_state_term_offset(&term_ahead);
    write_u64(&mut term_ahead, hard_state_offset, 7);
    rewrite_frame_checksum(&mut term_ahead);
    assert_eq!(
        decode_message(&term_ahead),
        Err(DecodePeerMessageError::InvalidSnapshotMetadata(
            SnapshotMetadataError::SnapshotTermAheadOfHardState {
                last_included_index: rafter::LogIndex(42),
                last_included_term: Term(8),
                hard_state_term: Term(7),
            },
        ))
    );
}

#[test]
fn decode_reports_every_voter_validation_class() {
    let encoded = encode_message(&append_entries_with(vec![stable_configuration_entry()]))
        .expect("configuration entry encodes");
    let membership_offset = first_stable_membership_offset();

    let mut empty = encoded.clone();
    write_u16(&mut empty, membership_offset, 0);
    rewrite_frame_checksum(&mut empty);
    assert_eq!(
        decode_message(&empty),
        Err(DecodePeerMessageError::InvalidMembership(
            MembershipValidationError::EmptyVoters,
        ))
    );

    let first_voter = membership_offset + 2;
    let second_voter = first_voter + 8;
    let mut duplicate = encoded.clone();
    write_u64(&mut duplicate, second_voter, 1);
    rewrite_frame_checksum(&mut duplicate);
    assert_eq!(
        decode_message(&duplicate),
        Err(DecodePeerMessageError::InvalidMembership(
            MembershipValidationError::DuplicateVoter { node_id: NodeId(1) },
        ))
    );

    let mut descending = encoded;
    write_u64(&mut descending, first_voter, 2);
    write_u64(&mut descending, second_voter, 1);
    rewrite_frame_checksum(&mut descending);
    assert_eq!(
        decode_message(&descending),
        Err(DecodePeerMessageError::NonCanonicalMembershipOrder {
            field: "membership_voters",
        })
    );
}

#[test]
fn decode_reports_every_learner_validation_class() {
    let encoded = encoded_membership_with_two_learners();
    let learner_count = first_stable_membership_offset() + 2 + 2 * 8;
    let first_learner = learner_count + 2;
    let second_learner = first_learner + 8;

    let mut duplicate = encoded.clone();
    write_u64(&mut duplicate, second_learner, 3);
    rewrite_frame_checksum(&mut duplicate);
    assert_eq!(
        decode_message(&duplicate),
        Err(DecodePeerMessageError::InvalidMembership(
            MembershipValidationError::DuplicateLearner { node_id: NodeId(3) },
        ))
    );

    let mut descending = encoded.clone();
    write_u64(&mut descending, first_learner, 4);
    write_u64(&mut descending, second_learner, 3);
    rewrite_frame_checksum(&mut descending);
    assert_eq!(
        decode_message(&descending),
        Err(DecodePeerMessageError::NonCanonicalMembershipOrder {
            field: "membership_learners",
        })
    );

    let mut overlap = encoded;
    write_u64(&mut overlap, first_learner, 1);
    rewrite_frame_checksum(&mut overlap);
    assert_eq!(
        decode_message(&overlap),
        Err(DecodePeerMessageError::InvalidMembership(
            MembershipValidationError::LearnerVoterOverlap { node_id: NodeId(1) },
        ))
    );
}

fn encoded_snapshot_chunk() -> Vec<u8> {
    encode_message(&snapshot_chunk(snapshot_metadata(None))).expect("snapshot chunk encodes")
}

fn encoded_membership_with_two_learners() -> Vec<u8> {
    let membership = MembershipSet::new(vec![NodeId(1), NodeId(2)], vec![NodeId(3), NodeId(4)])
        .expect("membership is valid before byte mutation");
    let entry = LogEntry::configuration(
        Term(8),
        ConfigurationEntry::stable(ConfigurationId(1), membership),
    );
    encode_message(&append_entries_with(vec![entry])).expect("configuration entry encodes")
}

fn lengthen_string(encoded: &mut Vec<u8>, length_offset: usize, target_len: usize) {
    let original_len = u16::from_be_bytes(
        encoded[length_offset..length_offset + 2]
            .try_into()
            .expect("string length is encoded"),
    ) as usize;
    assert!(target_len > original_len);
    let insertion_offset = length_offset + 2 + original_len;
    encoded.splice(
        insertion_offset..insertion_offset,
        vec![b'a'; target_len - original_len],
    );
    let encoded_len = u16::try_from(target_len).expect("test string length fits the wire prefix");
    write_u16(encoded, length_offset, encoded_len);
    rewrite_frame_checksum(encoded);
}

fn write_u16(encoded: &mut [u8], offset: usize, value: u16) {
    encoded[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

fn write_u64(encoded: &mut [u8], offset: usize, value: u64) {
    encoded[offset..offset + 8].copy_from_slice(&value.to_be_bytes());
}
