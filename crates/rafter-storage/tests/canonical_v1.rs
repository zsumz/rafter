//! Canonical version-1 storage decoding.
//!
//! These scenarios mutate checksum-valid envelopes into alternate byte
//! representations of the same logical value. Version-1 decoders reject them
//! so every accepted artifact has one stable encoding.

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    ConfigurationEntry, ConfigurationId, LogIndex, MembershipConfig, MembershipSet, NodeId,
    RaftSnapshotMetadata, SnapshotGroupId, Term,
};
use rafter_storage::{
    crc32, decode_raft_hard_state, decode_raft_log_entry, decode_raft_snapshot,
    encode_raft_hard_state, encode_raft_log_entry, encode_raft_snapshot, DecodeRaftHardStateError,
    DecodeRaftLogEntryError, DecodeRaftSnapshotError, PersistedRaftLogEntry, PersistedRaftSnapshot,
    RaftHardState,
};

const CHECKSUM_LEN: usize = 4;
const HARD_STATE_VOTED_FOR_NODE_OFFSET: usize = 4 + 1 + 8 + 1;
const HARD_STATE_COMMITTED_CONFIGURATION_INDEX_OFFSET: usize = 4 + 1 + 8 + 1 + 8 + 8 + 1;
const LOG_STABLE_VOTERS_OFFSET: usize = 4 + 1 + 8 + 8 + 1 + 8 + 4;

#[test]
fn hard_state_rejects_nonzero_absent_vote_field() {
    let mut encoded = encode_raft_hard_state(&hard_state_without_optional_values());
    encoded[HARD_STATE_VOTED_FOR_NODE_OFFSET..HARD_STATE_VOTED_FOR_NODE_OFFSET + 8]
        .copy_from_slice(&9_u64.to_be_bytes());
    rewrite_checksum(&mut encoded);

    assert_eq!(
        decode_raft_hard_state(&encoded),
        Err(DecodeRaftHardStateError::NonCanonicalAbsentVotedFor(
            NodeId(9),
        ))
    );
}

#[test]
fn hard_state_rejects_nonzero_absent_committed_configuration_fields() {
    let mut encoded = encode_raft_hard_state(&hard_state_without_optional_values());
    let index_offset = HARD_STATE_COMMITTED_CONFIGURATION_INDEX_OFFSET;
    encoded[index_offset..index_offset + 8].copy_from_slice(&11_u64.to_be_bytes());
    encoded[index_offset + 8..index_offset + 16].copy_from_slice(&17_u64.to_be_bytes());
    rewrite_checksum(&mut encoded);

    assert_eq!(
        decode_raft_hard_state(&encoded),
        Err(
            DecodeRaftHardStateError::NonCanonicalAbsentCommittedConfiguration {
                index: LogIndex(11),
                config_id: ConfigurationId(17),
            }
        )
    );
}

#[test]
fn log_entry_rejects_valid_membership_stored_out_of_order() {
    let membership = MembershipSet::new(vec![NodeId(1), NodeId(2)], vec![NodeId(3)])
        .expect("membership is valid");
    let entry = PersistedRaftLogEntry::configuration(
        LogIndex(42),
        Term(7),
        ConfigurationEntry::stable(ConfigurationId(4), membership),
    );
    let mut encoded = encode_raft_log_entry(&entry).expect("entry encodes");
    encoded[LOG_STABLE_VOTERS_OFFSET..LOG_STABLE_VOTERS_OFFSET + 8]
        .copy_from_slice(&2_u64.to_be_bytes());
    encoded[LOG_STABLE_VOTERS_OFFSET + 8..LOG_STABLE_VOTERS_OFFSET + 16]
        .copy_from_slice(&1_u64.to_be_bytes());
    rewrite_checksum(&mut encoded);

    assert_eq!(
        decode_raft_log_entry(&encoded),
        Err(DecodeRaftLogEntryError::NonCanonicalMembershipOrder {
            member_kind: "voters",
            previous: NodeId(2),
            actual: NodeId(1),
        })
    );
}

#[test]
fn snapshot_rejects_valid_membership_stored_out_of_order() {
    let membership = MembershipSet::new(vec![NodeId(1)], vec![NodeId(2), NodeId(3)])
        .expect("membership is valid");
    let metadata =
        snapshot_metadata().with_committed_membership(MembershipConfig::stable(membership));
    let snapshot = PersistedRaftSnapshot {
        metadata,
        application_payload: Vec::new(),
    };
    let mut encoded = encode_raft_snapshot(&snapshot).expect("snapshot encodes");
    let learner_offset = snapshot_first_learner_offset(&encoded);
    encoded[learner_offset..learner_offset + 8].copy_from_slice(&3_u64.to_be_bytes());
    encoded[learner_offset + 8..learner_offset + 16].copy_from_slice(&2_u64.to_be_bytes());
    rewrite_checksum(&mut encoded);

    assert_eq!(
        decode_raft_snapshot(&encoded),
        Err(DecodeRaftSnapshotError::NonCanonicalMembershipOrder {
            member_kind: "learners",
            previous: NodeId(3),
            actual: NodeId(2),
        })
    );
}

fn hard_state_without_optional_values() -> RaftHardState {
    RaftHardState {
        current_term: Term(7),
        voted_for: None,
        commit_index: LogIndex(3),
        committed_configuration: None,
    }
}

fn snapshot_metadata() -> RaftSnapshotMetadata {
    RaftSnapshotMetadata::new(
        SnapshotGroupId::new("canonical-group").expect("group id is valid"),
        NodeId(1),
        LogIndex(42),
        Term(7),
        Term(9),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("canonical_state").expect("kind is valid"),
            ApplicationSnapshotVersion::new(1).expect("version is valid"),
        ),
    )
    .expect("snapshot metadata is valid")
}

fn snapshot_first_learner_offset(encoded: &[u8]) -> usize {
    let mut position = 4 + 1;
    let group_id_len = read_u16(encoded, position);
    position += 2 + group_id_len;
    position += 8 * 4; // writer, boundary index/term, and hard-state term
    let application_kind_len = read_u16(encoded, position);
    position += 2 + application_kind_len;
    position += 2; // application version

    assert_eq!(encoded[position], 1, "committed membership is present");
    position += 1;
    assert_eq!(encoded[position], 0, "configuration identity is absent");
    position += 1;
    assert_eq!(encoded[position], 0, "membership is stable");
    position += 1;

    let voter_count = read_u16(encoded, position);
    position += 2 + voter_count * 8;
    let learner_count = read_u16(encoded, position);
    assert_eq!(learner_count, 2);
    position + 2
}

fn read_u16(bytes: &[u8], offset: usize) -> usize {
    usize::from(u16::from_be_bytes([bytes[offset], bytes[offset + 1]]))
}

fn rewrite_checksum(encoded: &mut [u8]) {
    let checksum_offset = encoded.len() - CHECKSUM_LEN;
    let checksum = crc32(&encoded[..checksum_offset]);
    encoded[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
}
