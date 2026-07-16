//! Shared message fixtures and byte-mutation helpers for codec scenarios.

use rafter::{
    AppendEntries, ApplicationSnapshotKind, ApplicationSnapshotMetadata,
    ApplicationSnapshotVersion, ConfigurationEntry, ConfigurationId, InstallSnapshotChunk,
    JointMembership, LogEntry, LogEntryKind, LogIndex, MembershipConfig, MembershipSet, Message,
    NodeId, RaftSnapshot, RaftSnapshotMetadata, RequestVote, SnapshotGroupId, SnapshotTransferId,
    Term,
};
use rafter_crc32::crc32;

use crate::{decode_message, encode_message, MAGIC, VERSION};

pub(super) fn round_trip(message: Message) {
    let encoded = encode_message(&message).expect("message encodes");
    assert_eq!(&encoded[..4], &MAGIC);
    assert_eq!(encoded[4], VERSION);
    assert_eq!(decode_message(&encoded), Ok(message));
}

pub(super) fn vote_request() -> Message {
    Message::RequestVote(RequestVote {
        term: Term(7),
        candidate_id: NodeId(2),
        last_log_index: LogIndex(55),
        last_log_term: Term(6),
    })
}

pub(super) fn append_entries() -> Message {
    append_entries_with(vec![LogEntry::application(
        Term(8),
        b"stream command bytes".to_vec(),
    )])
}

pub(super) fn append_entries_with(entries: Vec<LogEntry>) -> Message {
    Message::AppendEntries(AppendEntries {
        sequence: 0,
        term: Term(8),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(10),
        prev_log_term: Term(7),
        entries: entries.into(),
        leader_commit: LogIndex(11),
    })
}

pub(super) fn stable_configuration_entry() -> LogEntry {
    LogEntry::configuration(
        Term(8),
        ConfigurationEntry::stable(
            ConfigurationId(1),
            MembershipSet::new(vec![NodeId(1), NodeId(2)], vec![NodeId(3)])
                .expect("stable membership is valid"),
        ),
    )
}

pub(super) fn joint_configuration_entry() -> LogEntry {
    let old = MembershipSet::new(vec![NodeId(1), NodeId(2)], vec![NodeId(3)])
        .expect("old membership is valid");
    let new = MembershipSet::new(vec![NodeId(1), NodeId(3)], Vec::new())
        .expect("new membership is valid");
    LogEntry::configuration(
        Term(8),
        ConfigurationEntry::joint(ConfigurationId(2), JointMembership::new(old, new)),
    )
}

pub(super) fn application_payload(entry: &LogEntry) -> &rafter::SharedPayload {
    match &entry.kind {
        LogEntryKind::Application(payload) => payload,
        LogEntryKind::Configuration(_) | LogEntryKind::Noop => {
            panic!("test entry should be application payload")
        }
    }
}

pub(super) fn snapshot_metadata(membership: Option<MembershipConfig>) -> RaftSnapshotMetadata {
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("data-group-10").expect("valid group id"),
        NodeId(1),
        LogIndex(42),
        Term(8),
        Term(9),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("stream_data").expect("valid kind"),
            ApplicationSnapshotVersion::new(1).expect("valid version"),
        ),
    )
    .expect("valid snapshot metadata");
    membership.map_or(metadata.clone(), |membership| {
        metadata.with_committed_membership(membership)
    })
}

pub(super) fn test_snapshot() -> RaftSnapshot {
    let membership = MembershipConfig::stable(
        MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], vec![NodeId(4)])
            .expect("membership is valid"),
    );
    RaftSnapshot::from_payload(snapshot_metadata(Some(membership)), b"snapshot bytes")
}

pub(super) fn snapshot_chunk(metadata: RaftSnapshotMetadata) -> Message {
    Message::InstallSnapshotChunk(InstallSnapshotChunk {
        term: Term(9),
        leader_id: NodeId(1),
        transfer_id: SnapshotTransferId(123_456),
        metadata,
        total_payload_len: 14,
        application_payload_crc32: 0x1234_abcd,
        offset: 0,
        chunk: b"snapshot bytes".to_vec(),
        done: true,
    })
}

pub(super) fn first_append_entry_kind_offset() -> usize {
    4 + 1 + 1 + 8 + 8 + 8 + 8 + 4 + 8
}

pub(super) fn first_stable_membership_offset() -> usize {
    first_append_entry_kind_offset() + 1 + 8
}

pub(super) fn snapshot_metadata_offset() -> usize {
    4 + 1 + 1 + 8 + 8 + 8
}

pub(super) fn snapshot_group_bytes_offset() -> usize {
    snapshot_metadata_offset() + 2
}

pub(super) fn snapshot_kind_length_offset(encoded: &[u8]) -> usize {
    let metadata_offset = snapshot_metadata_offset();
    let group_id_len = u16::from_be_bytes(
        encoded[metadata_offset..metadata_offset + 2]
            .try_into()
            .expect("group id length is encoded"),
    ) as usize;
    metadata_offset + 2 + group_id_len + 8 + 8 + 8 + 8
}

pub(super) fn snapshot_version_offset(encoded: &[u8]) -> usize {
    let kind_length_offset = snapshot_kind_length_offset(encoded);
    let kind_len = u16::from_be_bytes(
        encoded[kind_length_offset..kind_length_offset + 2]
            .try_into()
            .expect("kind length is encoded"),
    ) as usize;
    kind_length_offset + 2 + kind_len
}

pub(super) fn snapshot_chunk_last_included_index_offset(encoded: &[u8]) -> usize {
    let metadata_offset = snapshot_metadata_offset();
    let group_id_len = u16::from_be_bytes(
        encoded[metadata_offset..metadata_offset + 2]
            .try_into()
            .expect("group id length is encoded"),
    ) as usize;
    metadata_offset + 2 + group_id_len + 8
}

pub(super) fn snapshot_chunk_last_included_term_offset(encoded: &[u8]) -> usize {
    snapshot_chunk_last_included_index_offset(encoded) + 8
}

pub(super) fn snapshot_chunk_hard_state_term_offset(encoded: &[u8]) -> usize {
    snapshot_chunk_last_included_term_offset(encoded) + 8
}

pub(super) fn rewrite_frame_checksum(encoded: &mut [u8]) {
    let checksum_offset = encoded.len() - 4;
    let checksum = crc32(&encoded[..checksum_offset]);
    encoded[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
}
