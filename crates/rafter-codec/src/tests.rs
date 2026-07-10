use super::*;
use rafter::{
    AppendEntries, AppendEntriesResponse, ApplicationSnapshotKind, ApplicationSnapshotMetadata,
    ApplicationSnapshotVersion, ConfigurationEntry, ConfigurationId, InstallSnapshotChunk,
    InstallSnapshotResponse, JointMembership, LogEntry, LogEntryKind, LogIndex, MembershipConfig,
    MembershipSet, Message, NodeId, PreVote, PreVoteResponse, RaftSnapshot, RaftSnapshotMetadata,
    RequestVote, RequestVoteResponse, SnapshotGroupId, SnapshotMetadataError, SnapshotTransferId,
    Term,
};

#[test]
fn request_vote_round_trips() {
    round_trip(Message::RequestVote(RequestVote {
        term: Term(7),
        candidate_id: NodeId(2),
        last_log_index: LogIndex(55),
        last_log_term: Term(6),
    }));
}

#[test]
fn request_vote_response_round_trips() {
    round_trip(Message::RequestVoteResponse(RequestVoteResponse {
        term: Term(7),
        voter_id: NodeId(3),
        vote_granted: true,
    }));
}

#[test]
fn pre_vote_round_trips() {
    round_trip(Message::PreVote(PreVote {
        term: Term(8),
        candidate_id: NodeId(2),
        last_log_index: LogIndex(55),
        last_log_term: Term(6),
    }));
}

#[test]
fn pre_vote_response_round_trips() {
    round_trip(Message::PreVoteResponse(PreVoteResponse {
        term: Term(8),
        voter_id: NodeId(3),
        vote_granted: true,
    }));
}

#[test]
fn pre_vote_frames_use_current_version_and_message_code() {
    let message = Message::PreVote(PreVote {
        term: Term(8),
        candidate_id: NodeId(2),
        last_log_index: LogIndex(55),
        last_log_term: Term(6),
    });
    let encoded = encode_message(&message).expect("message encodes");

    assert_eq!(encoded[4], VERSION);
    assert_eq!(encoded[5], MSG_PRE_VOTE);
}

#[test]
fn append_entries_round_trips_with_opaque_payloads() {
    round_trip(Message::AppendEntries(AppendEntries {
        sequence: 0,
        term: Term(8),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(10),
        prev_log_term: Term(7),
        entries: vec![
            LogEntry::application(Term(8), b"stream command bytes".to_vec()),
            LogEntry::application(Term(8), vec![0, 159, 146, 150, 255]),
        ]
        .into(),
        leader_commit: LogIndex(11),
    }));
}

#[test]
fn decoded_append_entries_payloads_share_the_frame_allocation() {
    let message = Message::AppendEntries(AppendEntries {
        sequence: 0,
        term: Term(8),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(10),
        prev_log_term: Term(7),
        entries: vec![
            LogEntry::application(Term(8), b"first payload".to_vec()),
            LogEntry::application(Term(8), b"second payload".to_vec()),
        ]
        .into(),
        leader_commit: LogIndex(11),
    });
    let encoded = encode_message(&message).expect("message encodes");
    let decoded = decode_message(&encoded).expect("message decodes");
    let Message::AppendEntries(decoded) = decoded else {
        panic!("message remains append entries");
    };
    let first = application_payload(&decoded.entries[0]);
    let second = application_payload(&decoded.entries[1]);

    assert_eq!(first, b"first payload");
    assert_eq!(second, b"second payload");
    assert!(
        first.shares_allocation(second),
        "decoded application payloads share the immutable frame allocation"
    );
}

#[test]
fn append_entries_round_trips_with_configuration_entries() {
    let stable = MembershipSet::new(vec![NodeId(1), NodeId(2)], vec![NodeId(3)])
        .expect("stable membership is valid");
    let old = MembershipSet::new(vec![NodeId(1), NodeId(2)], vec![NodeId(3)])
        .expect("old membership is valid");
    let new = MembershipSet::new(vec![NodeId(1), NodeId(3)], Vec::new())
        .expect("new membership is valid");

    round_trip(Message::AppendEntries(AppendEntries {
        sequence: 0,
        term: Term(8),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(10),
        prev_log_term: Term(7),
        entries: vec![
            LogEntry::configuration(
                Term(8),
                ConfigurationEntry::stable(ConfigurationId(1), stable),
            ),
            LogEntry::configuration(
                Term(8),
                ConfigurationEntry::joint(ConfigurationId(2), JointMembership::new(old, new)),
            ),
        ]
        .into(),
        leader_commit: LogIndex(11),
    }));
}

#[test]
fn append_entries_round_trips_with_noop_entries() {
    round_trip(Message::AppendEntries(AppendEntries {
        sequence: 0,
        term: Term(8),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(10),
        prev_log_term: Term(7),
        entries: vec![LogEntry::noop(Term(8))].into(),
        leader_commit: LogIndex(10),
    }));
}

#[test]
fn append_entries_response_round_trips() {
    round_trip(Message::AppendEntriesResponse(AppendEntriesResponse {
        sequence: 0,
        term: Term(8),
        follower_id: NodeId(2),
        success: false,
        match_index: LogIndex(10),
    }));
}

#[test]
fn decode_rejects_snapshot_frame_with_max_last_included_index() {
    let message = Message::InstallSnapshotChunk(InstallSnapshotChunk {
        term: Term(9),
        leader_id: NodeId(1),
        transfer_id: SnapshotTransferId(123_456),
        metadata: test_snapshot().metadata,
        total_payload_len: 14,
        application_payload_crc32: 0x1234_abcd,
        offset: 0,
        chunk: b"snapshot bytes".to_vec(),
        done: true,
    });
    let mut encoded = encode_message(&message).expect("snapshot message encodes");
    let index_offset = snapshot_chunk_last_included_index_offset(&encoded);
    encoded[index_offset..index_offset + 8].copy_from_slice(&u64::MAX.to_be_bytes());
    rewrite_frame_checksum(&mut encoded);

    assert_eq!(
        decode_message(&encoded),
        Err(DecodePeerMessageError::InvalidSnapshotMetadata(
            SnapshotMetadataError::LastIncludedIndexAtMaximum,
        ))
    );
}

#[test]
fn encode_rejects_whole_install_snapshot_peer_frame() {
    let message = Message::InstallSnapshot(rafter::InstallSnapshot {
        term: Term(9),
        leader_id: NodeId(1),
        metadata: test_snapshot().metadata,
        application_payload: b"snapshot bytes".to_vec(),
    });

    assert_eq!(
        encode_message(&message),
        Err(EncodePeerMessageError::UnsupportedMessage {
            message: "InstallSnapshot",
            reason: "use InstallSnapshotChunk for peer transport",
        })
    );
}

#[test]
fn install_snapshot_chunk_round_trips_with_opaque_chunk_payload() {
    round_trip(Message::InstallSnapshotChunk(InstallSnapshotChunk {
        term: Term(9),
        leader_id: NodeId(1),
        transfer_id: SnapshotTransferId(123_456),
        metadata: test_snapshot().metadata,
        total_payload_len: 99,
        application_payload_crc32: 0x1234_abcd,
        offset: 64,
        chunk: vec![0, 1, 2, 250, 255],
        done: false,
    }));
}

#[test]
fn install_snapshot_response_round_trips() {
    round_trip(Message::InstallSnapshotResponse(InstallSnapshotResponse {
        term: Term(9),
        follower_id: NodeId(2),
        success: true,
        last_included_index: LogIndex(42),
        transfer_id: Some(SnapshotTransferId(123_456)),
        next_offset: 17,
    }));
}

#[test]
fn decode_rejects_bad_magic() {
    let mut encoded = encode_message(&vote_request()).expect("message encodes");
    encoded[0] = b'X';

    assert_eq!(
        decode_message(&encoded),
        Err(DecodePeerMessageError::InvalidMagic(*b"XFPM"))
    );
}

#[test]
fn decode_rejects_unsupported_version() {
    let mut encoded = encode_message(&vote_request()).expect("message encodes");
    encoded[4] = VERSION + 1;

    assert_eq!(
        decode_message(&encoded),
        Err(DecodePeerMessageError::UnsupportedVersion(VERSION + 1))
    );
}

#[test]
fn decode_rejects_truncated_payload() {
    let encoded = encode_message(&append_entries()).expect("message encodes");
    let truncated = &encoded[..encoded.len() - 1];

    assert_eq!(
        decode_message(truncated),
        Err(DecodePeerMessageError::UnexpectedEof {
            needed: 4,
            remaining: 3,
        })
    );
}

#[test]
fn decode_rejects_trailing_bytes() {
    let mut encoded = encode_message(&vote_request()).expect("message encodes");
    encoded.push(0);

    assert_eq!(
        decode_message(&encoded),
        Err(DecodePeerMessageError::TrailingBytes(1))
    );
}

#[test]
fn decode_rejects_corrupt_frame_checksum() {
    let mut encoded = encode_message(&vote_request()).expect("message encodes");
    encoded[6] ^= 0x01;

    assert!(matches!(
        decode_message(&encoded),
        Err(DecodePeerMessageError::FrameChecksumMismatch { .. })
    ));
}

#[test]
fn decode_rejects_corrupt_checksum_field() {
    let mut encoded = encode_message(&vote_request()).expect("message encodes");
    let last = encoded.len() - 1;
    encoded[last] ^= 0x01;

    assert!(matches!(
        decode_message(&encoded),
        Err(DecodePeerMessageError::FrameChecksumMismatch { .. })
    ));
}

#[test]
fn decode_rejects_unknown_message_type() {
    let mut encoded = encode_message(&vote_request()).expect("message encodes");
    encoded[5] = 99;

    assert_eq!(
        decode_message(&encoded),
        Err(DecodePeerMessageError::UnknownMessageType(99))
    );
}

#[test]
fn decode_rejects_whole_install_snapshot_message_type() {
    let mut encoded = encode_message(&vote_request()).expect("message encodes");
    encoded[5] = 5;

    assert_eq!(
        decode_message(&encoded),
        Err(DecodePeerMessageError::UnknownMessageType(5))
    );
}

#[test]
fn decode_rejects_unknown_log_entry_kind() {
    let mut encoded = encode_message(&append_entries()).expect("message encodes");
    encoded[first_append_entry_kind_offset()] = 99;

    assert_eq!(
        decode_message(&encoded),
        Err(DecodePeerMessageError::UnknownLogEntryKind(99))
    );
}

#[test]
fn decode_rejects_invalid_boolean() {
    let mut encoded = encode_message(&Message::RequestVoteResponse(RequestVoteResponse {
        term: Term(7),
        voter_id: NodeId(3),
        vote_granted: true,
    }))
    .expect("message encodes");
    encoded[22] = 2;

    assert_eq!(
        decode_message(&encoded),
        Err(DecodePeerMessageError::InvalidBoolean(2))
    );
}

fn round_trip(message: Message) {
    let encoded = encode_message(&message).expect("message encodes");
    assert_eq!(&encoded[..4], &MAGIC);
    assert_eq!(encoded[4], VERSION);
    assert_eq!(decode_message(&encoded), Ok(message));
}

#[test]
fn encode_message_into_matches_owned_encoder_and_reuses_buffer() {
    let message = append_entries();
    let expected = encode_message(&message).expect("message encodes");
    let mut encoded = vec![0; expected.len() + 128];
    let original_ptr = encoded.as_ptr();

    encode_message_into(&mut encoded, &message).expect("message encodes into buffer");

    assert_eq!(encoded, expected);
    assert_eq!(encoded.as_ptr(), original_ptr);
}

#[test]
fn encode_message_into_clears_buffer_on_error() {
    let message = Message::InstallSnapshot(rafter::InstallSnapshot {
        term: Term(9),
        leader_id: NodeId(1),
        metadata: test_snapshot().metadata,
        application_payload: b"snapshot bytes".to_vec(),
    });
    let mut encoded = vec![1, 2, 3];

    assert!(matches!(
        encode_message_into(&mut encoded, &message),
        Err(EncodePeerMessageError::UnsupportedMessage {
            message: "InstallSnapshot",
            ..
        })
    ));
    assert!(encoded.is_empty());
}

fn vote_request() -> Message {
    Message::RequestVote(RequestVote {
        term: Term(7),
        candidate_id: NodeId(2),
        last_log_index: LogIndex(55),
        last_log_term: Term(6),
    })
}

fn append_entries() -> Message {
    Message::AppendEntries(AppendEntries {
        sequence: 0,
        term: Term(8),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(10),
        prev_log_term: Term(7),
        entries: vec![LogEntry::application(
            Term(8),
            b"stream command bytes".to_vec(),
        )]
        .into(),
        leader_commit: LogIndex(11),
    })
}

fn application_payload(entry: &LogEntry) -> &rafter::SharedPayload {
    match &entry.kind {
        LogEntryKind::Application(payload) => payload,
        LogEntryKind::Configuration(_) | LogEntryKind::Noop => {
            panic!("test entry should be application payload")
        }
    }
}

fn first_append_entry_kind_offset() -> usize {
    4 + 1 + 1 + 8 + 8 + 8 + 8 + 4 + 8
}

fn snapshot_chunk_last_included_index_offset(encoded: &[u8]) -> usize {
    let metadata_offset = 4 + 1 + 1 + 8 + 8 + 8;
    let group_id_len = u16::from_be_bytes(
        encoded[metadata_offset..metadata_offset + 2]
            .try_into()
            .expect("group id length is encoded"),
    ) as usize;
    metadata_offset + 2 + group_id_len + 8
}

fn rewrite_frame_checksum(encoded: &mut [u8]) {
    let checksum_offset = encoded.len() - 4;
    let checksum = crc32(&encoded[..checksum_offset]);
    encoded[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
}

fn test_snapshot() -> RaftSnapshot {
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
    .expect("valid snapshot metadata")
    .with_committed_membership(MembershipConfig::stable(
        MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], vec![NodeId(4)])
            .expect("membership is valid"),
    ));
    RaftSnapshot::from_payload(metadata, b"snapshot bytes")
}

#[test]
fn configuration_entry_size_accounting_is_upper_bound_of_encoding() {
    use rafter::{ConfigurationEntry, ConfigurationId, JointMembership, MembershipSet};

    let stable_small = ConfigurationEntry::stable(
        ConfigurationId(1),
        MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], vec![]).expect("valid"),
    );
    let stable_large = ConfigurationEntry::stable(
        ConfigurationId(2),
        MembershipSet::new(
            (1..=21).map(NodeId).collect(),
            (22..=30).map(NodeId).collect(),
        )
        .expect("valid"),
    );
    let joint = ConfigurationEntry::joint(
        ConfigurationId(3),
        JointMembership::new(
            MembershipSet::new((1..=5).map(NodeId).collect(), vec![]).expect("valid"),
            MembershipSet::new((1..=9).map(NodeId).collect(), vec![NodeId(10)]).expect("valid"),
        ),
    );

    let message_with = |entries: Vec<rafter::LogEntry>| {
        Message::AppendEntries(AppendEntries {
            sequence: 0,
            term: Term(8),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(10),
            prev_log_term: Term(7),
            entries: entries.into(),
            leader_commit: LogIndex(11),
        })
    };
    for entry in [stable_small, stable_large, joint] {
        let log_entry = rafter::LogEntry::configuration(Term(1), entry);
        let base = encode_message(&message_with(vec![]))
            .expect("base encodes")
            .len();
        let with_entry = encode_message(&message_with(vec![log_entry.clone()]))
            .expect("message encodes")
            .len();
        let encoded_marginal = with_entry - base;
        assert!(
            log_entry.replication_bytes() >= encoded_marginal,
            "budget accounting {} must upper-bound the wire encoding {}",
            log_entry.replication_bytes(),
            encoded_marginal
        );
    }
}

#[test]
fn timeout_now_round_trips() {
    round_trip(Message::TimeoutNow(rafter::TimeoutNow {
        term: Term(9),
        leader_id: NodeId(4),
    }));
}

#[test]
fn decode_rejects_an_append_entries_frame_that_overstates_its_entry_count() {
    // The libFuzzer reproducer shape: a well-formed header whose
    // append-entries count claims far more entries than the frame carries
    // bytes for. Decode must return a typed error, never speculatively
    // allocate for the claim (a remote out-of-memory before this fix).
    let mut frame = Vec::new();
    frame.extend_from_slice(&MAGIC);
    frame.push(VERSION);
    frame.push(MSG_APPEND_ENTRIES);
    frame.extend_from_slice(&7u64.to_be_bytes()); // term
    frame.extend_from_slice(&1u64.to_be_bytes()); // leader id
    frame.extend_from_slice(&0u64.to_be_bytes()); // prev log index
    frame.extend_from_slice(&0u64.to_be_bytes()); // prev log term
    frame.extend_from_slice(&u32::MAX.to_be_bytes()); // entry count: hostile
                                                      // No entry bytes follow.

    let error = decode_message(&frame).expect_err("an overstated count is rejected");
    assert!(matches!(
        error,
        DecodePeerMessageError::UnexpectedEof { .. }
    ));
}

#[test]
fn append_entries_reservation_is_bounded_by_encoded_entry_size() {
    let hostile_count = u32::MAX as usize;
    let remaining = 256;

    assert_eq!(
        append_entries_entry_capacity(hostile_count, remaining),
        remaining / MIN_ENCODED_LOG_ENTRY_BYTES
    );
    assert_eq!(append_entries_entry_capacity(3, remaining), 3);
}

#[test]
fn decode_rejects_huge_append_entries_count_with_small_payload_budget() {
    let mut frame = Vec::new();
    frame.extend_from_slice(&MAGIC);
    frame.push(VERSION);
    frame.push(MSG_APPEND_ENTRIES);
    frame.extend_from_slice(&7u64.to_be_bytes()); // term
    frame.extend_from_slice(&1u64.to_be_bytes()); // leader id
    frame.extend_from_slice(&0u64.to_be_bytes()); // prev log index
    frame.extend_from_slice(&0u64.to_be_bytes()); // prev log term
    frame.extend_from_slice(&u32::MAX.to_be_bytes()); // hostile entry count
    frame.resize(frame.len() + 256, 0);

    assert_eq!(append_entries_entry_capacity(u32::MAX as usize, 256), 28);
    let error = decode_message(&frame).expect_err("tiny payload cannot satisfy huge count");
    assert!(matches!(
        error,
        DecodePeerMessageError::UnexpectedEof { .. }
    ));
}
