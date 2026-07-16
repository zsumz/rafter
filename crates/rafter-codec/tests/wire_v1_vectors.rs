//! Version 1 exact-byte interoperability vectors.
//!
//! These fixtures pin encoder output independently of the implementation
//! layout and prove that each fixed frame reconstructs the intended message.

use rafter::{
    AppendEntries, AppendEntriesResponse, ApplicationSnapshotKind, ApplicationSnapshotMetadata,
    ApplicationSnapshotVersion, CommittedConfiguration, ConfigurationEntry, ConfigurationId,
    InstallSnapshot, InstallSnapshotChunk, InstallSnapshotResponse, JointMembership, LogEntry,
    LogIndex, MembershipConfig, MembershipSet, Message, NodeId, PreVote, PreVoteResponse,
    RaftSnapshotMetadata, RequestVote, RequestVoteResponse, SnapshotCommittedConfiguration,
    SnapshotGroupId, SnapshotTransferId, Term, TimeoutNow,
};
use rafter_codec::{
    decode_message, encode_message, DecodePeerMessageError, EncodePeerMessageError,
};

struct Vector {
    name: &'static str,
    message: Message,
    expected: &'static str,
}

#[test]
fn every_v1_vector_matches_exact_bytes_and_decodes() {
    for vector in vectors() {
        let encoded = encode_message(&vector.message).expect("vector message encodes");
        let expected = parse_hex(vector.expected);
        assert!(
            !expected.is_empty(),
            "{} vector must not be empty",
            vector.name
        );
        assert_eq!(encoded, expected, "{} encoder bytes drifted", vector.name);
        assert_eq!(
            decode_message(&expected),
            Ok(vector.message),
            "{} decoder contract drifted",
            vector.name
        );
    }
}

#[test]
fn every_truncated_v1_vector_is_rejected_at_every_byte_boundary() {
    for vector in vectors() {
        let expected = parse_hex(vector.expected);
        for prefix_len in 0..expected.len() {
            assert!(
                matches!(
                    decode_message(&expected[..prefix_len]),
                    Err(DecodePeerMessageError::UnexpectedEof { .. })
                ),
                "{} accepted or misclassified its {prefix_len}-byte prefix",
                vector.name,
            );
        }
    }
}

#[test]
fn whole_snapshot_form_remains_unencodable() {
    let message = Message::InstallSnapshot(InstallSnapshot {
        term: Term(9),
        leader_id: NodeId(1),
        metadata: metadata(None),
        application_payload: b"whole snapshot".to_vec(),
    });
    assert_eq!(
        encode_message(&message),
        Err(EncodePeerMessageError::UnsupportedMessage {
            message: "InstallSnapshot",
            reason: "use InstallSnapshotChunk for peer transport",
        })
    );
}

fn vectors() -> Vec<Vector> {
    let mut vectors = election_vectors();
    vectors.extend(append_vectors());
    vectors.extend(snapshot_vectors());
    vectors
}

fn election_vectors() -> Vec<Vector> {
    vec![
        vector(
            "request_vote",
            Message::RequestVote(RequestVote {
                term: Term(7),
                candidate_id: NodeId(2),
                last_log_index: LogIndex(55),
                last_log_term: Term(6),
            }),
            include_str!("vectors/v1/request_vote.hex"),
        ),
        vector(
            "request_vote_response",
            Message::RequestVoteResponse(RequestVoteResponse {
                term: Term(7),
                voter_id: NodeId(3),
                vote_granted: true,
            }),
            include_str!("vectors/v1/request_vote_response.hex"),
        ),
        vector(
            "pre_vote",
            Message::PreVote(PreVote {
                term: Term(8),
                candidate_id: NodeId(2),
                last_log_index: LogIndex(55),
                last_log_term: Term(6),
            }),
            include_str!("vectors/v1/pre_vote.hex"),
        ),
        vector(
            "pre_vote_response",
            Message::PreVoteResponse(PreVoteResponse {
                term: Term(8),
                voter_id: NodeId(3),
                vote_granted: false,
            }),
            include_str!("vectors/v1/pre_vote_response.hex"),
        ),
        vector(
            "timeout_now",
            Message::TimeoutNow(TimeoutNow {
                term: Term(9),
                leader_id: NodeId(4),
            }),
            include_str!("vectors/v1/timeout_now.hex"),
        ),
    ]
}

fn append_vectors() -> Vec<Vector> {
    vec![
        vector(
            "append_entries_empty",
            append_entries(Vec::new()),
            include_str!("vectors/v1/append_entries_empty.hex"),
        ),
        vector(
            "append_entries_application",
            append_entries(vec![LogEntry::application(
                Term(8),
                vec![0, 159, 146, 150, 255],
            )]),
            include_str!("vectors/v1/append_entries_application.hex"),
        ),
        vector(
            "append_entries_noop",
            append_entries(vec![LogEntry::noop(Term(8))]),
            include_str!("vectors/v1/append_entries_noop.hex"),
        ),
        vector(
            "append_entries_stable_configuration",
            append_entries(vec![stable_entry()]),
            include_str!("vectors/v1/append_entries_stable_configuration.hex"),
        ),
        vector(
            "append_entries_joint_configuration",
            append_entries(vec![joint_entry()]),
            include_str!("vectors/v1/append_entries_joint_configuration.hex"),
        ),
        vector(
            "append_entries_response",
            Message::AppendEntriesResponse(AppendEntriesResponse {
                term: Term(8),
                follower_id: NodeId(2),
                success: false,
                match_index: LogIndex(10),
                sequence: 3,
            }),
            include_str!("vectors/v1/append_entries_response.hex"),
        ),
    ]
}

fn snapshot_vectors() -> Vec<Vector> {
    vec![
        vector(
            "install_snapshot_response_without_transfer",
            snapshot_response(None),
            include_str!("vectors/v1/install_snapshot_response_without_transfer.hex"),
        ),
        vector(
            "install_snapshot_response_with_transfer",
            snapshot_response(Some(SnapshotTransferId(123_456))),
            include_str!("vectors/v1/install_snapshot_response_with_transfer.hex"),
        ),
        vector(
            "install_snapshot_chunk_no_configuration_empty",
            snapshot_chunk(metadata(None), Vec::new()),
            include_str!("vectors/v1/install_snapshot_chunk_no_configuration_empty.hex"),
        ),
        vector(
            "install_snapshot_chunk_stable_configuration",
            snapshot_chunk(
                metadata(Some(MembershipConfig::stable(stable_membership()))),
                vec![0, 1, 250, 255],
            ),
            include_str!("vectors/v1/install_snapshot_chunk_stable_configuration.hex"),
        ),
        vector(
            "install_snapshot_chunk_joint_configuration",
            snapshot_chunk(
                metadata(Some(MembershipConfig::joint(
                    stable_membership(),
                    new_membership(),
                ))),
                b"joint snapshot".to_vec(),
            ),
            include_str!("vectors/v1/install_snapshot_chunk_joint_configuration.hex"),
        ),
        vector(
            "install_snapshot_chunk_64k",
            snapshot_chunk(metadata(None), vec![0x5a; 64 * 1024]),
            include_str!("vectors/v1/install_snapshot_chunk_64k.hex"),
        ),
    ]
}

fn vector(name: &'static str, message: Message, expected: &'static str) -> Vector {
    Vector {
        name,
        message,
        expected,
    }
}

fn append_entries(entries: Vec<LogEntry>) -> Message {
    Message::AppendEntries(AppendEntries {
        term: Term(8),
        leader_id: NodeId(1),
        prev_log_index: LogIndex(10),
        prev_log_term: Term(7),
        entries: entries.into(),
        leader_commit: LogIndex(11),
        sequence: 3,
    })
}

fn stable_membership() -> MembershipSet {
    MembershipSet::new(vec![NodeId(1), NodeId(2)], vec![NodeId(3)])
        .expect("stable membership is valid")
}

fn new_membership() -> MembershipSet {
    MembershipSet::new(vec![NodeId(1), NodeId(3)], Vec::new()).expect("new membership is valid")
}

fn stable_entry() -> LogEntry {
    LogEntry::configuration(
        Term(8),
        ConfigurationEntry::stable(ConfigurationId(11), stable_membership()),
    )
}

fn joint_entry() -> LogEntry {
    LogEntry::configuration(
        Term(8),
        ConfigurationEntry::joint(
            ConfigurationId(12),
            JointMembership::new(stable_membership(), new_membership()),
        ),
    )
}

fn metadata(membership: Option<MembershipConfig>) -> RaftSnapshotMetadata {
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("vector-group").expect("valid group id"),
        NodeId(1),
        LogIndex(42),
        Term(8),
        Term(9),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("vector_state").expect("valid kind"),
            ApplicationSnapshotVersion::new(1).expect("valid version"),
        ),
    )
    .expect("valid snapshot metadata");
    membership.map_or(metadata.clone(), |membership| {
        metadata.with_committed_configuration(SnapshotCommittedConfiguration::new(
            Some(CommittedConfiguration {
                index: LogIndex(40),
                config_id: ConfigurationId(10),
            }),
            membership,
        ))
    })
}

fn snapshot_chunk(metadata: RaftSnapshotMetadata, chunk: Vec<u8>) -> Message {
    Message::InstallSnapshotChunk(InstallSnapshotChunk {
        term: Term(9),
        leader_id: NodeId(1),
        transfer_id: SnapshotTransferId(123_456),
        metadata,
        total_payload_len: chunk.len() as u64,
        application_payload_crc32: 0x1234_abcd,
        offset: 0,
        done: true,
        chunk,
    })
}

fn snapshot_response(transfer_id: Option<SnapshotTransferId>) -> Message {
    Message::InstallSnapshotResponse(InstallSnapshotResponse {
        term: Term(9),
        follower_id: NodeId(2),
        success: true,
        last_included_index: LogIndex(42),
        transfer_id,
        next_offset: 17,
    })
}

fn parse_hex(source: &str) -> Vec<u8> {
    source
        .split_whitespace()
        .flat_map(|token| {
            let (byte, count) = token.split_once('*').map_or((token, 1), |(byte, count)| {
                (byte, count.parse::<usize>().expect("valid repeat count"))
            });
            let byte = u8::from_str_radix(byte, 16).expect("valid vector hex byte");
            std::iter::repeat_n(byte, count)
        })
        .collect()
}
