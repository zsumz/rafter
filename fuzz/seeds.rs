//! Regenerates the committed seed corpora for the byte-oriented fuzz targets
//! by encoding real messages and snapshot envelopes.
//!
//! Run with: `cargo +nightly run --bin gen-seeds` (from `fuzz/`), or
//! `cargo +nightly run --manifest-path fuzz/Cargo.toml --bin gen-seeds`
//! from the repository root. Output is deterministic; re-running overwrites
//! the same `corpus/<target>/seed-*` files.

use std::fs;
use std::path::PathBuf;

use rafter::{
    AppendEntries, AppendEntriesResponse, ApplicationSnapshotKind, ApplicationSnapshotMetadata,
    ApplicationSnapshotVersion, ConfigurationEntry, ConfigurationId, InstallSnapshotChunk,
    InstallSnapshotResponse, JointMembership, LogEntry, LogIndex, MembershipConfig, MembershipSet,
    Message, NodeId, PreVoteResponse, RaftSnapshotMetadata, RequestVote, SnapshotTransferId, Term,
};
use rafter_storage::{crc32, encode_raft_snapshot, PersistedRaftSnapshot};

fn corpus_dir(target: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("corpus")
        .join(target)
}

fn write_seed(target: &str, name: &str, bytes: &[u8]) {
    let dir = corpus_dir(target);
    fs::create_dir_all(&dir).expect("create corpus dir");
    let path = dir.join(name);
    fs::write(&path, bytes).expect("write seed");
    println!("{} ({} bytes)", path.display(), bytes.len());
}

fn voters_123_learner_4() -> MembershipSet {
    MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], vec![NodeId(4)])
        .expect("valid membership")
}

fn voters_235() -> MembershipSet {
    MembershipSet::new(vec![NodeId(2), NodeId(3), NodeId(5)], vec![]).expect("valid membership")
}

fn snapshot_metadata(with_membership: bool) -> RaftSnapshotMetadata {
    let metadata = RaftSnapshotMetadata::new(
        rafter::SnapshotGroupId::new("g1").expect("valid group id"),
        NodeId(2),
        LogIndex(4),
        Term(2),
        Term(3),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("kv").expect("valid kind"),
            ApplicationSnapshotVersion::new(1).expect("valid version"),
        ),
    )
    .expect("valid snapshot metadata");
    if with_membership {
        metadata.with_committed_membership(MembershipConfig::stable(voters_123_learner_4()))
    } else {
        metadata
    }
}

fn codec_seeds() {
    let target = "codec_decode";
    let encode = |message: &Message| -> Vec<u8> {
        rafter_codec::encode_message(message).expect("seed message encodes")
    };

    write_seed(
        target,
        "seed-request-vote",
        &encode(&Message::RequestVote(RequestVote {
            term: Term(3),
            candidate_id: NodeId(2),
            last_log_index: LogIndex(4),
            last_log_term: Term(2),
        })),
    );

    write_seed(
        target,
        "seed-pre-vote-response",
        &encode(&Message::PreVoteResponse(PreVoteResponse {
            term: Term(4),
            voter_id: NodeId(3),
            vote_granted: true,
        })),
    );

    write_seed(
        target,
        "seed-append-entries",
        &encode(&Message::AppendEntries(AppendEntries {
            term: Term(2),
            leader_id: NodeId(1),
            prev_log_index: LogIndex(3),
            prev_log_term: Term(1),
            sequence: 5,
            entries: vec![
                LogEntry::application(Term(2), b"ab".to_vec()),
                LogEntry::configuration(
                    Term(2),
                    ConfigurationEntry::stable(ConfigurationId(1), voters_123_learner_4()),
                ),
                LogEntry::configuration(
                    Term(2),
                    ConfigurationEntry::joint(
                        ConfigurationId(2),
                        JointMembership::new(voters_123_learner_4(), voters_235()),
                    ),
                ),
            ],
            leader_commit: LogIndex(3),
        })),
    );

    write_seed(
        target,
        "seed-append-entries-response",
        &encode(&Message::AppendEntriesResponse(AppendEntriesResponse {
            term: Term(2),
            follower_id: NodeId(3),
            success: true,
            match_index: LogIndex(4),
            sequence: 5,
        })),
    );

    write_seed(
        target,
        "seed-install-snapshot-chunk",
        &encode(&Message::InstallSnapshotChunk(InstallSnapshotChunk {
            term: Term(3),
            leader_id: NodeId(2),
            transfer_id: SnapshotTransferId(7),
            metadata: snapshot_metadata(true),
            total_payload_len: 8,
            application_payload_crc32: 0x1234_abcd,
            offset: 4,
            chunk: b"chnk".to_vec(),
            done: true,
        })),
    );

    write_seed(
        target,
        "seed-install-snapshot-response",
        &encode(&Message::InstallSnapshotResponse(InstallSnapshotResponse {
            term: Term(3),
            follower_id: NodeId(3),
            success: true,
            last_included_index: LogIndex(4),
            transfer_id: Some(SnapshotTransferId(7)),
            next_offset: 8,
        })),
    );
}

/// Hand-builds a version 1 or version 2 snapshot envelope (both carry a u32
/// payload length; v1 additionally predates the membership field) so the
/// fuzzer starts with checksum-valid inputs on every rung of the decode
/// ladder. The current encoder only emits v3.
fn legacy_snapshot_envelope(version: u8, payload: &[u8]) -> Vec<u8> {
    assert!(version == 1 || version == 2);
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"RFSN");
    bytes.push(version);
    // group_id: u16 length-prefixed string
    bytes.extend_from_slice(&(2u16).to_be_bytes());
    bytes.extend_from_slice(b"g1");
    // writer_node_id, last_included_index, last_included_term, hard_state_term
    bytes.extend_from_slice(&2u64.to_be_bytes());
    bytes.extend_from_slice(&4u64.to_be_bytes());
    bytes.extend_from_slice(&2u64.to_be_bytes());
    bytes.extend_from_slice(&3u64.to_be_bytes());
    // application kind + version
    bytes.extend_from_slice(&(2u16).to_be_bytes());
    bytes.extend_from_slice(b"kv");
    bytes.extend_from_slice(&(1u16).to_be_bytes());
    if version == 2 {
        // committed membership present: stable, voters [1,2,3], learners [4]
        bytes.push(1); // MEMBERSHIP_PRESENT
        bytes.push(0); // MEMBERSHIP_STABLE
        bytes.extend_from_slice(&(3u16).to_be_bytes());
        for voter in [1u64, 2, 3] {
            bytes.extend_from_slice(&voter.to_be_bytes());
        }
        bytes.extend_from_slice(&(1u16).to_be_bytes());
        bytes.extend_from_slice(&4u64.to_be_bytes());
    }
    // u32 payload length (v1/v2), payload, payload crc, envelope crc
    bytes.extend_from_slice(&(u32::try_from(payload.len()).unwrap()).to_be_bytes());
    bytes.extend_from_slice(payload);
    bytes.extend_from_slice(&crc32(payload).to_be_bytes());
    let envelope_crc = crc32(&bytes);
    bytes.extend_from_slice(&envelope_crc.to_be_bytes());
    bytes
}

fn storage_seeds() {
    let target = "storage_snapshot_decode";

    write_seed(
        target,
        "seed-v3-minimal",
        &encode_raft_snapshot(&PersistedRaftSnapshot {
            metadata: snapshot_metadata(false),
            application_payload: Vec::new(),
        }),
    );

    write_seed(
        target,
        "seed-v3-payload",
        &encode_raft_snapshot(&PersistedRaftSnapshot {
            metadata: snapshot_metadata(false),
            application_payload: b"hello rafter".to_vec(),
        }),
    );

    write_seed(
        target,
        "seed-v3-stable-membership",
        &encode_raft_snapshot(&PersistedRaftSnapshot {
            metadata: snapshot_metadata(true),
            application_payload: b"kv".to_vec(),
        }),
    );

    write_seed(
        target,
        "seed-v3-joint-membership",
        &encode_raft_snapshot(&PersistedRaftSnapshot {
            metadata: snapshot_metadata(false).with_committed_membership(MembershipConfig::joint(
                voters_123_learner_4(),
                voters_235(),
            )),
            application_payload: b"joint".to_vec(),
        }),
    );

    let v2 = legacy_snapshot_envelope(2, b"legacy-v2");
    rafter_storage::decode_raft_snapshot(&v2).expect("hand-built v2 envelope decodes");
    write_seed(target, "seed-v2-stable-membership", &v2);

    let v1 = legacy_snapshot_envelope(1, b"legacy-v1");
    rafter_storage::decode_raft_snapshot(&v1).expect("hand-built v1 envelope decodes");
    write_seed(target, "seed-v1-minimal", &v1);
}

fn main() {
    codec_seeds();
    storage_seeds();
}
