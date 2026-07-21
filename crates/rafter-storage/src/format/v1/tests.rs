//! Scenarios for shared version-1 snapshot-metadata grammar.

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    CommittedConfiguration, ConfigurationId, LogIndex, MembershipConfig, MembershipSet, NodeId,
    RaftSnapshotMetadata, SnapshotCommittedConfiguration, SnapshotGroupId, Term,
};

use super::snapshot_metadata::{decode_snapshot_metadata, encode_snapshot_metadata};
use crate::{
    encode_raft_snapshot,
    format::{Reader, Writer},
    raft_snapshot_codec::encode_raft_snapshot_metadata_envelope,
    PersistedRaftSnapshot,
};

#[test]
fn shared_snapshot_metadata_grammar_round_trips_joint_membership() {
    let expected = metadata();
    let mut writer = Writer::new();
    encode_snapshot_metadata(&mut writer, &expected).expect("metadata encodes");
    let encoded = writer.finish();

    let mut reader = Reader::new(&encoded);
    let decoded = decode_snapshot_metadata(&mut reader).expect("metadata decodes");
    reader
        .finish()
        .expect("metadata consumes exactly its fields");

    assert_eq!(decoded, expected);
}

#[test]
fn metadata_only_envelope_matches_empty_snapshot_encoding() {
    let metadata = metadata();
    let expected = encode_raft_snapshot(&PersistedRaftSnapshot {
        metadata: metadata.clone(),
        application_payload: Vec::new(),
    })
    .expect("empty snapshot encodes");

    assert_eq!(
        encode_raft_snapshot_metadata_envelope(&metadata).expect("metadata envelope encodes"),
        expected,
    );
}

fn metadata() -> RaftSnapshotMetadata {
    let old = MembershipSet::new(vec![NodeId(1), NodeId(2)], vec![NodeId(4)])
        .expect("old membership is valid");
    let new = MembershipSet::new(vec![NodeId(1), NodeId(3)], Vec::new())
        .expect("new membership is valid");
    RaftSnapshotMetadata::new(
        SnapshotGroupId::new("format-v1-test").expect("group id is valid"),
        NodeId(1),
        LogIndex(42),
        Term(7),
        Term(9),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("test_state").expect("kind is valid"),
            ApplicationSnapshotVersion::new(1).expect("version is valid"),
        ),
    )
    .expect("metadata is valid")
    .with_committed_configuration(SnapshotCommittedConfiguration::new(
        Some(CommittedConfiguration {
            index: LogIndex(40),
            config_id: ConfigurationId(12),
        }),
        MembershipConfig::joint(old, new),
    ))
}
