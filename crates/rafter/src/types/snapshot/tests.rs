//! Snapshot metadata, identity, transfer, and source vocabulary.

use super::super::{LogIndex, MembershipConfig, MembershipSet, NodeId, Term};
use super::*;

#[test]
fn snapshot_ids_have_stable_display_and_validation() {
    let group_id = SnapshotGroupId::new("metadata:primary").expect("valid group id");
    let kind = ApplicationSnapshotKind::new("metadata_catalog.v1").expect("valid kind");

    assert_eq!(group_id.to_string(), "metadata:primary");
    assert_eq!(group_id.as_str(), "metadata:primary");
    assert_eq!(kind.to_string(), "metadata_catalog.v1");
    assert_eq!(kind.as_str(), "metadata_catalog.v1");
    assert_eq!(
        SnapshotGroupId::new(""),
        Err(SnapshotIdError::Empty {
            field: "snapshot group id"
        })
    );
    assert_eq!(
        ApplicationSnapshotKind::new("stream data"),
        Err(SnapshotIdError::InvalidCharacter {
            field: "application snapshot kind",
            character: ' '
        })
    );
}

#[test]
fn snapshot_version_rejects_zero() {
    assert_eq!(
        ApplicationSnapshotVersion::new(0),
        Err(SnapshotMetadataError::ZeroApplicationSnapshotVersion)
    );
    assert_eq!(
        ApplicationSnapshotVersion::new(3)
            .expect("non-zero version")
            .get(),
        3
    );
}

#[test]
fn raft_snapshot_metadata_preserves_valid_boundary_and_application_kind() {
    let metadata = test_snapshot_metadata(
        LogIndex(12),
        Term(4),
        Term(5),
        ApplicationSnapshotKind::new("stream_data").expect("valid kind"),
    )
    .expect("snapshot metadata is valid");

    assert_eq!(metadata.group_id.as_str(), "data-group-10");
    assert_eq!(metadata.writer_id, NodeId(2));
    assert_eq!(metadata.last_included_index, LogIndex(12));
    assert_eq!(metadata.last_included_term, Term(4));
    assert_eq!(metadata.hard_state_term, Term(5));
    assert_eq!(metadata.application.kind.as_str(), "stream_data");
    assert_eq!(metadata.application.version.get(), 1);
}

#[test]
fn raft_snapshot_metadata_can_carry_committed_membership() {
    let membership = MembershipConfig::stable(
        MembershipSet::new(vec![NodeId(1), NodeId(3), NodeId(4)], vec![NodeId(5)])
            .expect("membership is valid"),
    );
    let metadata = test_snapshot_metadata(
        LogIndex(12),
        Term(4),
        Term(5),
        ApplicationSnapshotKind::new("stream_data").expect("valid kind"),
    )
    .expect("snapshot metadata is valid")
    .with_committed_membership(membership.clone());

    assert_eq!(metadata.committed_membership(), Some(&membership));
}

#[test]
fn snapshot_transfer_id_is_deterministic_and_binds_transfer_header() {
    let metadata = test_snapshot_metadata(
        LogIndex(12),
        Term(4),
        Term(5),
        ApplicationSnapshotKind::new("stream_data").expect("valid kind"),
    )
    .expect("snapshot metadata is valid");
    let snapshot = RaftSnapshot::from_payload(metadata.clone(), b"snapshot bytes");

    assert_ne!(snapshot.transfer_id(), SnapshotTransferId(0));
    assert_eq!(
        snapshot.transfer_id(),
        RaftSnapshot::from_payload(metadata.clone(), b"snapshot bytes").transfer_id()
    );
    assert_ne!(
        snapshot.transfer_id(),
        RaftSnapshot::from_payload(metadata.clone(), b"snapshot bytes!").transfer_id()
    );
    assert_ne!(
        snapshot.transfer_id(),
        RaftSnapshot::new(
            metadata.clone(),
            snapshot.application_payload_len + 1,
            snapshot.application_payload_crc32,
        )
        .transfer_id()
    );

    let membership = MembershipConfig::stable(
        MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
            .expect("membership is valid"),
    );
    assert_ne!(
        snapshot.transfer_id(),
        RaftSnapshot::from_payload(
            metadata.with_committed_membership(membership),
            b"snapshot bytes",
        )
        .transfer_id()
    );
}

#[test]
fn raft_snapshot_metadata_rejects_zero_boundary() {
    assert_eq!(
        test_snapshot_metadata(LogIndex::ZERO, Term(1), Term(1), test_snapshot_kind()),
        Err(SnapshotMetadataError::ZeroLastIncludedIndex)
    );
    assert_eq!(
        test_snapshot_metadata(LogIndex(1), Term(0), Term(1), test_snapshot_kind()),
        Err(SnapshotMetadataError::ZeroLastIncludedTerm {
            last_included_index: LogIndex(1)
        })
    );
}

#[test]
fn raft_snapshot_metadata_rejects_term_ahead_of_hard_state() {
    assert_eq!(
        test_snapshot_metadata(LogIndex(8), Term(6), Term(5), test_snapshot_kind()),
        Err(SnapshotMetadataError::SnapshotTermAheadOfHardState {
            last_included_index: LogIndex(8),
            last_included_term: Term(6),
            hard_state_term: Term(5),
        })
    );
}

fn test_snapshot_metadata(
    last_included_index: LogIndex,
    last_included_term: Term,
    hard_state_term: Term,
    kind: ApplicationSnapshotKind,
) -> Result<RaftSnapshotMetadata, SnapshotMetadataError> {
    RaftSnapshotMetadata::new(
        SnapshotGroupId::new("data-group-10").expect("valid group id"),
        NodeId(2),
        last_included_index,
        last_included_term,
        hard_state_term,
        ApplicationSnapshotMetadata::new(
            kind,
            ApplicationSnapshotVersion::new(1).expect("valid version"),
        ),
    )
}

fn test_snapshot_kind() -> ApplicationSnapshotKind {
    ApplicationSnapshotKind::new("stream_data").expect("valid kind")
}
