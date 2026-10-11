use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    InstallSnapshot, InstallSnapshotChunk, InstallSnapshotResponse, LogIndex, MembershipConfig,
    Message, NodeId, RaftSnapshotMetadata, SnapshotGroupId, SnapshotTransferId, Term,
};

pub(super) fn cases() -> Vec<(String, Message)> {
    let mut cases = Vec::new();
    for (index, membership) in super::membership_cases().into_iter().enumerate() {
        for done in [false, true] {
            cases.push((
                format!("snapshot-chunk-{index}-{done}"),
                Message::InstallSnapshotChunk(InstallSnapshotChunk {
                    term: Term(21),
                    leader_id: NodeId(2),
                    transfer_id: SnapshotTransferId(1 << 48),
                    metadata: metadata(membership.clone()),
                    total_payload_len: 131_072,
                    application_payload_crc32: 0x1234_abcd,
                    offset: if done { 65_536 } else { 0 },
                    chunk: vec![0xd3; 65_536],
                    done,
                }),
            ));
        }
    }
    for transfer_id in [None, Some(SnapshotTransferId(1 << 48))] {
        for success in [false, true] {
            cases.push((
                format!("snapshot-response-{}-{success}", transfer_id.is_some()),
                Message::InstallSnapshotResponse(InstallSnapshotResponse {
                    term: Term(21),
                    follower_id: NodeId(3),
                    success,
                    last_included_index: LogIndex(900),
                    transfer_id,
                    next_offset: 65_536,
                }),
            ));
        }
    }
    cases
}

fn metadata(membership: Option<MembershipConfig>) -> RaftSnapshotMetadata {
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("wire-compatibility").unwrap(),
        NodeId(2),
        LogIndex(900),
        Term(19),
        Term(21),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("application-state").unwrap(),
            ApplicationSnapshotVersion::new(2).unwrap(),
        ),
    )
    .unwrap();
    membership.map_or(metadata.clone(), |membership| {
        metadata.with_committed_membership(membership)
    })
}

pub(in crate::messages) fn whole_snapshot() -> Message {
    Message::InstallSnapshot(InstallSnapshot {
        term: Term(21),
        leader_id: NodeId(2),
        metadata: metadata(None),
        application_payload: vec![0, 255],
    })
}
