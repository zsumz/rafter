//! Shared descriptors, sources, and message inspection for snapshot scenarios.

pub(super) use super::super::helpers::bootstrap_entry;
pub(super) use super::*;

pub(in crate::node::tests) fn push_log_entry(node: &mut Node, term: Term, payload: &[u8]) {
    node.persistent
        .log
        .push(LogEntry::application(term, payload.to_vec()));
}

pub(in crate::node::tests) fn leader_with_snapshot_and_suffix() -> Node {
    leader_with_snapshot_payload(b"snapshot bytes".to_vec()).0
}

pub(in crate::node::tests) fn leader_with_snapshot_payload(
    payload: Vec<u8>,
) -> (Node, InMemorySnapshotChunkSource) {
    let snapshot = test_snapshot(3, 4, 5, &payload);
    let source = snapshot_source(&snapshot, payload);
    let mut leader = Node::from_bootstrap(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3).expect("test config is valid"),
        BootstrapState {
            current_term: Term(5),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: Some(snapshot),
            log: vec![bootstrap_entry(4, 5, b"suffix-four")],
        },
    )
    .expect("leader hydrates from snapshot");
    leader.become_leader();
    (leader, source)
}

pub(in crate::node::tests) fn snapshot_source(
    snapshot: &crate::RaftSnapshot,
    payload: Vec<u8>,
) -> InMemorySnapshotChunkSource {
    let mut source = InMemorySnapshotChunkSource::new();
    source
        .insert(snapshot, payload)
        .expect("payload matches the snapshot's declared length");
    source
}

pub(in crate::node::tests) fn large_snapshot_payload() -> Vec<u8> {
    (0_u32..70_000)
        .map(|value| u8::try_from(value % 251).expect("value is below u8::MAX"))
        .collect()
}

pub(in crate::node::tests) fn snapshot_chunk_send_from_output(
    output: &Output,
) -> crate::SnapshotChunkSend {
    let Output::SendSnapshotChunk { chunk, .. } = output else {
        panic!("expected send snapshot chunk output");
    };
    chunk.clone()
}

pub(in crate::node::tests) fn install_snapshot_chunk_from_output(
    output: &Output,
    source: &InMemorySnapshotChunkSource,
) -> crate::InstallSnapshotChunk {
    snapshot_chunk_send_from_output(output)
        .resolve(source)
        .expect("source serves the snapshot chunk")
}

pub(in crate::node::tests) fn install_snapshot_response_from_outputs(
    outputs: &[Output],
) -> crate::InstallSnapshotResponse {
    outputs
        .iter()
        .find_map(|output| {
            let Output::Send { message, .. } = output else {
                return None;
            };
            let Message::InstallSnapshotResponse(response) = message else {
                return None;
            };
            Some(*response)
        })
        .expect("expected install snapshot response")
}

pub(in crate::node::tests) fn staged_snapshot_bytes(outputs: &[Output]) -> Vec<u8> {
    outputs
        .iter()
        .filter_map(|output| {
            let Output::StageSnapshotChunk { chunk } = output else {
                return None;
            };
            Some(chunk.bytes.as_slice())
        })
        .collect::<Vec<_>>()
        .concat()
}

pub(in crate::node::tests) fn test_snapshot(
    last_included_index: u64,
    last_included_term: u64,
    hard_state_term: u64,
    payload: &[u8],
) -> crate::RaftSnapshot {
    crate::RaftSnapshot::from_payload(
        snapshot_metadata(last_included_index, last_included_term, hard_state_term),
        payload,
    )
}

pub(in crate::node::tests) fn test_snapshot_with_committed_voters(
    last_included_index: u64,
    last_included_term: u64,
    hard_state_term: u64,
    payload: &[u8],
    voters: &[u64],
) -> crate::RaftSnapshot {
    let membership = MembershipConfig::stable(
        MembershipSet::new(voters.iter().copied().map(NodeId).collect(), Vec::new())
            .expect("test membership is valid"),
    );
    let metadata = snapshot_metadata(last_included_index, last_included_term, hard_state_term)
        .with_committed_membership(membership);
    crate::RaftSnapshot::from_payload(metadata, payload)
}

fn snapshot_metadata(
    last_included_index: u64,
    last_included_term: u64,
    hard_state_term: u64,
) -> crate::RaftSnapshotMetadata {
    crate::RaftSnapshotMetadata::new(
        crate::SnapshotGroupId::new("data-group-10").expect("valid snapshot group"),
        NodeId(1),
        LogIndex(last_included_index),
        Term(last_included_term),
        Term(hard_state_term),
        crate::ApplicationSnapshotMetadata::new(
            crate::ApplicationSnapshotKind::new("stream_data").expect("valid kind"),
            crate::ApplicationSnapshotVersion::new(1).expect("valid version"),
        ),
    )
    .expect("valid snapshot metadata")
}
