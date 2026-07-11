use super::super::helpers::{
    deliver_append_entries, deliver_append_entries_response, pre_vote, pre_vote_response,
    request_vote,
};
use super::super::*;
use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    BootstrapLogEntry, BootstrapState, Message, RaftSnapshot, RaftSnapshotMetadata,
    RequestVoteResponse, SnapshotGroupId,
};

pub(super) fn elect_node_one_with_node_three(cluster: &mut Cluster) {
    for _ in 0..3 {
        cluster.tick(NodeId(1));
    }
    assert_eq!(cluster.deliver_matching(pre_vote(NodeId(1), NodeId(3))), 1);
    assert_eq!(
        cluster.deliver_matching(pre_vote_response(NodeId(3), NodeId(1))),
        1
    );
    assert_eq!(
        cluster.deliver_matching(request_vote(NodeId(1), NodeId(3))),
        1
    );
    assert_eq!(
        cluster.deliver_matching(vote_response(NodeId(3), NodeId(1))),
        1
    );
    assert_eq!(cluster.role(NodeId(1)), Role::Leader);
}

pub(super) fn force_snapshot_catchup_to_node_two(cluster: &mut Cluster) {
    for _ in 0..8 {
        if cluster.deliver_matching(install_snapshot_chunk(NodeId(1), NodeId(2))) == 1 {
            assert_eq!(
                cluster.deliver_matching(install_snapshot_response(NodeId(2), NodeId(1))),
                1
            );
            assert_eq!(
                cluster.deliver_matching(deliver_append_entries(NodeId(1), NodeId(2))),
                1
            );
            return;
        }
        assert_eq!(
            cluster.deliver_matching(deliver_append_entries(NodeId(1), NodeId(2))),
            1
        );
        assert_eq!(
            cluster.deliver_matching(deliver_append_entries_response(NodeId(2), NodeId(1))),
            1
        );
    }
    panic!("leader did not fall back to snapshot transfer");
}

pub(super) fn install_snapshot_chunk(from: NodeId, to: NodeId) -> impl FnMut(&Envelope) -> bool {
    move |envelope| {
        envelope.from == from
            && envelope.to == to
            && matches!(envelope.message, Message::InstallSnapshotChunk(_))
    }
}

pub(super) fn install_snapshot_response(from: NodeId, to: NodeId) -> impl FnMut(&Envelope) -> bool {
    move |envelope| {
        envelope.from == from
            && envelope.to == to
            && matches!(envelope.message, Message::InstallSnapshotResponse(_))
    }
}

fn vote_response(from: NodeId, to: NodeId) -> impl FnMut(&Envelope) -> bool {
    move |envelope| {
        envelope.from == from
            && envelope.to == to
            && matches!(
                envelope.message,
                Message::RequestVoteResponse(RequestVoteResponse { .. })
            )
    }
}

pub(super) fn bootstrap_state(
    current_term: Term,
    entries: &[(u64, Term, &[u8])],
) -> BootstrapState {
    BootstrapState {
        current_term,
        voted_for: None,
        commit_index: LogIndex::ZERO,
        committed_configuration: None,
        snapshot: None,
        log: entries
            .iter()
            .map(|(index, term, payload)| {
                BootstrapLogEntry::application(LogIndex(*index), *term, (*payload).to_vec())
            })
            .collect(),
    }
}

pub(super) fn bootstrap_with_snapshot(
    current_term: Term,
    snapshot: RaftSnapshot,
    entries: &[(u64, Term, &[u8])],
) -> BootstrapState {
    BootstrapState {
        current_term,
        voted_for: None,
        commit_index: LogIndex::ZERO,
        committed_configuration: None,
        snapshot: Some(snapshot),
        log: entries
            .iter()
            .map(|(index, term, payload)| {
                BootstrapLogEntry::application(LogIndex(*index), *term, (*payload).to_vec())
            })
            .collect(),
    }
}

/// Builds a snapshot descriptor for `payload` and returns both; the caller
/// seeds the payload into each node whose store must hold the content.
pub(super) fn test_snapshot(
    writer_id: u64,
    last_included_index: u64,
    last_included_term: u64,
    hard_state_term: u64,
    payload: &[u8],
) -> (RaftSnapshot, Vec<u8>) {
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("sim-data-group").expect("valid snapshot group id"),
        NodeId(writer_id),
        LogIndex(last_included_index),
        Term(last_included_term),
        Term(hard_state_term),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("stream_data").expect("valid snapshot kind"),
            ApplicationSnapshotVersion::new(1).expect("valid snapshot version"),
        ),
    )
    .expect("valid snapshot metadata");
    let snapshot = RaftSnapshot::from_payload(metadata, payload);
    (snapshot, payload.to_vec())
}

/// A payload larger than the kernel's 64 KiB chunk directive, so a transfer
/// takes more than one `InstallSnapshotChunk` message.
pub(super) fn multi_chunk_payload() -> Vec<u8> {
    let mut payload = Vec::with_capacity(70 * 1024);
    while payload.len() < 70 * 1024 {
        payload.extend_from_slice(b"simulator-multi-chunk-snapshot-payload");
    }
    payload.truncate(70 * 1024);
    payload
}
