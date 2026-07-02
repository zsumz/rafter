use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    BootstrapLogEntry, BootstrapState, LogIndex, MembershipConfig, Message, NodeConfig, NodeId,
    RaftSnapshot, RaftSnapshotMetadata, Role, SnapshotGroupId, Term,
};

use crate::{Cluster, Envelope};

use super::{NodeSummary, ProposalId, StateSummary};

pub(super) fn proposal_payload(proposal_id: ProposalId) -> Vec<u8> {
    format!("model-proposal-{}", proposal_id.0).into_bytes()
}

pub(super) fn three_node_configs() -> Vec<NodeConfig> {
    vec![
        config(1, &[2, 3], 3),
        config(2, &[1, 3], 3),
        config(3, &[1, 2], 3),
    ]
}

#[cfg(test)]
pub(super) fn three_node_lease_configs() -> Vec<NodeConfig> {
    vec![
        production_config(1, &[2, 3], 8).with_lease_reads(true),
        production_config(2, &[1, 3], 8).with_lease_reads(true),
        production_config(3, &[1, 2], 8).with_lease_reads(true),
    ]
}

#[cfg(test)]
pub(super) fn four_node_future_learner_configs() -> Vec<NodeConfig> {
    vec![
        config(1, &[2, 3], 3),
        config(2, &[1, 3], 3),
        config(3, &[1, 2], 3),
        non_voter_config(4, &[1, 2, 3], 3),
    ]
}

/// The bounded explorers pin the minimal protocol — no pre-vote, no
/// check-quorum — so their state spaces stay exactly the historically
/// verified ones; the pre-vote leg opts in through its own configs.
pub(super) fn config(id: u64, peers: &[u64], election_timeout_ticks: u64) -> NodeConfig {
    NodeConfig::new(
        NodeId(id),
        peers.iter().copied().map(NodeId).collect(),
        election_timeout_ticks,
    )
    .expect("model-check Raft node config must be valid")
    .with_pre_vote(false)
    .with_check_quorum(false)
}

#[cfg(test)]
fn non_voter_config(id: u64, peers: &[u64], election_timeout_ticks: u64) -> NodeConfig {
    NodeConfig::new_non_voter(
        NodeId(id),
        peers.iter().copied().map(NodeId).collect(),
        election_timeout_ticks,
    )
    .expect("model-check non-voter config must be valid")
    .with_pre_vote(false)
    .with_check_quorum(false)
}

#[cfg(test)]
fn production_config(id: u64, peers: &[u64], election_timeout_ticks: u64) -> NodeConfig {
    NodeConfig::new(
        NodeId(id),
        peers.iter().copied().map(NodeId).collect(),
        election_timeout_ticks,
    )
    .expect("production model-check Raft node config must be valid")
}

pub(super) fn elect_node_one(cluster: &mut Cluster) {
    for _ in 0..32 {
        if cluster.role(NodeId(1)) == Role::Leader {
            return;
        }
        cluster.tick(NodeId(1));
        cluster.deliver_all();
    }
    panic!("node-1 did not become leader within the model-check election budget");
}

pub(super) fn elect_node_one_with_node_three(cluster: &mut Cluster) {
    for _ in 0..3 {
        cluster.tick(NodeId(1));
    }
    deliver_one(cluster, request_vote(NodeId(1), NodeId(3)));
    deliver_one(cluster, request_vote_response(NodeId(3), NodeId(1)));
    debug_assert_eq!(cluster.role(NodeId(1)), Role::Leader);
}

pub(super) fn deliver_one(cluster: &mut Cluster, mut predicate: impl FnMut(&Envelope) -> bool) {
    assert!(
        cluster.deliver_one_matching(&mut predicate),
        "expected one ready message to deliver"
    );
}

pub(super) fn request_vote(from: NodeId, to: NodeId) -> impl FnMut(&Envelope) -> bool {
    move |envelope| {
        envelope.from == from
            && envelope.to == to
            && matches!(envelope.message, Message::RequestVote(_))
    }
}

pub(super) fn request_vote_response(from: NodeId, to: NodeId) -> impl FnMut(&Envelope) -> bool {
    move |envelope| {
        envelope.from == from
            && envelope.to == to
            && matches!(envelope.message, Message::RequestVoteResponse(_))
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
    let metadata = test_snapshot_metadata(
        writer_id,
        last_included_index,
        last_included_term,
        hard_state_term,
    );
    let snapshot = RaftSnapshot::from_payload(metadata, payload);
    (snapshot, payload.to_vec())
}

pub(super) fn test_snapshot_with_committed_membership(
    writer_id: u64,
    last_included_index: u64,
    last_included_term: u64,
    hard_state_term: u64,
    payload: &[u8],
    membership: MembershipConfig,
) -> (RaftSnapshot, Vec<u8>) {
    let metadata = test_snapshot_metadata(
        writer_id,
        last_included_index,
        last_included_term,
        hard_state_term,
    )
    .with_committed_membership(membership);
    let snapshot = RaftSnapshot::from_payload(metadata, payload);
    (snapshot, payload.to_vec())
}

fn test_snapshot_metadata(
    writer_id: u64,
    last_included_index: u64,
    last_included_term: u64,
    hard_state_term: u64,
) -> RaftSnapshotMetadata {
    RaftSnapshotMetadata::new(
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
    .expect("valid snapshot metadata")
}

pub(super) fn large_snapshot_payload() -> Vec<u8> {
    let mut payload = Vec::with_capacity(70 * 1024);
    while payload.len() < 70 * 1024 {
        payload.extend_from_slice(b"snapshot-model-check-payload");
    }
    payload.truncate(70 * 1024);
    payload
}

pub(super) fn summarize(cluster: &Cluster) -> StateSummary {
    StateSummary {
        nodes: cluster
            .nodes
            .iter()
            .map(|(node_id, node)| NodeSummary {
                node_id: *node_id,
                term: node.current_term(),
                role: node.role(),
                commit_index: node.commit_index(),
                snapshot_index: node.snapshot_index(),
                last_log_index: node.last_log_index(),
            })
            .collect(),
    }
}
