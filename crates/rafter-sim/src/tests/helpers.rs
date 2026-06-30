use super::*;
use rafter::Message;

pub(super) fn three_node_cluster() -> Cluster {
    Cluster::new(vec![
        config(1, &[2, 3], 3),
        config(2, &[1, 3], 9),
        config(3, &[1, 2], 9),
    ])
}

pub(super) fn seeded_three_node_cluster(seed: u64) -> Cluster {
    Cluster::new_with_seed(
        vec![
            config(1, &[2, 3], 3),
            config(2, &[1, 3], 9),
            config(3, &[1, 2], 9),
        ],
        SimSeed(seed),
    )
}

pub(super) fn config(id: u64, peers: &[u64], election_timeout_ticks: u64) -> NodeConfig {
    NodeConfig::new(
        NodeId(id),
        peers.iter().copied().map(NodeId).collect(),
        election_timeout_ticks,
    )
    .expect("test Raft node config is valid")
}

/// The minimal-protocol inverse of the default posture: elections campaign
/// directly, with no pre-vote poll. Scenarios that must elect a node while
/// its peers still hold a fresh leader hint need this — under pre-vote,
/// leader stickiness (thesis 4.2.3) denies such polls by design.
pub(super) fn direct_election_config(
    id: u64,
    peers: &[u64],
    election_timeout_ticks: u64,
) -> NodeConfig {
    config(id, peers, election_timeout_ticks).with_pre_vote(false)
}

pub(super) fn direct_election_three_node_cluster() -> Cluster {
    Cluster::new(vec![
        direct_election_config(1, &[2, 3], 3),
        direct_election_config(2, &[1, 3], 9),
        direct_election_config(3, &[1, 2], 9),
    ])
}

/// Ticks node 1 to its timeout and lets quiescence-driven delivery run the
/// whole election — the pre-vote round included when the posture has one.
pub(super) fn elect_node_one(cluster: &mut Cluster) {
    cluster.tick(NodeId(1));
    cluster.tick(NodeId(1));
    cluster.tick(NodeId(1));
    cluster.deliver_all();
}

/// Drives a direct election of node 2 with exact per-message drops and
/// deliveries; callers use [`direct_election_three_node_cluster`], because
/// node 3's fresh leader hint would deny a pre-vote poll here.
pub(super) fn elect_node_two_without_reaching_node_one(cluster: &mut Cluster) {
    for _ in 0..9 {
        cluster.tick(NodeId(2));
    }

    assert_eq!(
        cluster.drop_matching(|envelope| envelope.to == NodeId(1)),
        1
    );
    assert_eq!(
        cluster.deliver_matching(|envelope| envelope.to == NodeId(3)),
        1
    );
    assert_eq!(
        cluster.deliver_matching(|envelope| envelope.to == NodeId(2)),
        1
    );

    assert_eq!(cluster.role(NodeId(2)), Role::Leader);

    assert_eq!(
        cluster.drop_matching(|envelope| envelope.to == NodeId(1)),
        1
    );
    assert_eq!(
        cluster.deliver_matching(|envelope| envelope.to == NodeId(3)),
        1
    );
    assert_eq!(
        cluster.deliver_matching(|envelope| envelope.to == NodeId(2)),
        1
    );
}

pub(super) fn deliver_append_entries(from: NodeId, to: NodeId) -> impl FnMut(&Envelope) -> bool {
    move |envelope| {
        envelope.from == from
            && envelope.to == to
            && matches!(envelope.message, Message::AppendEntries(_))
    }
}

pub(super) fn deliver_append_entries_response(
    from: NodeId,
    to: NodeId,
) -> impl FnMut(&Envelope) -> bool {
    move |envelope| {
        envelope.from == from
            && envelope.to == to
            && matches!(envelope.message, Message::AppendEntriesResponse(_))
    }
}

pub(super) fn request_vote(from: NodeId, to: NodeId) -> impl FnMut(&Envelope) -> bool {
    move |envelope| {
        envelope.from == from
            && envelope.to == to
            && matches!(envelope.message, Message::RequestVote(_))
    }
}

pub(super) fn pre_vote(from: NodeId, to: NodeId) -> impl FnMut(&Envelope) -> bool {
    move |envelope| {
        envelope.from == from
            && envelope.to == to
            && matches!(envelope.message, Message::PreVote(_))
    }
}

pub(super) fn pre_vote_response(from: NodeId, to: NodeId) -> impl FnMut(&Envelope) -> bool {
    move |envelope| {
        envelope.from == from
            && envelope.to == to
            && matches!(envelope.message, Message::PreVoteResponse(_))
    }
}
