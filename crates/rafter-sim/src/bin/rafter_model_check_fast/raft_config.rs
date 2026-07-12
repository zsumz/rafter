use rafter::{NodeConfig, NodeId};

/// The plain legs pin the minimal protocol — no pre-vote, no check-quorum —
/// so their explored state spaces stay exactly the historically verified
/// ones; production and lease configs are explicit opt-ins.
pub(crate) fn three_node_configs(election_timeout_ticks: u64) -> Vec<NodeConfig> {
    vec![
        config(1, &[2, 3], election_timeout_ticks),
        config(2, &[1, 3], election_timeout_ticks),
        config(3, &[1, 2], election_timeout_ticks),
    ]
}

pub(crate) fn four_node_future_learner_configs(election_timeout_ticks: u64) -> Vec<NodeConfig> {
    vec![
        config(1, &[2, 3], election_timeout_ticks),
        config(2, &[1, 3], election_timeout_ticks),
        config(3, &[1, 2], election_timeout_ticks),
        non_voter_config(4, &[1, 2, 3], election_timeout_ticks),
    ]
}

pub(crate) fn four_node_future_learner_pre_vote_configs(
    election_timeout_ticks: u64,
) -> Vec<NodeConfig> {
    four_node_future_learner_configs(election_timeout_ticks)
        .into_iter()
        .map(|config| config.with_pre_vote(true))
        .collect()
}

pub(crate) fn four_node_future_learner_check_quorum_configs(
    election_timeout_ticks: u64,
) -> Vec<NodeConfig> {
    four_node_future_learner_configs(election_timeout_ticks)
        .into_iter()
        .map(|config| config.with_check_quorum(true))
        .collect()
}

fn config(id: u64, peers: &[u64], election_timeout_ticks: u64) -> NodeConfig {
    NodeConfig::new(
        NodeId(id),
        peers.iter().copied().map(NodeId).collect(),
        election_timeout_ticks,
    )
    .expect("fast model-check config must be valid")
    .with_pre_vote(false)
    .with_check_quorum(false)
}

fn non_voter_config(id: u64, peers: &[u64], election_timeout_ticks: u64) -> NodeConfig {
    NodeConfig::new_non_voter(
        NodeId(id),
        peers.iter().copied().map(NodeId).collect(),
        election_timeout_ticks,
    )
    .expect("fast model-check non-voter config must be valid")
    .with_pre_vote(false)
    .with_check_quorum(false)
}

pub(crate) fn three_node_pre_vote_configs(election_timeout_ticks: u64) -> Vec<NodeConfig> {
    three_node_configs(election_timeout_ticks)
        .into_iter()
        .map(|config| config.with_pre_vote(true))
        .collect()
}

pub(crate) fn three_node_check_quorum_configs(election_timeout_ticks: u64) -> Vec<NodeConfig> {
    three_node_configs(election_timeout_ticks)
        .into_iter()
        .map(|config| config.with_check_quorum(true))
        .collect()
}

pub(crate) fn three_node_production_configs(election_timeout_ticks: u64) -> Vec<NodeConfig> {
    vec![
        production_config(1, &[2, 3], election_timeout_ticks),
        production_config(2, &[1, 3], election_timeout_ticks),
        production_config(3, &[1, 2], election_timeout_ticks),
    ]
}

pub(crate) fn three_node_lease_configs(election_timeout_ticks: u64) -> Vec<NodeConfig> {
    three_node_production_configs(election_timeout_ticks)
        .into_iter()
        .map(|config| config.with_lease_reads(true))
        .collect()
}

fn production_config(id: u64, peers: &[u64], election_timeout_ticks: u64) -> NodeConfig {
    NodeConfig::new(
        NodeId(id),
        peers.iter().copied().map(NodeId).collect(),
        election_timeout_ticks,
    )
    .expect("production model-check config must be valid")
}

/// Explicit per-follower in-flight append window coverage: the replication
/// legs must explore both the serialized window of one and a pipelined
/// window, not silently inherit the default.
pub(crate) fn three_node_configs_with_inflight_window(
    election_timeout_ticks: u64,
    max_inflight_appends: usize,
) -> Vec<NodeConfig> {
    three_node_configs(election_timeout_ticks)
        .into_iter()
        .map(|config| config.with_max_inflight_appends(max_inflight_appends))
        .collect()
}
