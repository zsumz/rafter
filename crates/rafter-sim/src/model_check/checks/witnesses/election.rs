//! Rare election-path witnesses for the semantic detector leg.
//!
//! Each scenario drives one deliberately uncommon branch — a non-voter vote,
//! a joint-membership election, a leader receiving a pre-vote request, and a
//! restart replaying a nonempty pending prefix — to a single observation.

use rafter::{
    BootstrapLogEntry, BootstrapState, CommittedConfiguration, ConfigurationEntry, ConfigurationId,
    JointMembership, LogIndex, MembershipSet, Message, NodeConfig, NodeId, Term,
};

use crate::Cluster;

use super::super::super::{
    explorers::ElectionSafetyExplorer,
    helpers::{
        config, deliver_all_in_state, elect_node_one_in_state, summarize, three_node_configs,
    },
    observations::Observation,
    scheduling::Operation,
    state::ExplorationState,
    state::{
        apply_pending_application_replay_seed, apply_to_state, restart_node,
        PendingApplicationReplaySeed,
    },
    Bounds, Failure, Summary,
};

use super::{require_observation, witness_harness_error};

pub(super) fn leader_pre_vote_stability_summary() -> Result<Summary, Failure> {
    let mut state = ExplorationState::new(Cluster::new(vec![
        pre_vote_config(1, &[2, 3], 3)?,
        pre_vote_config(2, &[1, 3], 9)?,
        pre_vote_config(3, &[1, 2], 9)?,
    ]));
    for _ in 0..3 {
        apply_to_state(&mut state, Operation::Tick(NodeId(1)));
    }
    deliver_all_in_state(&mut state);
    if state.cluster().leaders() != [NodeId(1)] {
        return Err(witness_harness_error(
            "pre-vote witness failed to elect node-1".to_owned(),
        ));
    }
    for _ in 0..18 {
        apply_to_state(&mut state, Operation::Tick(NodeId(3)));
    }
    let position = state
        .cluster()
        .pending()
        .enumerate()
        .find_map(|(position, envelope)| {
            (envelope.from == NodeId(3)
                && envelope.to == NodeId(1)
                && matches!(envelope.message, Message::PreVote(_)))
            .then_some(position)
        })
        .ok_or_else(|| {
            witness_harness_error(
                "pre-vote witness did not queue a request to the leader".to_owned(),
            )
        })?;
    apply_to_state(&mut state, Operation::DeliverReadyAt(position));
    let state_summary = summarize(state.cluster());
    let mut explorer = ElectionSafetyExplorer::new(Bounds::new(0));
    explorer.explore(&state, &mut Vec::new(), 0)?;
    require_observation(
        explorer.summary(),
        Observation::LeaderPreVoteRequestDeliveries,
        state_summary,
    )
}

pub(super) fn pending_application_replay_summary() -> Result<Summary, Failure> {
    let mut state = ExplorationState::new(Cluster::new(vec![config(1, &[], 3)]));
    apply_pending_application_replay_seed(
        &mut state,
        PendingApplicationReplaySeed {
            node_id: NodeId(1),
            bootstrap: BootstrapState {
                current_term: Term(1),
                voted_for: None,
                commit_index: LogIndex(1),
                committed_configuration: None,
                snapshot: None,
                log: vec![BootstrapLogEntry::application(
                    LogIndex(1),
                    Term(1),
                    b"pending-application-replay".to_vec(),
                )],
            },
        },
    )
    .map_err(|error| witness_harness_error(format!("seed pending replay: {error:?}")))?;
    state.witness_seeded_commit_authority(LogIndex::ZERO, LogIndex(1), Term(1));
    restart_node(&mut state, NodeId(1), &[])?;
    let state_summary = summarize(state.cluster());
    let mut explorer = ElectionSafetyExplorer::new(Bounds::new(0));
    explorer.explore(&state, &mut Vec::new(), 0)?;
    require_observation(
        explorer.summary(),
        Observation::RestartNonemptyExpectedReplayComparisons,
        state_summary,
    )
}

pub(super) fn pre_vote_config(
    id: u64,
    peers: &[u64],
    election_timeout_ticks: u64,
) -> Result<NodeConfig, Failure> {
    NodeConfig::new(
        NodeId(id),
        peers.iter().copied().map(NodeId).collect(),
        election_timeout_ticks,
    )
    .map_err(|error| witness_harness_error(format!("build pre-vote node {id}: {error:?}")))
}

pub(super) fn joint_election_summary() -> Result<Summary, Failure> {
    let config_id = ConfigurationId(17);
    let old = MembershipSet::new(vec![NodeId(1), NodeId(2)], Vec::new())
        .map_err(|error| witness_harness_error(format!("build old voter set: {error:?}")))?;
    let new = MembershipSet::new(vec![NodeId(1), NodeId(3)], Vec::new())
        .map_err(|error| witness_harness_error(format!("build new voter set: {error:?}")))?;
    let configuration = ConfigurationEntry::joint(config_id, JointMembership::new(old, new));
    let mut cluster = Cluster::new(three_node_configs());
    for node_id in [NodeId(1), NodeId(2), NodeId(3)] {
        cluster
            .restart_node_from_bootstrap(
                node_id,
                BootstrapState {
                    current_term: Term(1),
                    voted_for: None,
                    commit_index: LogIndex(1),
                    committed_configuration: Some(CommittedConfiguration {
                        index: LogIndex(1),
                        config_id,
                    }),
                    snapshot: None,
                    log: vec![BootstrapLogEntry::configuration(
                        LogIndex(1),
                        Term(1),
                        configuration.clone(),
                    )],
                },
            )
            .map_err(|error| {
                witness_harness_error(format!("seed joint voter {node_id}: {error:?}"))
            })?;
    }
    let mut state = ExplorationState::new(cluster);
    state.witness_seeded_commit_authority(LogIndex::ZERO, LogIndex(1), Term(1));
    elect_node_one_in_state(&mut state);
    let state_summary = summarize(state.cluster());
    let mut explorer = ElectionSafetyExplorer::new(Bounds::new(0));
    explorer.explore(&state, &mut Vec::new(), 0)?;
    require_observation(
        explorer.summary(),
        Observation::JointElectionCertificates,
        state_summary,
    )
}

pub(super) fn four_node_future_learner_configs() -> Result<Vec<NodeConfig>, Failure> {
    let learner = NodeConfig::new_non_voter(NodeId(4), vec![NodeId(1), NodeId(2), NodeId(3)], 3)
        .map_err(|error| witness_harness_error(format!("build future learner: {error:?}")))?
        .with_pre_vote(false)
        .with_check_quorum(false);
    Ok(vec![
        config(1, &[2, 3], 3),
        config(2, &[1, 3], 3),
        config(3, &[1, 2], 3),
        learner,
    ])
}
