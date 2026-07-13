use rafter::{
    BootstrapLogEntry, BootstrapState, CommittedConfiguration, ConfigurationEntry, ConfigurationId,
    JointMembership, LogIndex, MembershipSet, Message, NodeConfig, NodeId, RequestVote, Term,
};

use crate::Cluster;

use super::super::{
    explorers::{CommitSafetyExplorer, ElectionSafetyExplorer, RestartSafetyExplorer},
    helpers::{
        bootstrap_state, bootstrap_with_snapshot, config, deliver_all_in_state,
        elect_node_one_in_state, elect_node_one_with_node_three_in_state, summarize, test_snapshot,
        three_node_configs,
    },
    observations::Observation,
    scheduling::Operation,
    state::{
        apply_pending_application_replay_seed, apply_snapshot_bootstrap_seeds,
        apply_to_restart_snapshot_state, apply_to_state, restart_node,
        PendingApplicationReplaySeed, SnapshotBootstrapSeed,
    },
    state::{ExpectedSnapshot, ExplorationState, RestartSnapshotState},
    Bounds, Failure, FailureKind, StateSummary, Summary,
};

/// Runs compact deterministic scenarios for semantic detector branches that
/// are intentionally rare in the broad bounded explorers.
///
/// # Errors
///
/// Returns [`Failure`] when a scenario violates an invariant or fails to
/// exercise every required detector branch.
pub fn check_raft_semantic_witness_safety() -> Result<Summary, Failure> {
    let mut summary = nonvoter_vote_summary()?;
    summary = summary.combined(joint_election_summary()?);
    summary = summary.combined(post_append_joint_commit_summary()?);
    summary = summary.combined(same_boundary_snapshot_pair_summary()?);
    summary = summary.combined(leader_pre_vote_stability_summary()?);
    summary = summary.combined(pending_application_replay_summary()?);
    Ok(summary)
}

fn leader_pre_vote_stability_summary() -> Result<Summary, Failure> {
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

fn pending_application_replay_summary() -> Result<Summary, Failure> {
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

fn pre_vote_config(
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

fn joint_election_summary() -> Result<Summary, Failure> {
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

fn nonvoter_vote_summary() -> Result<Summary, Failure> {
    let mut cluster = Cluster::new(four_node_future_learner_configs()?);
    cluster.queue_message(
        NodeId(4),
        NodeId(1),
        Message::RequestVote(RequestVote {
            term: Term(1),
            candidate_id: NodeId(4),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        }),
    );
    let state = ExplorationState::new(cluster);
    let state_summary = summarize(state.cluster());
    let mut explorer = ElectionSafetyExplorer::new(Bounds::new(1));
    explorer.explore(&state, &mut Vec::new(), 0)?;
    require_observation(
        explorer.summary(),
        Observation::NonvoterVoteDecisions,
        state_summary,
    )
}

fn post_append_joint_commit_summary() -> Result<Summary, Failure> {
    let mut state = ExplorationState::new(Cluster::new(vec![config(1, &[], 1)]));
    elect_node_one_in_state(&mut state);
    let target = MembershipSet::new(vec![NodeId(1)], vec![NodeId(2)]).map_err(|error| {
        witness_harness_error(format!("build single-voter target membership: {error:?}"))
    })?;
    apply_to_state(
        &mut state,
        Operation::EnterJoint {
            to: NodeId(1),
            target,
            promotion_barriers: Vec::new(),
        },
    );
    let state_summary = summarize(state.cluster());
    let mut explorer = CommitSafetyExplorer::new(Bounds::new(0));
    explorer.explore(&state, &mut Vec::new(), 0)?;
    require_observation(
        explorer.summary(),
        Observation::PostAppendJointCommitCertificates,
        state_summary,
    )
}

fn same_boundary_snapshot_pair_summary() -> Result<Summary, Failure> {
    let mut state = snapshot_pair_state()?;
    for _ in 0..256 {
        if state.state.cluster().snapshot_installs().len() >= 2 {
            break;
        }
        if state.state.cluster().pending().next().is_none() {
            break;
        }
        apply_to_restart_snapshot_state(&mut state, Operation::DeliverReadyAt(0), &[])?;
    }
    let state_summary = summarize(state.state.cluster());
    let mut explorer = RestartSafetyExplorer::new(Bounds::new(0));
    explorer.explore(&state, &mut Vec::new(), 0)?;
    require_observation(
        explorer.summary(),
        Observation::SameBoundarySnapshotInstallPairs,
        state_summary,
    )
}

fn snapshot_pair_state() -> Result<RestartSnapshotState, Failure> {
    let mut cluster = Cluster::new(three_node_configs());
    let (snapshot, payload) = test_snapshot(1, 2, 1, 2, b"snapshot boundary");
    cluster
        .restart_node_from_bootstrap(
            NodeId(1),
            bootstrap_state(
                Term(2),
                &[
                    (1, Term(1), b"old prefix"),
                    (2, Term(1), b"snapshot boundary"),
                ],
            ),
        )
        .map_err(|error| witness_harness_error(format!("seed visible leader: {error:?}")))?;
    for node_id in [NodeId(2), NodeId(3)] {
        cluster
            .restart_node_from_bootstrap(
                node_id,
                bootstrap_state(Term(2), &[(1, Term(1), b"old prefix")]),
            )
            .map_err(|error| {
                witness_harness_error(format!("seed lagging follower {node_id}: {error:?}"))
            })?;
    }
    let mut state = ExplorationState::new(cluster);
    apply_snapshot_bootstrap_seeds(
        &mut state,
        vec![SnapshotBootstrapSeed {
            node_id: NodeId(1),
            snapshot: snapshot.clone(),
            payload: payload.clone(),
            bootstrap: bootstrap_with_snapshot(Term(2), snapshot.clone(), &[]),
        }],
    )
    .map_err(|error| witness_harness_error(format!("seed compacted leader: {error:?}")))?;
    elect_node_one_with_node_three_in_state(&mut state);
    Ok(RestartSnapshotState {
        state,
        expected_snapshot: Some(ExpectedSnapshot {
            snapshot,
            payload: payload.into(),
        }),
        divergent_payloads: Vec::new(),
    })
}

fn require_observation(
    summary: Summary,
    observation: Observation,
    state: crate::model_check::StateSummary,
) -> Result<Summary, Failure> {
    if summary.observations.contains(observation) {
        return Ok(summary);
    }
    Err(Failure {
        kind: FailureKind::CoverageNotReached,
        invariant: "verification-semantic-witness",
        message: format!("semantic witness {} was not reached", observation.label()),
        trace: Vec::new(),
        state,
    })
}

fn four_node_future_learner_configs() -> Result<Vec<NodeConfig>, Failure> {
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

fn witness_harness_error(message: String) -> Failure {
    Failure {
        kind: FailureKind::HarnessError,
        invariant: "verification-semantic-witness",
        message,
        trace: Vec::new(),
        state: StateSummary { nodes: Vec::new() },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn semantic_witness_scenarios_reach_all_rare_branches() {
        let summary = check_raft_semantic_witness_safety().expect("semantic witness leg passes");
        for observation in [
            Observation::NonvoterVoteDecisions,
            Observation::JointElectionCertificates,
            Observation::PostAppendJointCommitCertificates,
            Observation::SameBoundarySnapshotInstallPairs,
            Observation::LeaderPreVoteRequestDeliveries,
            Observation::RestartNonemptyExpectedReplayComparisons,
        ] {
            assert!(summary.observations.contains(observation));
        }
    }
}
