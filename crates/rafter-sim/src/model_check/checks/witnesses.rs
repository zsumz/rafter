//! Compact scenarios for detector branches the broad explorers rarely reach.
//!
//! Each scenario drives one deliberately rare branch — a non-voter vote, a
//! joint election certificate, a post-append joint commit, a same-boundary
//! snapshot pair, a leader pre-vote, a pending application replay — and fails
//! when its branch goes unexercised, so no detector stays permanently untested.

use rafter::{LogIndex, Message, NodeId, RequestVote, Term};

use crate::Cluster;

use super::super::{
    explorers::ElectionSafetyExplorer, helpers::summarize, observations::Observation,
    state::ExplorationState, Bounds, Failure, FailureKind, StateSummary, Summary,
};

mod commit;
mod election;

use commit::{post_append_joint_commit_summary, same_boundary_snapshot_pair_summary};
use election::{
    four_node_future_learner_configs, joint_election_summary, leader_pre_vote_stability_summary,
    pending_application_replay_summary,
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
