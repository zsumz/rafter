use rafter::{LogIndex, Message, NodeId};

use super::super::application::apply_to_state;
use super::super::helpers::{config, elect_node_one, summarize, three_node_configs};
use super::super::scheduling::Operation;
use super::super::state::{ClientReadOutcome, ClientWriteStatus, ExplorationState};
use super::super::{
    catalog, check_raft_leadership_noop_safety, check_raft_restart_and_snapshot_safety,
    check_raft_seeded_commit_safety, Bounds, Failure, FailureKind, ProposalId,
};
use crate::Cluster;

#[test]
fn seeded_commit_safety_passes_for_precommitted_and_prediverged_followers() {
    let summary = check_raft_seeded_commit_safety(
        vec![
            config(1, &[2, 3], 2),
            config(2, &[1, 3], 2),
            config(3, &[1, 2], 2),
        ],
        Bounds::new(1),
    )
    .expect("seeded commit safety should pass");

    assert!(summary.explored_states() > 2);
    assert!(summary.unique_states() > 2);
    assert!(summary.unique_states() <= summary.explored_states());
    assert!(summary.explored_actions() > 2);
    assert_eq!(summary.max_depth(), 1);
}

#[test]
fn seeded_leadership_noop_safety_passes_for_targeted_cases() {
    let summary = check_raft_leadership_noop_safety(Bounds::new(8))
        .expect("seeded leadership no-op safety should pass");

    assert!(summary.explored_states() > 4);
    assert!(summary.unique_states() > 4);
    assert!(summary.unique_states() <= summary.explored_states());
    assert!(summary.explored_actions() > 4);
    assert_eq!(summary.max_depth(), 8);
}

#[test]
fn shallow_restart_check_reports_coverage_not_reached() {
    let failure = check_raft_restart_and_snapshot_safety(Bounds::new(0))
        .expect_err("depth zero cannot reach a restart witness");

    assert_eq!(failure.kind(), FailureKind::CoverageNotReached);
    assert_eq!(failure.invariant(), catalog::PS_03_EXACT_DURABLE_RESTART);
    assert!(failure.message().contains("did not reach a restart action"));
}

#[test]
fn shallow_leadership_noop_check_reports_coverage_not_reached() {
    let failure = check_raft_leadership_noop_safety(Bounds::new(0))
        .expect_err("depth zero cannot reach the seeded apply witness");

    assert_eq!(failure.kind(), FailureKind::CoverageNotReached);
    assert_eq!(
        failure.invariant(),
        catalog::AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION
    );
    assert!(failure.message().contains("did not reach required Apply"));
}

#[test]
fn failure_kind_is_explicit_not_derived_from_message_text() {
    let state = ExplorationState::new(Cluster::new(three_node_configs()));
    let failure = Failure {
        kind: FailureKind::InvariantViolation,
        invariant: catalog::AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION,
        message: "did not reach required Apply appears in this invariant message".to_string(),
        trace: Vec::new(),
        state: summarize(&state.cluster),
    };

    assert_eq!(failure.kind(), FailureKind::InvariantViolation);
}

#[test]
fn seeded_single_voter_prior_application_noop_requires_apply() {
    let mut state = ExplorationState::seeded_single_voter_prior_application_noop();

    state.cluster.tick(NodeId(1));

    assert_eq!(state.cluster.commit_index(NodeId(1)), LogIndex(2));
    assert!(state.cluster.applied().iter().any(|applied| {
        applied.node_id == NodeId(1)
            && applied.index == LogIndex(1)
            && applied.payload.as_ref() == b"leadership-noop-prior-app"
    }));
}

#[test]
fn seeded_single_voter_prior_configuration_noop_commits_identity() {
    let mut state = ExplorationState::seeded_single_voter_prior_configuration_noop();

    state.cluster.tick(NodeId(1));

    assert_eq!(state.cluster.commit_index(NodeId(1)), LogIndex(2));
    assert_eq!(
        state.cluster.committed_configuration_state(NodeId(1)),
        Some(rafter::CommittedConfiguration {
            index: LogIndex(1),
            config_id: rafter::ConfigurationId(7),
        })
    );
}

#[test]
fn seeded_joint_self_quorum_prior_application_noop_applies_suffix() {
    let mut state = ExplorationState::seeded_joint_self_quorum_prior_application_noop();

    state.cluster.tick(NodeId(1));

    assert_eq!(state.cluster.commit_index(NodeId(1)), LogIndex(3));
    assert!(state.cluster.applied().iter().any(|applied| {
        applied.node_id == NodeId(1)
            && applied.index == LogIndex(2)
            && applied.payload.as_ref() == b"joint-self-quorum-prior-app"
    }));
}

#[test]
fn seeded_leadership_transfer_reaches_target_noop_commit() {
    let mut state = ExplorationState::seeded_leadership_transfer_noop_commit();

    state.cluster.deliver_all();

    assert_eq!(state.cluster.role(NodeId(2)), rafter::Role::Leader);
    assert!(state.cluster.commit_index(NodeId(2)) >= LogIndex(2));
}

#[test]
fn seeded_low_empty_probe_keeps_precommitted_floor() {
    let mut state = ExplorationState::seeded_low_empty_probe(vec![
        config(1, &[2, 3], 2),
        config(2, &[1, 3], 2),
        config(3, &[1, 2], 2),
    ]);

    assert_eq!(state.cluster.commit_index(NodeId(2)), LogIndex(1));
    assert!(state.cluster.pending().any(|envelope| {
        envelope.from == NodeId(1)
            && envelope.to == NodeId(2)
            && matches!(
                &envelope.message,
                Message::AppendEntries(append)
                    if append.prev_log_index == LogIndex::ZERO
                        && append.entries.is_empty()
                        && append.leader_commit == LogIndex(3)
            )
    }));

    state.cluster.deliver_all();
    assert_eq!(state.cluster.commit_index(NodeId(2)), LogIndex(1));
}

#[test]
fn seeded_divergent_suffix_probe_confirms_only_the_shared_prefix() {
    let mut state = ExplorationState::seeded_divergent_suffix_probe(vec![
        config(1, &[2, 3], 2),
        config(2, &[1, 3], 2),
        config(3, &[1, 2], 2),
    ]);

    assert_eq!(state.cluster.commit_index(NodeId(1)), LogIndex(2));
    assert_eq!(state.cluster.commit_index(NodeId(2)), LogIndex(1));
    assert!(state.cluster.pending().any(|envelope| {
        envelope.from == NodeId(1)
            && envelope.to == NodeId(2)
            && matches!(
                &envelope.message,
                Message::AppendEntries(append)
                    if append.prev_log_index == LogIndex(1)
                        && append.entries.is_empty()
                        && append.leader_commit == LogIndex(2)
            )
    }));

    state.cluster.deliver_all();
    assert_eq!(state.cluster.commit_index(NodeId(2)), LogIndex(1));
    assert!(!state.cluster.applied().iter().any(|applied| {
        applied.node_id == NodeId(2) && applied.payload.as_ref() == b"divergent-two"
    }));
}

#[test]
fn client_history_records_write_completion_and_read_proof() {
    let mut cluster = Cluster::new(three_node_configs());
    elect_node_one(&mut cluster);
    cluster.propose(NodeId(1), b"history-seed".to_vec());
    cluster.deliver_all();
    let mut state = ExplorationState::new(cluster);

    apply_to_state(
        &mut state,
        Operation::Propose {
            to: NodeId(1),
            proposal_id: ProposalId(42),
            stale_leader: false,
        },
    );
    state.cluster.deliver_all();
    state.refresh_client_history();
    let write = &state.client_history.writes[&ProposalId(42)];
    assert!(matches!(
        write.status,
        ClientWriteStatus::Completed { index, .. } if index > LogIndex::ZERO
    ));

    apply_to_state(
        &mut state,
        Operation::ReadIndex {
            to: NodeId(1),
            request_id: 77,
        },
    );
    state.cluster.deliver_all();
    state.refresh_client_history();
    let read = &state.client_history.reads[&77];
    match &read.outcome {
        ClientReadOutcome::Completed { proof, result, .. } => {
            assert!(proof.read_index >= read.committed_floor);
            assert!(proof.local_applied_index >= proof.read_index);
            assert!(result.is_some());
        }
        ClientReadOutcome::ProofGranted { proof } => {
            assert!(proof.read_index >= read.committed_floor);
        }
        ClientReadOutcome::Pending => panic!("read should have reached a proof or completion"),
    }
}
