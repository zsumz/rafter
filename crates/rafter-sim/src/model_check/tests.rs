use super::application::apply_to_state;
use super::helpers::{
    config, elect_node_one, four_node_future_learner_configs, proposal_payload, request_vote,
    three_node_lease_configs,
};
use super::scheduling::enabled_soak_actions;
use super::scheduling::Operation;
use super::state::ExplorationState;
use super::state::{ClientReadOutcome, ClientWriteStatus};
use super::*;
use crate::SimSeed;
use rafter::{LogIndex, Message, NodeId};
use std::collections::BTreeSet;
use std::time::Duration;

#[path = "tests/linearizability.rs"]
mod linearizability;

#[test]
fn bounded_raft_election_safety_passes_for_three_node_cluster() {
    let summary = check_raft_election_safety(
        vec![
            config(1, &[2, 3], 2),
            config(2, &[1, 3], 2),
            config(3, &[1, 2], 2),
        ],
        Bounds::new(6),
    )
    .expect("bounded election safety check should pass");

    assert!(summary.explored_states() > 1);
    assert!(summary.unique_states() > 1);
    assert!(summary.unique_states() <= summary.explored_states());
    assert!(summary.explored_actions() > 1);
    assert_eq!(summary.max_depth(), 6);
}

#[test]
fn bounded_raft_commit_safety_passes_for_three_node_cluster() {
    let summary = check_raft_commit_safety(
        vec![
            config(1, &[2, 3], 2),
            config(2, &[1, 3], 2),
            config(3, &[1, 2], 2),
        ],
        Bounds::new(8).with_max_proposals(1),
    )
    .expect("bounded commit safety check should pass");

    assert!(summary.explored_states() > 1);
    assert!(summary.unique_states() > 1);
    assert!(summary.unique_states() <= summary.explored_states());
    assert!(summary.explored_actions() > 1);
    assert_eq!(summary.max_depth(), 8);
}

#[test]
fn bounded_raft_membership_safety_passes_for_future_learner_cluster() {
    let summary = check_raft_membership_safety(
        four_node_future_learner_configs(),
        Bounds::new(5)
            .with_max_proposals(1)
            .with_max_membership_changes(1),
    )
    .expect("bounded membership safety check should pass");

    assert!(summary.explored_states() > 1);
    assert!(summary.unique_states() > 1);
    assert!(summary.unique_states() <= summary.explored_states());
    assert!(summary.explored_actions() > 1);
    assert_eq!(summary.max_depth(), 5);
}

#[test]
fn bounded_raft_membership_safety_does_not_require_client_proposals() {
    let summary = check_raft_membership_safety(
        four_node_future_learner_configs(),
        Bounds::new(4).with_max_membership_changes(1),
    )
    .expect("membership actions should not depend on the client proposal budget");

    assert!(summary.explored_actions() > 1);
    assert_eq!(summary.max_depth(), 4);
}

#[test]
fn bounded_joint_membership_restart_and_snapshot_safety_passes() {
    let summary = check_raft_joint_membership_restart_and_snapshot_safety(
        Bounds::new(8).with_max_restarts(1),
    )
    .expect("joint-membership restart and snapshot safety check should pass");

    assert!(summary.explored_states() > 1);
    assert!(summary.unique_states() > 1);
    assert!(summary.unique_states() <= summary.explored_states());
    assert!(summary.explored_actions() > 1);
    assert_eq!(summary.max_depth(), 12);
}

#[test]
fn unique_state_budget_stops_expansion_without_failing() {
    let summary = check_raft_election_safety(
        vec![
            config(1, &[2, 3], 2),
            config(2, &[1, 3], 2),
            config(3, &[1, 2], 2),
        ],
        Bounds::new(6).with_max_unique_states(1),
    )
    .expect("bounded election safety check should stop at the unique-state cap");

    assert_eq!(summary.unique_states(), 1);
    assert!(summary.explored_states() > summary.unique_states());
    assert!(summary.explored_actions() > 0);
    assert_eq!(summary.max_depth(), 6);
}

#[test]
fn bounds_expose_dedup_budget_controls() {
    let bounds = Bounds::new(7)
        .with_max_unique_states(42)
        .with_max_wall_clock(Duration::from_secs(5));

    assert_eq!(bounds.max_depth(), 7);
    assert_eq!(bounds.max_unique_states(), Some(42));
    assert_eq!(bounds.max_wall_clock(), Some(Duration::from_secs(5)));
}

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

#[test]
fn bounded_raft_restart_and_snapshot_safety_passes() {
    let summary = check_raft_restart_and_snapshot_safety(Bounds::new(8).with_max_restarts(1))
        .expect("bounded restart and snapshot safety check should pass");

    assert!(summary.explored_states() > 1);
    assert!(summary.unique_states() > 1);
    assert!(summary.unique_states() <= summary.explored_states());
    assert!(summary.explored_actions() > 1);
    assert_eq!(summary.max_depth(), 12);
}

#[test]
fn replay_raft_trace_reaches_expected_final_state() {
    let configs = vec![
        config(1, &[2, 3], 2),
        config(2, &[1, 3], 2),
        config(3, &[1, 2], 2),
    ];
    let trace = vec![
        Action::Tick(NodeId(1)),
        Action::Tick(NodeId(1)),
        Action::Deliver {
            from: NodeId(1),
            to: NodeId(2),
            message: MessageKind::RequestVote,
        },
    ];

    let mut expected_cluster = Cluster::new(configs.clone());
    expected_cluster.tick(NodeId(1));
    expected_cluster.tick(NodeId(1));
    assert!(expected_cluster.deliver_one_matching(request_vote(NodeId(1), NodeId(2))));
    let expected = summarize(&expected_cluster);

    let report = replay_raft_trace(
        configs,
        &trace,
        ReplayCheck::ElectionSafety,
        ReplayExpectation::FinalState(&expected),
    )
    .expect("trace replay should reach the expected final state");

    assert_eq!(report.state(), &expected);
    assert!(report.failure().is_none());
    assert_eq!(trace[2].to_string(), "deliver request_vote node-1->node-2");
}

#[test]
fn commit_safety_allows_old_leader_commit_before_newer_candidate_wins() {
    let configs = vec![
        config(1, &[2, 3], 2),
        config(2, &[1, 3], 2),
        config(3, &[1, 2], 2),
    ];
    let trace = vec![
        Action::Tick(NodeId(1)),
        Action::Tick(NodeId(1)),
        Action::Deliver {
            from: NodeId(1),
            to: NodeId(2),
            message: MessageKind::RequestVote,
        },
        Action::Tick(NodeId(2)),
        Action::Tick(NodeId(2)),
        Action::Deliver {
            from: NodeId(2),
            to: NodeId(1),
            message: MessageKind::RequestVoteResponse,
        },
        Action::Propose {
            to: NodeId(1),
            proposal_id: ProposalId(1),
        },
        Action::Deliver {
            from: NodeId(1),
            to: NodeId(3),
            message: MessageKind::AppendEntries,
        },
        Action::Deliver {
            from: NodeId(3),
            to: NodeId(1),
            message: MessageKind::AppendEntriesResponse,
        },
    ];

    let mut expected_cluster = Cluster::new(configs.clone());
    expected_cluster.tick(NodeId(1));
    expected_cluster.tick(NodeId(1));
    assert!(expected_cluster.deliver_one_matching(request_vote(NodeId(1), NodeId(2))));
    expected_cluster.tick(NodeId(2));
    expected_cluster.tick(NodeId(2));
    assert!(expected_cluster.deliver_one_matching(|envelope| {
        envelope.from == NodeId(2)
            && envelope.to == NodeId(1)
            && matches!(envelope.message, Message::RequestVoteResponse(_))
    }));
    expected_cluster.propose(NodeId(1), proposal_payload(ProposalId(1)));
    assert!(expected_cluster.deliver_one_matching(|envelope| {
        envelope.from == NodeId(1)
            && envelope.to == NodeId(3)
            && matches!(envelope.message, Message::AppendEntries(_))
    }));
    assert!(expected_cluster.deliver_one_matching(|envelope| {
        envelope.from == NodeId(3)
            && envelope.to == NodeId(1)
            && matches!(envelope.message, Message::AppendEntriesResponse(_))
    }));
    let expected = summarize(&expected_cluster);

    let report = replay_raft_trace(
        configs,
        &trace,
        ReplayCheck::CommitSafety,
        ReplayExpectation::FinalState(&expected),
    )
    .expect("a higher-term candidate is not yet a newer-term winning leader");

    assert_eq!(report.state(), &expected);
    assert!(report.failure().is_none());
}

#[test]
fn raft_trace_projects_to_tla_action_vocabulary() {
    let trace = vec![
        Action::Tick(NodeId(1)),
        Action::Propose {
            to: NodeId(1),
            proposal_id: ProposalId(7),
        },
        Action::ReadIndex {
            to: NodeId(1),
            request_id: 11,
        },
        Action::Restart(NodeId(3)),
        Action::Deliver {
            from: NodeId(1),
            to: NodeId(2),
            message: MessageKind::RequestVote,
        },
        Action::Deliver {
            from: NodeId(1),
            to: NodeId(2),
            message: MessageKind::AppendEntries,
        },
    ];

    let projection = project_raft_trace_to_tla(&trace);
    let actions = require_tla_projectable_raft_trace(&trace)
        .expect("trace should fit the abstract TLA+ action vocabulary");

    assert_eq!(
        actions,
        vec![
            TlaAction::Timeout { node_id: NodeId(1) },
            TlaAction::ClientAppend { node_id: NodeId(1) },
            TlaAction::RegisterRead { node_id: NodeId(1) },
            TlaAction::Restart { node_id: NodeId(3) },
            TlaAction::DeliverRequestVote {
                from: NodeId(1),
                to: NodeId(2),
            },
            TlaAction::DeliverAppend {
                from: NodeId(1),
                to: NodeId(2),
            },
        ]
    );
    assert_eq!(actions[0].tla_name(), "Timeout");
    assert_eq!(actions[2].tla_name(), "RegisterRead");
    assert_eq!(actions[3].tla_name(), "Restart");
    assert_eq!(actions[5].to_string(), "DeliverAppend(node-1->node-2)");
    assert_eq!(projection[1].rust_action(), &trace[1]);
    assert_eq!(
        projection[1].projection(),
        TlaProjection::Action(actions[1])
    );
}

#[test]
fn raft_trace_tla_projection_names_abstraction_gaps() {
    let trace = vec![
        Action::Deliver {
            from: NodeId(2),
            to: NodeId(1),
            message: MessageKind::AppendEntriesResponse,
        },
        Action::Deliver {
            from: NodeId(2),
            to: NodeId(1),
            message: MessageKind::PreVote,
        },
        Action::AddLearner {
            to: NodeId(1),
            learner_id: NodeId(4),
        },
    ];

    let projection = project_raft_trace_to_tla(&trace);
    let failure = require_tla_projectable_raft_trace(&trace)
        .expect_err("response messages are an implementation-level gap");

    assert_eq!(
        projection[0].projection(),
        TlaProjection::Gap(TlaAbstractionGap::AppendEntriesResponse)
    );
    assert_eq!(
        projection[1].projection(),
        TlaProjection::Gap(TlaAbstractionGap::PreVote)
    );
    assert_eq!(
        projection[2].projection(),
        TlaProjection::Gap(TlaAbstractionGap::MembershipChange)
    );
    assert_eq!(failure.action_index(), 0);
    assert_eq!(failure.action(), &trace[0]);
    assert_eq!(failure.gap().code(), "append_entries_response_abstracted");
}

#[test]
fn raft_trace_renders_tla_tlc_checkable_sample_spec() {
    let trace = vec![
        Action::Tick(NodeId(1)),
        Action::Restart(NodeId(2)),
        Action::Tick(NodeId(2)),
    ];
    let spec = render_tla_trace_spec("RaftTraceSample", &trace)
        .expect("sample trace should fit the abstract TLA+ action subset");

    assert_eq!(
        spec.module(),
        include_str!("../../../../specs/tla/raft/RaftTraceSample.tla")
    );
    assert_eq!(
        spec.config(),
        include_str!("../../../../specs/tla/raft/RaftTraceSample.cfg")
    );
}

#[test]
fn raft_trace_tla_render_rejects_node_outside_config_bound() {
    let trace = vec![Action::Tick(NodeId(4))];
    let error = render_tla_trace_spec("InvalidNodeTrace", &trace)
        .expect_err("n4 is outside the generated three-node TLA+ config");
    let text = error.to_string();

    match error {
        TlaTraceRenderError::NodeOutOfBounds {
            action_index,
            action,
            node_id,
        } => {
            assert_eq!(action_index, 0);
            assert_eq!(action, TlaAction::Timeout { node_id: NodeId(4) });
            assert_eq!(node_id, NodeId(4));
        }
        other => panic!("unexpected render error: {other}"),
    }
    assert!(text.contains("trace action 0"));
    assert!(text.contains("n4"));
}

#[test]
fn raft_trace_tla_render_rejects_too_many_proposal_values() {
    let trace = vec![
        Action::Propose {
            to: NodeId(1),
            proposal_id: ProposalId(1),
        },
        Action::Propose {
            to: NodeId(1),
            proposal_id: ProposalId(2),
        },
        Action::Propose {
            to: NodeId(1),
            proposal_id: ProposalId(3),
        },
    ];
    let error = render_tla_trace_spec("TooManyValuesTrace", &trace)
        .expect_err("third proposal needs a third symbolic TLA+ value");
    let text = error.to_string();

    match error {
        TlaTraceRenderError::TooManyValues {
            action_index,
            requested_value,
            available_values,
            ..
        } => {
            assert_eq!(action_index, 2);
            assert_eq!(requested_value, 3);
            assert_eq!(available_values, 2);
        }
        other => panic!("unexpected render error: {other}"),
    }
    assert!(text.contains("trace action 2"));
    assert!(text.contains("v3"));
}

#[test]
fn raft_trace_tla_render_rejects_too_many_read_requests() {
    let trace = vec![
        Action::ReadIndex {
            to: NodeId(1),
            request_id: 1,
        },
        Action::ReadIndex {
            to: NodeId(1),
            request_id: 2,
        },
        Action::ReadIndex {
            to: NodeId(1),
            request_id: 3,
        },
    ];
    let error = render_tla_trace_spec("TooManyReadsTrace", &trace)
        .expect_err("third read needs a third symbolic TLA+ read request");
    let text = error.to_string();

    match error {
        TlaTraceRenderError::TooManyReadRequests {
            action_index,
            requested_read_request,
            available_read_requests,
            ..
        } => {
            assert_eq!(action_index, 2);
            assert_eq!(requested_read_request, 3);
            assert_eq!(available_read_requests, 2);
        }
        other => panic!("unexpected render error: {other}"),
    }
    assert!(text.contains("trace action 2"));
    assert!(text.contains("r3"));
}

#[test]
fn randomized_raft_soak_fast_profile_is_deterministic() {
    let config = SoakConfig::new(SimSeed(0x9095), 96)
        .with_max_proposals(8)
        .with_max_restarts(4);
    let first = run_raft_random_soak(three_node_configs(), config)
        .expect("deterministic random soak should preserve Raft invariants");
    let second = run_raft_random_soak(three_node_configs(), config)
        .expect("same seed should preserve Raft invariants again");

    assert_eq!(first, second);
    assert_eq!(first.seed(), SimSeed(0x9095));
    assert_eq!(first.steps_executed(), 96);
    for kind in [
        SoakActionKind::Tick,
        SoakActionKind::Propose,
        SoakActionKind::Deliver,
        SoakActionKind::Delay,
        SoakActionKind::Drop,
        SoakActionKind::Duplicate,
        SoakActionKind::Restart,
    ] {
        assert!(
            first.observed_actions().contains(&kind),
            "fast soak should observe {kind:?}"
        );
    }
}

#[test]
fn randomized_soak_liveness_phase_elects_leader_and_commits_probe() {
    let summary = run_raft_random_soak(three_node_configs(), SoakConfig::new(SimSeed(0x11_5e), 0))
        .expect("post-soak liveness phase should elect and commit without random steps");

    assert_eq!(summary.steps_executed(), 0);
    for kind in [
        SoakActionKind::Tick,
        SoakActionKind::Deliver,
        SoakActionKind::Propose,
    ] {
        assert!(
            summary.observed_actions().contains(&kind),
            "liveness phase should observe {kind:?}"
        );
    }
}

#[test]
fn randomized_lease_soak_exercises_read_fault_and_timing_actions() {
    let config = SoakConfig::new(SimSeed(0x6c35_ea5e), 320)
        .with_max_proposals(24)
        .with_max_restarts(12)
        .with_max_read_indexes(4)
        .with_max_membership_changes(8)
        .with_max_transfers(2)
        .with_max_partitions(2)
        .with_max_lossy_restarts(2)
        .with_tick_skew(NodeId(1), 3);
    let summary = run_raft_random_soak(three_node_lease_configs(), config)
        .expect("lease-enabled production soak should preserve Raft invariants");

    for kind in [
        SoakActionKind::Tick,
        SoakActionKind::Restart,
        SoakActionKind::ReadIndex,
        SoakActionKind::Partition,
    ] {
        assert!(
            summary.observed_actions().contains(&kind),
            "lease soak should observe {kind:?}"
        );
    }
}

#[test]
fn randomized_membership_soak_exercises_dynamic_membership_actions() {
    let config = SoakConfig::new(SimSeed(0x6c35_ea5e), 320)
        .with_max_proposals(8)
        .with_max_restarts(4)
        .with_max_read_indexes(4)
        .with_max_membership_changes(8)
        .with_max_partitions(4)
        .with_tick_skew(NodeId(1), 3);
    let summary = run_raft_random_soak(four_node_future_learner_configs(), config)
        .expect("membership soak should preserve Raft invariants");

    for kind in [
        SoakActionKind::AddLearner,
        SoakActionKind::RemoveLearner,
        SoakActionKind::PromoteLearner,
        SoakActionKind::RemoveVoter,
        SoakActionKind::EnterJoint,
    ] {
        assert!(
            summary.observed_actions().contains(&kind),
            "membership soak should observe {kind:?}"
        );
    }
}

#[test]
fn enabled_membership_soak_actions_cover_joint_transition_phases() {
    let mut cluster = Cluster::new(four_node_future_learner_configs());
    elect_node_one(&mut cluster);
    let base_state = ExplorationState::new(cluster.clone());
    let base_kinds = enabled_soak_kinds(&base_state);
    for kind in [
        SoakActionKind::AddLearner,
        SoakActionKind::RemoveVoter,
        SoakActionKind::EnterJoint,
    ] {
        assert!(
            base_kinds.contains(&kind),
            "base membership state should enable {kind:?}"
        );
    }

    cluster.add_learner(NodeId(1), NodeId(4));
    cluster.deliver_all();
    let learner_state = ExplorationState::new(cluster.clone());
    let learner_kinds = enabled_soak_kinds(&learner_state);
    for kind in [
        SoakActionKind::RemoveLearner,
        SoakActionKind::PromoteLearner,
    ] {
        assert!(
            learner_kinds.contains(&kind),
            "learner membership state should enable {kind:?}"
        );
    }

    let promotion_barrier = cluster
        .promotion_barrier(NodeId(1), NodeId(4))
        .expect("caught-up learner should have a promotion barrier");
    cluster.promote_learner(NodeId(1), NodeId(4), promotion_barrier);
    cluster.deliver_all();
    let joint_state = ExplorationState::new(cluster);
    let joint_kinds = enabled_soak_kinds(&joint_state);
    assert!(
        joint_kinds.contains(&SoakActionKind::LeaveJoint),
        "joint membership state should enable leave-joint"
    );
}

fn enabled_soak_kinds(state: &ExplorationState) -> BTreeSet<SoakActionKind> {
    enabled_soak_actions(
        state,
        SoakConfig::new(SimSeed(0xfeed), 1).with_max_membership_changes(1),
    )
    .into_iter()
    .map(|action| action.trace.kind())
    .collect()
}

#[test]
fn bounded_raft_read_index_safety_passes_for_three_node_cluster() {
    let summary = check_raft_read_index_safety(
        vec![
            config(1, &[2, 3], 2),
            config(2, &[1, 3], 2),
            config(3, &[1, 2], 2),
        ],
        Bounds::new(5)
            .with_max_proposals(1)
            .with_max_read_indexes(1),
    )
    .expect("bounded read-index exploration finds no violation");
    assert!(summary.explored_states() > 1_000);
    assert!(summary.unique_states() > 100);
    assert!(summary.unique_states() <= summary.explored_states());
}

#[test]
fn bounded_raft_lease_read_safety_passes_for_production_cluster() {
    let summary = check_raft_read_index_safety(
        three_node_lease_configs(),
        Bounds::new(5)
            .with_max_proposals(1)
            .with_max_read_indexes(2),
    )
    .expect("bounded lease-read exploration finds no violation");
    assert!(summary.explored_states() > 100);
    assert!(summary.unique_states() > 100);
    assert!(summary.unique_states() <= summary.explored_states());
}
