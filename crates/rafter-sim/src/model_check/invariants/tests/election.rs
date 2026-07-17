use super::super::election::{
    check_eligible_leader_certificates, check_higher_term_authority_fencing,
    check_joint_election_quorums, check_pre_vote_leader_stability,
    check_pre_vote_request_authority, check_stable_election_quorums,
    check_stale_authority_leadership, check_stale_authority_state,
    check_stale_pre_vote_response_authority, check_vote_candidate_eligibility,
    check_vote_candidate_log_freshness, check_vote_grant_durability,
};
use super::*;
use crate::model_check::{
    helpers::elect_node_one_in_state,
    observations::Observation,
    scheduling::{Operation, SoakOperation},
    state::{apply_to_state, restart_node, try_apply_soak_action},
};
use rafter_invariant_test::{
    oracle_assert, oracle_assert_eq, oracle_expect_err, oracle_invoke_recorder,
};

#[test]
fn authority_history_records_seeded_term_and_vote() {
    let mut cluster = one_node_cluster();
    let mut bootstrap = bootstrap_state(Term(4), &[]);
    bootstrap.voted_for = Some(NodeId(2));
    cluster
        .restart_node_from_bootstrap(NodeId(1), bootstrap)
        .expect("seeded vote bootstrap is valid");

    let state = ExplorationState::new(cluster);

    assert_eq!(
        state.election_history().term_floor_by_node.get(&NodeId(1)),
        Some(&Term(4))
    );
    assert_eq!(
        state
            .election_history()
            .votes_by_node_term
            .get(&(NodeId(1), Term(4))),
        Some(&NodeId(2))
    );
}

#[test]
fn pre_elected_constructor_state_is_coverage_not_reached() {
    let mut cluster = two_node_cluster();
    elect_node_one(&mut cluster);
    let state = ExplorationState::new(cluster);

    let failure = check_election_history(&state, &[])
        .expect_err("an unobserved seeded election cannot count as verified history");
    assert_eq!(
        failure.kind(),
        crate::model_check::FailureKind::CoverageNotReached
    );
    assert_eq!(
        failure.invariant(),
        catalog::EL_05_ELECTION_SAFETY_OVER_HISTORY
    );
    assert!(
        failure.message.contains("when exploration history began"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[::rafter_invariant_test::detector_test]
fn term_monotonicity_history_detects_regression_from_observation() {
    let mut cluster = one_node_cluster();
    cluster
        .restart_node_from_bootstrap(NodeId(1), bootstrap_state(Term(4), &[]))
        .expect("seeded term bootstrap is valid");
    let mut state = ExplorationState::new(cluster);

    state
        .inject_bootstrap_state(NodeId(1), bootstrap_state(Term(3), &[]))
        .expect("regressed term bootstrap is valid");
    oracle_invoke_recorder!(record_election_authority_observation(&mut state));

    let failure = oracle_expect_err!(
        check_election_history(&state, &[]),
        "observed term regression must be detected",
    );
    oracle_assert_eq!(failure.invariant(), catalog::EL_01_TERM_MONOTONICITY);
    oracle_assert!(
        failure
            .message
            .contains("node-1 term regressed from observed floor 4 to 3"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[::rafter_invariant_test::detector_test]
fn durable_vote_history_rejects_second_vote_in_term() {
    let mut cluster = one_node_cluster();
    let mut first_vote = bootstrap_state(Term(7), &[]);
    first_vote.voted_for = Some(NodeId(2));
    cluster
        .restart_node_from_bootstrap(NodeId(1), first_vote)
        .expect("first vote bootstrap is valid");
    let mut state = ExplorationState::new(cluster);

    let mut second_vote = bootstrap_state(Term(7), &[]);
    second_vote.voted_for = Some(NodeId(3));
    state
        .inject_bootstrap_state(NodeId(1), second_vote)
        .expect("second vote bootstrap is valid");
    oracle_invoke_recorder!(record_election_authority_observation(&mut state));

    let failure = oracle_expect_err!(
        check_election_history(&state, &[]),
        "conflicting durable votes in one term must be detected",
    );
    oracle_assert_eq!(
        failure.invariant(),
        catalog::EL_02_ONE_DURABLE_VOTE_PER_TERM
    );
    oracle_assert!(
        failure
            .message
            .contains("node-1 recorded conflicting durable votes in term 7: node-2 then node-3"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[::rafter_invariant_test::detector_test]
fn durable_vote_history_detects_lost_vote_same_term() {
    let mut cluster = one_node_cluster();
    let mut first_vote = bootstrap_state(Term(5), &[]);
    first_vote.voted_for = Some(NodeId(2));
    cluster
        .restart_node_from_bootstrap(NodeId(1), first_vote)
        .expect("first vote bootstrap is valid");
    let mut state = ExplorationState::new(cluster);

    state
        .inject_bootstrap_state(NodeId(1), bootstrap_state(Term(5), &[]))
        .expect("lost vote bootstrap is valid");
    oracle_invoke_recorder!(record_election_authority_observation(&mut state));

    let failure = oracle_expect_err!(
        check_election_history(&state, &[]),
        "lost durable vote must be detected",
    );
    oracle_assert_eq!(
        failure.invariant(),
        catalog::EL_02_ONE_DURABLE_VOTE_PER_TERM
    );
    oracle_assert!(
        failure
            .message
            .contains("node-1 lost durable vote for node-2 in term 5"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn modeled_restart_preserves_observed_durable_vote() {
    let mut cluster = one_node_cluster();
    let mut first_vote = bootstrap_state(Term(6), &[]);
    first_vote.voted_for = Some(NodeId(2));
    cluster
        .restart_node_from_bootstrap(NodeId(1), first_vote)
        .expect("seeded vote bootstrap is valid");
    let mut state = ExplorationState::new(cluster);

    restart_node(&mut state, NodeId(1), &[]).expect("ordinary restart should preserve vote");

    assert_eq!(state.restarts_issued(), 1);
    assert!(
        state
            .observation_set()
            .contains(Observation::RestartTermComparisons),
        "successful restart must record the explicit term comparison"
    );
    check_election_history(&state, &[]).expect("ordinary restart must keep durable vote history");
    assert_eq!(
        state
            .election_history_mut()
            .votes_by_node_term
            .get(&(NodeId(1), Term(6))),
        Some(&NodeId(2))
    );
}

#[test]
fn modeled_lossy_restart_preserves_observed_durable_vote() {
    let mut cluster = one_node_cluster();
    let mut first_vote = bootstrap_state(Term(6), &[]);
    first_vote.voted_for = Some(NodeId(2));
    cluster
        .restart_node_from_bootstrap(NodeId(1), first_vote)
        .expect("seeded vote bootstrap is valid");
    let mut state = ExplorationState::new(cluster);

    try_apply_soak_action(&mut state, SoakOperation::LossyRestart(NodeId(1)))
        .expect("fixture lossy restart must remain valid");

    check_election_history(&state, &[]).expect("lossy restart must keep durable vote history");
    assert_eq!(
        state
            .election_history_mut()
            .votes_by_node_term
            .get(&(NodeId(1), Term(6))),
        Some(&NodeId(2))
    );
}

#[test]
fn authority_fencing_observation_accepts_higher_term_append_entries() {
    let mut state = ExplorationState::new(two_node_cluster());
    state.inject_message(
        NodeId(2),
        NodeId(1),
        Message::AppendEntries(AppendEntries {
            term: Term(3),
            leader_id: NodeId(2),
            prev_log_index: LogIndex::ZERO,
            prev_log_term: Term::default(),
            sequence: 7,
            entries: SharedEntries::default(),
            leader_commit: LogIndex::ZERO,
        }),
    );

    apply_to_state(&mut state, Operation::DeliverReadyAt(0));

    assert_eq!(state.cluster().current_term(NodeId(1)), Term(3));
    assert_eq!(state.cluster().role(NodeId(1)), rafter::Role::Follower);
    check_election_history(&state, &[]).expect("higher-term append should fence cleanly");
}

#[test]
fn instrumented_delivery_observes_higher_term_append_entries_response() {
    let mut state = ExplorationState::new(two_node_cluster());
    elect_node_one_in_state(&mut state);
    state.drop_all_messages();
    let higher_term = state.cluster().current_term(NodeId(1)).next();
    state.inject_message(
        NodeId(2),
        NodeId(1),
        Message::AppendEntriesResponse(AppendEntriesResponse {
            term: higher_term,
            follower_id: NodeId(2),
            success: false,
            match_index: LogIndex::ZERO,
            sequence: 91,
        }),
    );

    apply_to_state(&mut state, Operation::DeliverReadyAt(0));

    assert_eq!(state.cluster().current_term(NodeId(1)), higher_term);
    assert_eq!(state.cluster().role(NodeId(1)), rafter::Role::Follower);
    assert_eq!(
        state
            .election_history_mut()
            .term_floor_by_node
            .get(&NodeId(1)),
        Some(&higher_term),
        "the instrumented boundary must observe response-driven authority changes"
    );
    check_election_history(&state, &[]).expect("higher-term response should fence cleanly");
}

#[::rafter_invariant_test::detector_test]
fn authority_fencing_oracle_rejects_unfenced_higher_term_response() {
    let mut before = two_node_cluster();
    elect_node_one(&mut before);
    let mut state = ExplorationState::new(before.clone());
    let delivered = Envelope {
        from: NodeId(2),
        to: NodeId(1),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term: before.current_term(NodeId(1)).next(),
            follower_id: NodeId(2),
            success: false,
            match_index: LogIndex::ZERO,
            sequence: 11,
        }),
    };

    state.record_election_observation(&before, Some(&delivered), &[]);

    let failure = oracle_expect_err!(
        check_higher_term_authority_fencing(&state, &[]),
        "higher-term authority must fence a leader",
    );
    assert_eq!(before.role(NodeId(1)), rafter::Role::Leader);
    oracle_assert_eq!(
        failure.invariant(),
        catalog::EL_07_TERM_AND_AUTHORITY_FENCING
    );
    oracle_assert!(
        failure
            .message
            .contains("did not fence higher-term authority"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[::rafter_invariant_test::detector_test]
fn authority_fencing_oracle_rejects_stale_response_leadership() {
    let mut before = two_node_cluster();
    before
        .restart_node_from_bootstrap(NodeId(1), bootstrap_state(Term(3), &[]))
        .expect("before bootstrap is valid");
    let mut after = before.clone();
    elect_node_one(&mut after);
    let mut state = ExplorationState::new(after);
    let delivered = Envelope {
        from: NodeId(2),
        to: NodeId(1),
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term: Term(2),
            voter_id: NodeId(2),
            vote_granted: true,
        }),
    };

    state.record_election_observation(&before, Some(&delivered), &[]);

    let failure = oracle_expect_err!(
        check_stale_authority_leadership(&state, &[]),
        "stale-term traffic must not create leadership",
    );
    assert_eq!(before.role(NodeId(1)), rafter::Role::Follower);
    assert_eq!(state.cluster().role(NodeId(1)), rafter::Role::Leader);
    assert!(state.cluster().current_term(NodeId(1)) > before.current_term(NodeId(1)));
    oracle_assert_eq!(
        failure.invariant(),
        catalog::EL_07_TERM_AND_AUTHORITY_FENCING
    );
    oracle_assert!(
        failure
            .message
            .contains("let stale-term traffic create leadership"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[::rafter_invariant_test::detector_test]
fn authority_fencing_oracle_rejects_stale_authority_regression() {
    let mut before = one_node_cluster();
    let mut authority = bootstrap_state(Term(3), &[]);
    authority.voted_for = Some(NodeId(1));
    before
        .restart_node_from_bootstrap(NodeId(1), authority)
        .expect("before bootstrap is valid");
    let mut after = before.clone();
    let mut regressed = bootstrap_state(Term(2), &[]);
    regressed.voted_for = Some(NodeId(1));
    after
        .restart_node_from_bootstrap(NodeId(1), regressed)
        .expect("regressed bootstrap is structurally valid");
    let mut state = ExplorationState::new(after);
    let delivered = Envelope {
        from: NodeId(2),
        to: NodeId(1),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            term: Term(2),
            follower_id: NodeId(2),
            success: false,
            match_index: LogIndex::ZERO,
            sequence: 12,
        }),
    };

    state.record_election_observation(&before, Some(&delivered), &[]);

    let failure = oracle_expect_err!(
        check_stale_authority_state(&state, &[]),
        "stale-term traffic must not regress durable authority",
    );
    oracle_assert_eq!(
        failure.invariant(),
        catalog::EL_07_TERM_AND_AUTHORITY_FENCING
    );
    oracle_assert!(
        failure
            .message
            .contains("let stale-term traffic lower durable authority"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn pre_vote_observation_accepts_non_binding_request() {
    let before = one_node_cluster();
    let mut state = ExplorationState::new(before.clone());
    let delivered = Envelope {
        from: NodeId(2),
        to: NodeId(1),
        message: Message::PreVote(PreVote {
            term: Term(1),
            candidate_id: NodeId(2),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        }),
    };
    let emitted = [Envelope {
        from: NodeId(1),
        to: NodeId(2),
        message: Message::PreVoteResponse(PreVoteResponse {
            term: Term(1),
            voter_id: NodeId(1),
            vote_granted: true,
        }),
    }];

    state.record_election_observation(&before, Some(&delivered), &emitted);

    assert!(
        state.election_history_mut().pre_vote_violations.is_empty(),
        "non-binding pre-vote request must not produce a recorder violation"
    );
    check_election_history(&state, &[]).expect("non-binding pre-vote request should pass");
}

#[::rafter_invariant_test::detector_test]
fn pre_vote_oracle_rejects_request_term_mutation() {
    let before = one_node_cluster();
    let mut after = one_node_cluster();
    after
        .restart_node_from_bootstrap(NodeId(1), bootstrap_state(Term(1), &[]))
        .expect("after bootstrap is valid");
    let mut state = ExplorationState::new(after);
    let delivered = Envelope {
        from: NodeId(2),
        to: NodeId(1),
        message: Message::PreVote(PreVote {
            term: Term(1),
            candidate_id: NodeId(2),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        }),
    };

    state.record_election_observation(&before, Some(&delivered), &[]);

    let failure = oracle_expect_err!(
        check_pre_vote_request_authority(&state, &[]),
        "pre-vote request must not mutate durable authority",
    );
    oracle_assert_eq!(failure.invariant(), catalog::EL_08_PRE_VOTE_NON_BINDING);
    oracle_assert!(
        failure
            .message
            .contains("pre-vote request mutated authority"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[::rafter_invariant_test::detector_test]
fn pre_vote_oracle_rejects_request_disrupting_leader() {
    let mut before = two_node_cluster();
    elect_node_one(&mut before);
    let mut after = before.clone();
    after
        .restart_node_from_bootstrap(NodeId(1), before.bootstrap_state(NodeId(1)))
        .expect("same-authority follower bootstrap is valid");
    let mut state = ExplorationState::new(after);
    let delivered = Envelope {
        from: NodeId(2),
        to: NodeId(1),
        message: Message::PreVote(PreVote {
            term: before.current_term(NodeId(1)).next(),
            candidate_id: NodeId(2),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        }),
    };

    state.record_election_observation(&before, Some(&delivered), &[]);

    let failure = oracle_expect_err!(
        check_pre_vote_leader_stability(&state, &[]),
        "pre-vote request must not disrupt an established leader",
    );
    assert_eq!(before.role(NodeId(1)), rafter::Role::Leader);
    assert_eq!(state.cluster().role(NodeId(1)), rafter::Role::Follower);
    oracle_assert_eq!(failure.invariant(), catalog::EL_08_PRE_VOTE_NON_BINDING);
    oracle_assert!(
        failure
            .message
            .contains("pre-vote request disrupted a leader"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[::rafter_invariant_test::detector_test]
fn pre_vote_oracle_rejects_stale_response_authority_advance() {
    let mut before = one_node_cluster();
    before
        .restart_node_from_bootstrap(NodeId(1), bootstrap_state(Term(3), &[]))
        .expect("before bootstrap is valid");
    let mut after = one_node_cluster();
    after
        .restart_node_from_bootstrap(NodeId(1), bootstrap_state(Term(4), &[]))
        .expect("after bootstrap is valid");
    let mut state = ExplorationState::new(after);
    let delivered = Envelope {
        from: NodeId(2),
        to: NodeId(1),
        message: Message::PreVoteResponse(PreVoteResponse {
            term: Term(3),
            voter_id: NodeId(2),
            vote_granted: true,
        }),
    };

    state.record_election_observation(&before, Some(&delivered), &[]);

    let failure = oracle_expect_err!(
        check_stale_pre_vote_response_authority(&state, &[]),
        "stale pre-vote response must not advance authority",
    );
    oracle_assert_eq!(failure.invariant(), catalog::EL_08_PRE_VOTE_NON_BINDING);
    oracle_assert!(
        failure
            .message
            .contains("stale pre-vote response advanced authority"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn pre_vote_partition_heal_does_not_disrupt_leader_in_model_check() {
    let mut state = ExplorationState::new(pre_vote_three_node_cluster());

    for _ in 0..3 {
        apply_to_state(&mut state, Operation::Tick(NodeId(1)));
    }
    deliver_all_pending_in_state(&mut state);
    assert_eq!(state.cluster().leaders(), vec![NodeId(1)]);

    for _ in 0..18 {
        apply_to_state(&mut state, Operation::Tick(NodeId(3)));
    }
    assert_eq!(state.cluster().role(NodeId(3)), rafter::Role::PreCandidate);
    assert_eq!(state.cluster().current_term(NodeId(3)), Term(1));

    let delivered = deliver_pending_matching_in_state(&mut state, |envelope| {
        envelope.from == NodeId(3) && matches!(envelope.message, Message::PreVote(_))
    });
    assert!(
        delivered > 0,
        "partition-heal scenario should deliver stale pre-votes"
    );
    deliver_all_pending_in_state(&mut state);

    assert_eq!(state.cluster().role(NodeId(3)), rafter::Role::PreCandidate);
    assert_eq!(state.cluster().leaders(), vec![NodeId(1)]);

    apply_to_state(&mut state, Operation::Tick(NodeId(1)));
    deliver_all_pending_in_state(&mut state);

    assert_eq!(state.cluster().leaders(), vec![NodeId(1)]);
    assert_eq!(state.cluster().role(NodeId(3)), rafter::Role::Follower);
    for node_id in [1, 2, 3] {
        assert_eq!(state.cluster().current_term(NodeId(node_id)), Term(1));
    }
    assert!(
        state.election_history_mut().pre_vote_violations.is_empty(),
        "legitimate partition-heal pre-vote traffic should stay non-disruptive"
    );
    check_election_history(&state, &[]).expect("pre-vote partition heal should pass EL-08");
}

#[test]
fn vote_grant_observation_accepts_eligible_request() {
    let state = request_vote_grant_state(
        NodeId(2),
        &[(1, Term(2), b"voter-entry")],
        RequestVote {
            term: Term(4),
            candidate_id: NodeId(2),
            last_log_index: LogIndex(1),
            last_log_term: Term(2),
        },
        Some(NodeId(2)),
    );

    check_election_history(&state, &[]).expect("eligible vote grant should pass");
    let grant = state
        .election_history()
        .vote_grants
        .last()
        .expect("vote grant observation should be recorded");
    assert_eq!(grant.candidate_id, NodeId(2));
    assert_eq!(grant.voter_id, NodeId(1));
    assert_eq!(grant.term, Term(4));
    assert_eq!(grant.candidate_last_log_index, LogIndex(1));
    assert_eq!(grant.candidate_last_log_term, Term(2));
    assert_eq!(grant.voter_last_log_index, LogIndex(1));
    assert_eq!(grant.voter_last_log_term, Term(2));
    assert!(grant.voter_membership.contains_voter(NodeId(2)));
    assert_eq!(grant.durable_vote, Some(NodeId(2)));
}

#[test]
fn vote_grant_observation_records_partition_dropped_response() {
    let mut state = ExplorationState::new(two_node_cluster());
    state.inject_message(
        NodeId(2),
        NodeId(1),
        Message::RequestVote(RequestVote {
            term: Term(1),
            candidate_id: NodeId(2),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        }),
    );
    state.inject_blocked_pair(NodeId(1), NodeId(2));

    apply_to_state(&mut state, Operation::DeliverReadyAt(0));

    assert!(
        state.cluster().pending().all(|envelope| !matches!(
            &envelope.message,
            Message::RequestVoteResponse(RequestVoteResponse {
                vote_granted: true,
                ..
            })
        )),
        "granted response should be dropped by the simulated partition"
    );
    let grant = state
        .election_history_mut()
        .vote_grants
        .last()
        .expect("dropped granted response should still be observed");
    assert_eq!(grant.voter_id, NodeId(1));
    assert_eq!(grant.candidate_id, NodeId(2));
    assert_eq!(grant.term, Term(1));
    assert_eq!(grant.durable_vote, Some(NodeId(2)));
    check_election_history(&state, &[]).expect("eligible dropped-response grant should pass");
}

#[test]
fn vote_grant_observation_ignores_denied_response() {
    let before = one_node_cluster();
    let mut state = ExplorationState::new(before.clone());
    let delivered = Envelope {
        from: NodeId(2),
        to: NodeId(1),
        message: Message::RequestVote(RequestVote {
            term: Term(4),
            candidate_id: NodeId(2),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        }),
    };
    let emitted = [Envelope {
        from: NodeId(1),
        to: NodeId(2),
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term: Term(4),
            voter_id: NodeId(1),
            vote_granted: false,
        }),
    }];

    state.record_election_observation(&before, Some(&delivered), &emitted);

    assert!(
        state.election_history_mut().vote_grants.is_empty(),
        "denied RequestVote responses must not create grant observations"
    );
}

#[::rafter_invariant_test::detector_test]
fn vote_grant_oracle_rejects_non_voter_candidate() {
    let state = request_vote_grant_state(
        NodeId(4),
        &[],
        RequestVote {
            term: Term(4),
            candidate_id: NodeId(4),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        },
        Some(NodeId(4)),
    );

    let failure = oracle_expect_err!(
        check_vote_candidate_eligibility(&state, &[]),
        "grant to candidate outside membership must be rejected",
    );
    oracle_assert_eq!(failure.invariant(), catalog::EL_03_SAFE_VOTE_ELIGIBILITY);
    oracle_assert!(
        failure
            .message
            .contains("node-1 granted term 4 vote to non-voter node-4"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[::rafter_invariant_test::detector_test]
fn vote_grant_oracle_rejects_stale_candidate_log() {
    let state = request_vote_grant_state(
        NodeId(2),
        &[(1, Term(2), b"voter-entry")],
        RequestVote {
            term: Term(4),
            candidate_id: NodeId(2),
            last_log_index: LogIndex(1),
            last_log_term: Term(1),
        },
        Some(NodeId(2)),
    );

    let failure = oracle_expect_err!(
        check_vote_candidate_log_freshness(&state, &[]),
        "grant to stale candidate log must be rejected",
    );
    oracle_assert_eq!(failure.invariant(), catalog::EL_03_SAFE_VOTE_ELIGIBILITY);
    oracle_assert!(
        failure
            .message
            .contains("with stale candidate log (1, 1) below voter log (1, 2)"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn vote_grant_oracle_requires_durable_vote_for_candidate() {
    let state = request_vote_grant_state(
        NodeId(2),
        &[],
        RequestVote {
            term: Term(4),
            candidate_id: NodeId(2),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        },
        None,
    );

    let failure = check_vote_grant_durability(&state, &[])
        .expect_err("grant response must leave a durable vote for the candidate");
    assert_eq!(
        failure.invariant(),
        catalog::EL_02_ONE_DURABLE_VOTE_PER_TERM
    );
    assert!(
        failure
            .message
            .contains("node-1 granted term 4 vote to node-2 but durable vote is None"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[::rafter_invariant_test::detector_test]
fn election_history_detects_second_leader_in_same_term() {
    let membership = stable_membership(&[1, 2, 3], &[]);
    let first = election_certificate(4, 1, membership.clone(), &[1, 2]);
    let second = election_certificate(4, 2, membership, &[2, 3]);
    let mut state = ExplorationState::new(one_node_cluster());

    oracle_invoke_recorder!(record_election_certificate(&mut state, first));
    oracle_invoke_recorder!(record_election_certificate(&mut state, second));

    let failure = oracle_expect_err!(
        check_election_history(&state, &[]),
        "second leader in one term must be detected",
    );
    oracle_assert_eq!(
        failure.invariant(),
        catalog::EL_05_ELECTION_SAFETY_OVER_HISTORY
    );
    oracle_assert!(
        failure
            .message
            .contains("term 4 elected both node-1 and node-2"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[::rafter_invariant_test::detector_test]
fn election_history_preserves_same_leader_certificates_for_validation() {
    let first = election_certificate(4, 1, stable_membership(&[1, 2, 3], &[]), &[1, 2]);
    let second = election_certificate(4, 1, stable_membership(&[2, 3, 4], &[1]), &[2, 3]);
    let mut state = ExplorationState::new(one_node_cluster());

    oracle_invoke_recorder!(record_election_certificate(&mut state, first));
    oracle_invoke_recorder!(record_election_certificate(&mut state, second));

    let failure = oracle_expect_err!(
        check_eligible_leader_certificates(&state, &[]),
        "every same-term certificate must be validated",
    );
    assert_eq!(state.election_history().elected_by_term[&Term(4)].len(), 2);
    oracle_assert_eq!(
        failure.invariant(),
        catalog::EL_06_LEADER_HAS_VALID_ELECTION_QUORUM
    );
    oracle_assert!(
        failure
            .message
            .contains("outside the effective voting membership"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn election_certificate_rejects_learner_grant() {
    let certificate = election_certificate(2, 1, stable_membership(&[1, 2, 3], &[4]), &[1, 2, 4]);
    let state = state_with_recorded_certificate(certificate);

    let failure = check_election_certificate_voters(&state, &[])
        .expect_err("learner grants must not appear in an election certificate");
    assert_eq!(
        failure.invariant(),
        catalog::EL_06_LEADER_HAS_VALID_ELECTION_QUORUM
    );
    assert!(
        failure.message.contains("includes non-voter grant node-4"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[::rafter_invariant_test::detector_test]
fn election_certificate_requires_joint_quorum() {
    let certificate = election_certificate(3, 1, joint_membership(&[1, 2, 3], &[1, 4, 5]), &[1, 2]);
    let state = state_with_recorded_certificate(certificate);

    let failure = oracle_expect_err!(
        check_joint_election_quorums(&state, &[]),
        "joint elections must satisfy both majorities",
    );
    oracle_assert_eq!(
        failure.invariant(),
        catalog::EL_06_LEADER_HAS_VALID_ELECTION_QUORUM
    );
    oracle_assert!(
        failure.message.contains("lacks an effective quorum"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn election_certificate_rejects_non_voter_leader() {
    let certificate = election_certificate(5, 4, stable_membership(&[1, 2, 3], &[4]), &[1, 2, 4]);
    let state = state_with_recorded_certificate(certificate);

    let failure = check_eligible_leader_certificates(&state, &[])
        .expect_err("non-voter leaders must be detected");
    assert_eq!(
        failure.invariant(),
        catalog::EL_06_LEADER_HAS_VALID_ELECTION_QUORUM
    );
    assert!(
        failure
            .message
            .contains("outside the effective voting membership"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[::rafter_invariant_test::detector_test]
fn election_certificate_requires_stable_quorum() {
    let certificate = election_certificate(6, 1, stable_membership(&[1, 2, 3], &[]), &[1]);
    let state = state_with_recorded_certificate(certificate);

    let failure = oracle_expect_err!(
        check_stable_election_quorums(&state, &[]),
        "stable elections must satisfy the stable majority",
    );
    oracle_assert_eq!(
        failure.invariant(),
        catalog::EL_06_LEADER_HAS_VALID_ELECTION_QUORUM
    );
    oracle_assert!(
        failure.message.contains("lacks an effective quorum"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn election_history_deduplicates_duplicate_grants() {
    let mut state = ExplorationState::new(one_node_cluster());
    let before = state.cluster().clone();
    let envelope = Envelope {
        from: NodeId(2),
        to: NodeId(1),
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term: Term(7),
            voter_id: NodeId(2),
            vote_granted: true,
        }),
    };

    state.record_election_observation(&before, Some(&envelope), &[]);
    state.record_election_observation(&before, Some(&envelope), &[]);

    assert_eq!(
        state
            .election_history_mut()
            .grants_by_candidate
            .get(&(Term(7), NodeId(1)))
            .expect("grant history should be recorded")
            .len(),
        1
    );
}

#[test]
fn every_operation_class_records_election_transition_context() {
    let operations = [
        Operation::Propose {
            to: NodeId(1),
            proposal_id: crate::model_check::ProposalId(31),
            stale_leader: true,
        },
        Operation::ReadIndex {
            to: NodeId(1),
            request_id: 31,
        },
        Operation::AddLearner {
            to: NodeId(1),
            learner_id: NodeId(4),
        },
        Operation::Transfer {
            from: NodeId(1),
            target: NodeId(2),
        },
    ];
    for operation in operations {
        let mut state = ExplorationState::new(one_node_cluster());
        assert_eq!(state.election_transition_contexts_observed(), 0);
        apply_to_state(&mut state, operation);
        assert_eq!(state.election_transition_contexts_observed(), 1);
        check_election_history(&state, &[])
            .expect("non-election operation must preserve valid election authority");
    }

    for application_loss in [false, true] {
        let mut state = ExplorationState::new(one_node_cluster());
        let result = if application_loss {
            crate::model_check::state::restart_node_losing_application_state(
                &mut state,
                NodeId(1),
                &[],
            )
        } else {
            crate::model_check::state::restart_node(&mut state, NodeId(1), &[])
        };
        result.expect("fixture restart transition must remain valid");
        assert_eq!(state.election_transition_contexts_observed(), 1);
        check_election_history(&state, &[])
            .expect("restart operation must preserve valid election authority");
    }
}

fn request_vote_grant_state(
    candidate_id: NodeId,
    voter_entries: &[(u64, Term, &[u8])],
    request: RequestVote,
    durable_vote: Option<NodeId>,
) -> ExplorationState {
    let mut before = one_node_cluster();
    before
        .restart_node_from_bootstrap(NodeId(1), bootstrap_state(Term(3), voter_entries))
        .expect("before voter bootstrap is valid");

    let mut after_bootstrap = bootstrap_state(request.term, voter_entries);
    after_bootstrap.voted_for = durable_vote;
    let mut after = one_node_cluster();
    after
        .restart_node_from_bootstrap(NodeId(1), after_bootstrap)
        .expect("after voter bootstrap is valid");

    let mut state = ExplorationState::new(after);
    let delivered = Envelope {
        from: candidate_id,
        to: NodeId(1),
        message: Message::RequestVote(request),
    };
    let emitted = [Envelope {
        from: NodeId(1),
        to: candidate_id,
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term: request.term,
            voter_id: NodeId(1),
            vote_granted: true,
        }),
    }];
    state.record_election_observation(&before, Some(&delivered), &emitted);
    state
}

fn pre_vote_three_node_cluster() -> Cluster {
    Cluster::new(vec![
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3).expect("node-1 config is valid"),
        NodeConfig::new(NodeId(2), vec![NodeId(1), NodeId(3)], 9).expect("node-2 config is valid"),
        NodeConfig::new(NodeId(3), vec![NodeId(1), NodeId(2)], 9).expect("node-3 config is valid"),
    ])
}

fn record_election_authority_observation(state: &mut ExplorationState) {
    state.observe_election_authority();
}

fn record_election_certificate(state: &mut ExplorationState, certificate: ElectionCertificate) {
    state.election_history_mut().record_election(certificate);
}

fn deliver_all_pending_in_state(state: &mut ExplorationState) {
    while state.cluster().pending().next().is_some() {
        apply_to_state(state, Operation::DeliverReadyAt(0));
    }
}

fn deliver_pending_matching_in_state(
    state: &mut ExplorationState,
    mut predicate: impl FnMut(&Envelope) -> bool,
) -> usize {
    let mut delivered = 0;
    loop {
        let position = state
            .cluster()
            .pending()
            .enumerate()
            .find_map(|(position, envelope)| predicate(envelope).then_some(position));
        let Some(position) = position else {
            break;
        };
        apply_to_state(state, Operation::DeliverReadyAt(position));
        delivered += 1;
    }
    delivered
}
