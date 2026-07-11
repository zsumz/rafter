use super::*;
use crate::model_check::{
    application::{apply_soak_action, apply_to_state, restart_node},
    scheduling::{Operation, SoakOperation},
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
        state.election_history.term_floor_by_node.get(&NodeId(1)),
        Some(&Term(4))
    );
    assert_eq!(
        state
            .election_history
            .votes_by_node_term
            .get(&(NodeId(1), Term(4))),
        Some(&NodeId(2))
    );
}

#[test]
fn term_monotonicity_history_detects_regression_from_observation() {
    let mut cluster = one_node_cluster();
    cluster
        .restart_node_from_bootstrap(NodeId(1), bootstrap_state(Term(4), &[]))
        .expect("seeded term bootstrap is valid");
    let mut state = ExplorationState::new(cluster);

    state
        .cluster
        .restart_node_from_bootstrap(NodeId(1), bootstrap_state(Term(3), &[]))
        .expect("regressed term bootstrap is valid");
    state.observe_election_authority();

    let failure =
        check_election_history(&state, &[]).expect_err("observed term regression must be detected");
    assert_eq!(failure.invariant(), catalog::EL_01_TERM_MONOTONICITY);
    assert!(
        failure
            .message
            .contains("node-1 term regressed from observed floor 4 to 3"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
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
        .cluster
        .restart_node_from_bootstrap(NodeId(1), second_vote)
        .expect("second vote bootstrap is valid");
    state.observe_election_authority();

    let failure = check_election_history(&state, &[])
        .expect_err("conflicting durable votes in one term must be detected");
    assert_eq!(
        failure.invariant(),
        catalog::EL_02_ONE_DURABLE_VOTE_PER_TERM
    );
    assert!(
        failure
            .message
            .contains("node-1 recorded conflicting durable votes in term 7: node-2 then node-3"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn durable_vote_history_detects_lost_vote_same_term() {
    let mut cluster = one_node_cluster();
    let mut first_vote = bootstrap_state(Term(5), &[]);
    first_vote.voted_for = Some(NodeId(2));
    cluster
        .restart_node_from_bootstrap(NodeId(1), first_vote)
        .expect("first vote bootstrap is valid");
    let mut state = ExplorationState::new(cluster);

    state
        .cluster
        .restart_node_from_bootstrap(NodeId(1), bootstrap_state(Term(5), &[]))
        .expect("lost vote bootstrap is valid");
    state.observe_election_authority();

    let failure =
        check_election_history(&state, &[]).expect_err("lost durable vote must be detected");
    assert_eq!(
        failure.invariant(),
        catalog::EL_02_ONE_DURABLE_VOTE_PER_TERM
    );
    assert!(
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

    check_election_history(&state, &[]).expect("ordinary restart must keep durable vote history");
    assert_eq!(
        state
            .election_history
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

    apply_soak_action(&mut state, SoakOperation::LossyRestart(NodeId(1)));

    check_election_history(&state, &[]).expect("lossy restart must keep durable vote history");
    assert_eq!(
        state
            .election_history
            .votes_by_node_term
            .get(&(NodeId(1), Term(6))),
        Some(&NodeId(2))
    );
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
        .election_history
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
    state.cluster.queue_message(
        NodeId(2),
        NodeId(1),
        Message::RequestVote(RequestVote {
            term: Term(1),
            candidate_id: NodeId(2),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term::default(),
        }),
    );
    state.cluster.blocked_pairs.insert((NodeId(1), NodeId(2)));

    apply_to_state(&mut state, Operation::DeliverReadyAt(0));

    assert!(
        state.cluster.pending().all(|envelope| !matches!(
            &envelope.message,
            Message::RequestVoteResponse(RequestVoteResponse {
                vote_granted: true,
                ..
            })
        )),
        "granted response should be dropped by the simulated partition"
    );
    let grant = state
        .election_history
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
        state.election_history.vote_grants.is_empty(),
        "denied RequestVote responses must not create grant observations"
    );
}

#[test]
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

    let failure = check_election_history(&state, &[])
        .expect_err("grant to candidate outside membership must be rejected");
    assert_eq!(failure.invariant(), catalog::EL_03_SAFE_VOTE_ELIGIBILITY);
    assert!(
        failure
            .message
            .contains("node-1 granted term 4 vote to non-voter node-4"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
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

    let failure = check_election_history(&state, &[])
        .expect_err("grant to stale candidate log must be rejected");
    assert_eq!(failure.invariant(), catalog::EL_03_SAFE_VOTE_ELIGIBILITY);
    assert!(
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

    let failure = check_election_history(&state, &[])
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

#[test]
fn election_history_detects_second_leader_in_same_term() {
    let membership = stable_membership(&[1, 2, 3], &[]);
    let first = election_certificate(4, 1, membership.clone(), &[1, 2]);
    let second = election_certificate(4, 2, membership, &[2, 3]);
    let mut state = ExplorationState::new(one_node_cluster());

    state.election_history.record_election(first);
    state.election_history.record_election(second);

    let failure = check_election_history(&state, &[])
        .expect_err("second leader in one term must be detected");
    assert_eq!(
        failure.invariant(),
        catalog::EL_05_ELECTION_SAFETY_OVER_HISTORY
    );
    assert!(
        failure
            .message
            .contains("term 4 elected both node-1 and node-2"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn election_certificate_rejects_learner_grant() {
    let certificate = election_certificate(2, 1, stable_membership(&[1, 2, 3], &[4]), &[1, 2, 4]);
    let state = state_with_recorded_certificate(certificate);

    let failure = check_election_history(&state, &[])
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

#[test]
fn election_certificate_requires_joint_quorum() {
    let certificate = election_certificate(3, 1, joint_membership(&[1, 2, 3], &[1, 4, 5]), &[1, 2]);
    let state = state_with_recorded_certificate(certificate);

    let failure = check_election_history(&state, &[])
        .expect_err("joint elections must satisfy both majorities");
    assert_eq!(
        failure.invariant(),
        catalog::EL_06_LEADER_HAS_VALID_ELECTION_QUORUM
    );
    assert!(
        failure.message.contains("lacks an effective quorum"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn election_certificate_rejects_non_voter_leader() {
    let certificate = election_certificate(5, 4, stable_membership(&[1, 2, 3], &[4]), &[1, 2, 4]);
    let state = state_with_recorded_certificate(certificate);

    let failure =
        check_election_history(&state, &[]).expect_err("non-voter leaders must be detected");
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

#[test]
fn election_history_deduplicates_duplicate_grants() {
    let mut state = ExplorationState::new(one_node_cluster());
    let before = state.cluster.clone();
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
            .election_history
            .grants_by_candidate
            .get(&(Term(7), NodeId(1)))
            .expect("grant history should be recorded")
            .len(),
        1
    );
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
