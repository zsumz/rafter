use super::*;

#[test]
fn election_history_detects_second_leader_in_same_term() {
    let certificate = election_certificate(4, 1, stable_membership(&[1, 2, 3], &[]), &[1, 2]);
    let mut state = state_with_certificate(certificate);
    state
        .election_history
        .conflicting_elections
        .insert(ElectionConflict {
            term: Term(4),
            first_leader: NodeId(1),
            second_leader: NodeId(2),
        });

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
    let state = state_with_certificate(certificate);

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
    let state = state_with_certificate(certificate);

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
    let state = state_with_certificate(certificate);

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

    state.record_election_observation(&before, Some(&envelope));
    state.record_election_observation(&before, Some(&envelope));

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
