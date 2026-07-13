//! Campaign, vote, removal, and learner authority fencing.

use super::support::*;

#[test]
fn vote_request_from_candidate_outside_effective_membership_is_rejected() {
    let mut voter = node_with_configuration(
        2,
        &[1, 3, 4],
        stable_configuration(ConfigurationId(3), &[1, 2, 3]),
    );

    let outputs = voter.step(Input::Message {
        from: NodeId(4),
        message: Message::RequestVote(RequestVote {
            term: Term(2),
            candidate_id: NodeId(4),
            last_log_index: LogIndex(1),
            last_log_term: Term(1),
        }),
    });

    assert_vote_response(&outputs, NodeId(4), false);
    assert_eq!(voter.current_term(), Term(2));
    assert_eq!(voter.voted_for(), None);
}

#[test]
fn pre_vote_from_candidate_outside_effective_membership_is_rejected() {
    let mut voter = node_with_configuration(
        2,
        &[1, 3, 4],
        stable_configuration(ConfigurationId(3), &[1, 2, 3]),
    );

    let outputs = voter.step(Input::Message {
        from: NodeId(4),
        message: Message::PreVote(PreVote {
            term: Term(2),
            candidate_id: NodeId(4),
            last_log_index: LogIndex(1),
            last_log_term: Term(1),
        }),
    });

    assert_pre_vote_response(&outputs, NodeId(4), Term(1), false);
    assert_eq!(voter.current_term(), Term(1));
    assert_eq!(voter.voted_for(), None);
}

#[test]
fn non_voter_cannot_campaign_or_become_leader() {
    let mut non_voter = node_with_configuration(4, &[1, 2, 3], learner_configuration());

    assert!(!non_voter.is_effective_voter(NodeId(4)));
    assert!(non_voter.is_effective_voter(NodeId(1)));
    assert!(non_voter.is_effective_voter(NodeId(2)));

    assert!(non_voter.step(Input::Tick).is_empty());
    assert!(non_voter.step(Input::Tick).is_empty());
    assert!(non_voter.step(Input::Tick).is_empty());
    assert_eq!(non_voter.role(), Role::Follower);
    assert_eq!(non_voter.current_term(), Term(1));

    // Even a campaign already holding one effective voter grant is fenced
    // before the next grant could complete the two-of-three voter quorum.
    non_voter.volatile.role = Role::Candidate;
    non_voter.persistent.current_term = Term(2);
    non_voter.persistent.voted_for = Some(NodeId(4));
    non_voter.election.record_vote(NodeId(1));

    let outputs = grant_vote(&mut non_voter, NodeId(2));

    assert!(outputs.is_empty());
    assert_eq!(non_voter.role(), Role::Follower);
    assert_eq!(non_voter.current_term(), Term(2));
    assert_eq!(non_voter.last_log_index(), LogIndex(1));
}

#[test]
fn removed_candidate_steps_down_instead_of_winning() {
    let mut candidate = node_with_configuration(
        2,
        &[1, 3, 4],
        stable_configuration(ConfigurationId(4), &[1, 3, 4]),
    );
    candidate.volatile.role = Role::Candidate;
    candidate.persistent.current_term = Term(2);
    candidate.persistent.voted_for = Some(NodeId(2));
    candidate.election.record_vote(NodeId(2));

    let outputs = candidate.step(Input::Message {
        from: NodeId(1),
        message: Message::RequestVoteResponse(RequestVoteResponse {
            term: Term(2),
            voter_id: NodeId(1),
            vote_granted: true,
        }),
    });

    assert!(outputs.is_empty());
    assert_eq!(candidate.role(), Role::Follower);
    assert_eq!(candidate.voted_for(), Some(NodeId(2)));
}

#[test]
fn promoted_voter_grants_vote_only_after_local_membership_includes_it() {
    let request = RequestVote {
        term: Term(2),
        candidate_id: NodeId(4),
        last_log_index: LogIndex(1),
        last_log_term: Term(1),
    };

    let mut promoted_member = node_with_configuration(
        2,
        &[1, 3, 4],
        stable_configuration(ConfigurationId(3), &[1, 2, 3, 4]),
    );
    let outputs = promoted_member.step(Input::Message {
        from: NodeId(4),
        message: Message::RequestVote(request),
    });
    assert_vote_response(&outputs, NodeId(4), true);

    let mut stale_learner_view = node_with_configuration(3, &[1, 2, 4], learner_configuration());
    let outputs = stale_learner_view.step(Input::Message {
        from: NodeId(4),
        message: Message::RequestVote(request),
    });
    assert_vote_response(&outputs, NodeId(4), false);

    let mut candidate = node_with_configuration(
        4,
        &[1, 2, 3],
        stable_configuration(ConfigurationId(3), &[1, 2, 3, 4]),
    );
    assert!(candidate.step(Input::Tick).is_empty());
    assert!(candidate.step(Input::Tick).is_empty());
    let polls = candidate.step(Input::Tick);
    assert_eq!(candidate.role(), Role::PreCandidate);
    assert_eq!(send_targets(&polls), vec![NodeId(1), NodeId(2), NodeId(3)]);

    assert!(grant_pre_vote(&mut candidate, NodeId(2)).is_empty());
    let requests = grant_pre_vote(&mut candidate, NodeId(3));
    assert_eq!(candidate.role(), Role::Candidate);
    assert_eq!(
        send_targets(&requests),
        vec![NodeId(1), NodeId(2), NodeId(3)]
    );

    assert!(grant_vote(&mut candidate, NodeId(2)).is_empty());
    assert_eq!(candidate.role(), Role::Candidate);

    let heartbeats = grant_vote(&mut candidate, NodeId(3));

    assert_eq!(candidate.role(), Role::Leader);
    assert!(!heartbeats.is_empty());
}

#[test]
fn learner_grant_does_not_create_quorum() {
    let mut candidate = node_with_configuration(1, &[2, 3, 4], learner_configuration());

    assert!(candidate.step(Input::Tick).is_empty());
    assert!(candidate.step(Input::Tick).is_empty());
    let polls = candidate.step(Input::Tick);

    assert_eq!(candidate.role(), Role::PreCandidate);
    assert_eq!(send_targets(&polls), vec![NodeId(2), NodeId(3)]);

    // The learner's poll grant creates no pre-vote quorum either.
    assert!(grant_pre_vote(&mut candidate, NodeId(4)).is_empty());
    assert_eq!(candidate.role(), Role::PreCandidate);

    let requests = grant_pre_vote(&mut candidate, NodeId(2));
    assert_eq!(candidate.role(), Role::Candidate);
    assert_eq!(send_targets(&requests), vec![NodeId(2), NodeId(3)]);

    assert!(grant_vote(&mut candidate, NodeId(4)).is_empty());
    assert_eq!(candidate.role(), Role::Candidate);

    let heartbeats = grant_vote(&mut candidate, NodeId(2));

    assert_eq!(candidate.role(), Role::Leader);
    assert!(!heartbeats.is_empty());
}
