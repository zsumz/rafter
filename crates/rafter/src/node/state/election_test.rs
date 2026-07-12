//! Explicit boundaries between pre-vote, election, leadership, and follower state.

use super::election::ElectionState;
use crate::NodeId;

#[test]
fn election_state_names_round_boundaries_explicitly() {
    let mut state = ElectionState::default();
    state.record_vote(NodeId(2));

    state.begin_pre_vote(NodeId(1));
    state.record_pre_vote(NodeId(3));

    assert_eq!(state.votes().collect::<Vec<_>>(), vec![NodeId(2)]);
    assert_eq!(
        state.pre_votes().collect::<Vec<_>>(),
        vec![NodeId(1), NodeId(3)],
    );

    state.begin_election(NodeId(1));

    assert_eq!(state.votes().collect::<Vec<_>>(), vec![NodeId(1)]);
    assert!(state.pre_votes().next().is_none());

    state.enter_leadership();
    assert_eq!(state.elapsed(), 0);
    assert_eq!(state.votes().collect::<Vec<_>>(), vec![NodeId(1)]);

    state.reset_for_follower();
    assert!(state.votes().next().is_none());
    assert!(state.pre_votes().next().is_none());
}
