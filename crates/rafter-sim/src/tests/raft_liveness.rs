use super::helpers::{
    deliver_append_entries, deliver_append_entries_response, direct_election_three_node_cluster,
    elect_node_one, elect_node_two_without_reaching_node_one, pre_vote, pre_vote_response,
    request_vote, three_node_cluster,
};
use super::*;
use rafter::Message;

#[test]
fn election_progresses_after_temporary_vote_delay() {
    let mut cluster = three_node_cluster();

    for _ in 0..3 {
        cluster.tick(NodeId(1));
    }
    assert_eq!(cluster.deliver_matching(pre_vote(NodeId(1), NodeId(3))), 1);
    assert_eq!(
        cluster.deliver_matching(pre_vote_response(NodeId(3), NodeId(1))),
        1
    );
    assert_eq!(
        cluster.delay_matching(request_vote(NodeId(1), NodeId(2)), 2),
        1
    );
    assert_eq!(
        cluster.deliver_matching(request_vote(NodeId(1), NodeId(3))),
        1
    );
    assert_eq!(
        cluster.deliver_matching(vote_response(NodeId(3), NodeId(1))),
        1
    );

    assert_eq!(cluster.leaders_in_term(Term(1)), vec![NodeId(1)]);

    cluster.advance_clock();
    cluster.advance_clock();
    assert_eq!(
        cluster.deliver_matching(request_vote(NodeId(1), NodeId(2))),
        1
    );
}

#[test]
fn stable_leader_commits_after_quorum_delivery_and_lagging_follower_catches_up() {
    let mut cluster = three_node_cluster();
    elect_node_one(&mut cluster);

    cluster.propose(NodeId(1), b"progress".to_vec());
    assert_eq!(
        cluster.delay_matching(deliver_append_entries(NodeId(1), NodeId(2)), 2),
        1
    );
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries(NodeId(1), NodeId(3))),
        1
    );
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries_response(NodeId(3), NodeId(1))),
        1
    );

    assert_eq!(cluster.commit_index(NodeId(1)), LogIndex(2));
    assert_eq!(cluster.commit_index(NodeId(2)), LogIndex::ZERO);

    cluster.advance_clock();
    cluster.advance_clock();
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries(NodeId(1), NodeId(2))),
        1
    );

    cluster.tick(NodeId(1));
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries(NodeId(1), NodeId(2))),
        1
    );

    assert_eq!(cluster.commit_index(NodeId(2)), LogIndex(2));
}

#[test]
fn surviving_quorum_elects_replacement_and_commits_after_leader_loss() {
    let mut cluster = direct_election_three_node_cluster();
    elect_node_one(&mut cluster);

    elect_node_two_without_reaching_node_one(&mut cluster);

    cluster.propose(NodeId(2), b"post-failover".to_vec());
    for _ in 0..4 {
        let _ = cluster.deliver_matching(deliver_append_entries(NodeId(2), NodeId(3)));
        let _ = cluster.deliver_matching(deliver_append_entries_response(NodeId(3), NodeId(2)));
    }

    assert_eq!(cluster.role(NodeId(2)), Role::Leader);
    assert_eq!(cluster.commit_index(NodeId(2)), LogIndex(3));
    assert!(cluster.applied().iter().any(
        |applied| applied.node_id == NodeId(2) && applied.payload == b"post-failover".to_vec()
    ));
}

fn vote_response(from: NodeId, to: NodeId) -> impl FnMut(&Envelope) -> bool {
    move |envelope| {
        envelope.from == from
            && envelope.to == to
            && matches!(envelope.message, Message::RequestVoteResponse(_))
    }
}
