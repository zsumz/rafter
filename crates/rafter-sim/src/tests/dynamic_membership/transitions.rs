use super::super::helpers::{
    deliver_append_entries, deliver_append_entries_response, elect_node_one, request_vote,
};
use super::super::*;
use super::fixtures::{
    add_voter_joint_entry, applied_payloads, commit_add_voter_transition,
    commit_remove_node_two_transition, direct_election_learner_four_node_cluster,
    direct_election_stable_four_voter_cluster, elect_node_one_after_removal, elect_node_two,
    flush_commit_notifications, learner_four_node_cluster,
};
use rafter::ConfigurationId;

#[test]
fn add_voter_transition_preserves_committed_prefix() {
    let mut cluster = learner_four_node_cluster(SimSeed(0xadd0));
    elect_node_one(&mut cluster);

    cluster.propose(NodeId(1), b"before-add".to_vec());
    cluster.deliver_all();
    commit_add_voter_transition(&mut cluster, ConfigurationId(20));
    cluster.propose(NodeId(1), b"after-add".to_vec());
    cluster.deliver_all();
    flush_commit_notifications(&mut cluster, NodeId(1));

    for node_id in [NodeId(1), NodeId(2), NodeId(3), NodeId(4)] {
        assert_eq!(
            applied_payloads(&cluster, node_id),
            vec![b"before-add".to_vec(), b"after-add".to_vec()],
            "{node_id} should preserve the committed prefix through add-voter"
        );
        assert!(cluster
            .effective_membership(node_id)
            .contains_voter(NodeId(4)));
    }
}

#[test]
fn lossy_restart_preserves_committed_membership_state() {
    let mut cluster = learner_four_node_cluster(SimSeed(0x1055_1e55));
    elect_node_one(&mut cluster);

    cluster.propose(NodeId(1), b"before-lossy-restart".to_vec());
    cluster.deliver_all();
    commit_add_voter_transition(&mut cluster, ConfigurationId(70));

    let committed_prefix = cluster.log_entries_from(NodeId(1), LogIndex(1));
    let committed_index = cluster.commit_index(NodeId(1));
    let committed_configuration = cluster
        .committed_configuration_state(NodeId(1))
        .expect("committed configuration should be known after membership change");

    cluster.restart_node_lossy(NodeId(1));
    cluster.restart_node_lossy(NodeId(1));

    assert_eq!(cluster.commit_index(NodeId(1)), committed_index);
    assert_eq!(
        cluster.committed_configuration_state(NodeId(1)),
        Some(committed_configuration),
    );
    assert_eq!(
        cluster.log_entries_from(NodeId(1), LogIndex(1)),
        committed_prefix,
        "lossy restart must not erase the committed membership prefix"
    );
}

#[test]
fn remove_voter_transition_preserves_prefix_and_steps_down_removed_leader() {
    let mut cluster = direct_election_stable_four_voter_cluster(SimSeed(0xfeed), NodeId(2));
    elect_node_two(&mut cluster);

    cluster.propose(NodeId(2), b"before-remove".to_vec());
    cluster.deliver_all();
    commit_remove_node_two_transition(&mut cluster, ConfigurationId(30));

    assert_eq!(cluster.role(NodeId(2)), Role::Follower);
    for retained in [NodeId(1), NodeId(3), NodeId(4)] {
        assert!(!cluster
            .effective_membership(retained)
            .contains_voter(NodeId(2)));
        assert_eq!(
            applied_payloads(&cluster, retained),
            vec![b"before-remove".to_vec()],
            "{retained} should keep the prefix committed before removal"
        );
    }

    elect_node_one_after_removal(&mut cluster);
    cluster.propose(NodeId(1), b"after-remove".to_vec());
    cluster.deliver_all();
    flush_commit_notifications(&mut cluster, NodeId(1));

    for retained in [NodeId(1), NodeId(3), NodeId(4)] {
        assert_eq!(
            applied_payloads(&cluster, retained),
            vec![b"before-remove".to_vec(), b"after-remove".to_vec()],
            "{retained} should commit after removal"
        );
    }
    assert_eq!(
        applied_payloads(&cluster, NodeId(2)),
        vec![b"before-remove".to_vec()],
        "removed voter should not receive post-removal proposals"
    );
}

#[test]
fn partitioned_joint_configuration_does_not_create_conflicting_leaders() {
    let mut cluster = direct_election_learner_four_node_cluster(SimSeed(0x91));
    elect_node_one(&mut cluster);
    let barrier = cluster
        .promotion_barrier(NodeId(1), NodeId(4))
        .expect("learner has caught up during election heartbeats");

    cluster.dangerous_raw_configuration_proposal(
        NodeId(1),
        add_voter_joint_entry(ConfigurationId(40)),
        vec![barrier],
    );
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries(NodeId(1), NodeId(4))),
        1
    );
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries_response(NodeId(4), NodeId(1))),
        1
    );
    assert!(
        cluster.drop_matching(|envelope| envelope.from == NodeId(1) || envelope.to == NodeId(1))
            >= 2
    );

    for _ in 0..9 {
        cluster.tick(NodeId(4));
    }
    assert!(cluster.deliver_one_matching(request_vote(NodeId(4), NodeId(1))));
    assert!(cluster.deliver_one_matching(|envelope| {
        envelope.from == NodeId(1)
            && envelope.to == NodeId(4)
            && matches!(envelope.message, rafter::Message::RequestVoteResponse(_))
    }));
    assert!(cluster.deliver_one_matching(request_vote(NodeId(4), NodeId(3))));
    assert!(cluster.deliver_one_matching(|envelope| {
        envelope.from == NodeId(3)
            && envelope.to == NodeId(4)
            && matches!(
                envelope.message,
                rafter::Message::RequestVoteResponse(rafter::RequestVoteResponse {
                    vote_granted: false,
                    ..
                })
            )
    }));
    // Node 4 carries the joint configuration entry, but node 3 has not
    // learned that configuration yet, so active voter fencing makes node 3
    // reject the vote instead of binding itself to a candidate it does not
    // currently recognize as a voter.
    assert_eq!(cluster.role(NodeId(4)), Role::Candidate);

    // Node 2 can still win the same term under node 3's old configuration:
    // the rejected vote for node 4 did not consume node 3's one real vote.
    for _ in 0..9 {
        cluster.tick(NodeId(2));
    }
    assert!(cluster.deliver_one_matching(request_vote(NodeId(2), NodeId(3))));
    assert!(cluster.deliver_one_matching(|envelope| {
        envelope.from == NodeId(3)
            && envelope.to == NodeId(2)
            && matches!(envelope.message, rafter::Message::RequestVoteResponse(_))
    }));
    assert_eq!(cluster.role(NodeId(2)), Role::Leader);

    let max_term = [NodeId(1), NodeId(2), NodeId(3), NodeId(4)]
        .into_iter()
        .map(|node_id| cluster.current_term(node_id).0)
        .max()
        .expect("cluster has nodes");
    for term in 1..=max_term {
        assert!(
            cluster.leaders_in_term(Term(term)).len() <= 1,
            "term {term} should not have conflicting leaders"
        );
    }
}
