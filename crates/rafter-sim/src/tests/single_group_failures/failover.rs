use super::super::helpers::{
    deliver_append_entries, deliver_append_entries_response, direct_election_three_node_cluster,
    elect_node_one, elect_node_two_without_reaching_node_one, pre_vote, pre_vote_response,
    request_vote, seeded_three_node_cluster, three_node_cluster,
};
use super::super::*;
use super::fixtures::vote_response;
use rafter_invariant_test::{oracle_assert, oracle_assert_eq};

#[test]
fn simulator_elects_and_fails_over_without_clients_under_delays() {
    let mut cluster = seeded_three_node_cluster(17);

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
    assert_eq!(cluster.role(NodeId(1)), Role::Leader);
    cluster.advance_clock();
    cluster.advance_clock();
    assert_eq!(
        cluster.deliver_matching(request_vote(NodeId(1), NodeId(2))),
        1
    );

    for _ in 0..9 {
        cluster.tick(NodeId(2));
    }
    assert!(
        cluster.drop_matching(|envelope| envelope.from == NodeId(1) || envelope.to == NodeId(1))
            >= 1
    );
    assert_eq!(cluster.deliver_matching(pre_vote(NodeId(2), NodeId(3))), 1);
    assert_eq!(
        cluster.deliver_matching(pre_vote_response(NodeId(3), NodeId(2))),
        1
    );
    assert_eq!(
        cluster.delay_matching(request_vote(NodeId(2), NodeId(3)), 1),
        1
    );
    assert!(!cluster.deliver_one_matching(request_vote(NodeId(2), NodeId(3))));
    cluster.advance_clock();
    assert_eq!(
        cluster.deliver_matching(request_vote(NodeId(2), NodeId(3))),
        1
    );
    assert_eq!(
        cluster.deliver_matching(vote_response(NodeId(3), NodeId(2))),
        1
    );

    assert_eq!(cluster.role(NodeId(2)), Role::Leader);
    assert_eq!(cluster.leaders_in_term(Term(1)), vec![NodeId(1)]);
    assert_eq!(cluster.leaders_in_term(Term(2)), vec![NodeId(2)]);
}

#[test]
fn simulator_restarted_follower_preserves_prefix_through_failover() {
    let mut cluster = direct_election_three_node_cluster();
    elect_node_one(&mut cluster);
    cluster.propose(NodeId(1), b"old-committed".to_vec());
    cluster.deliver_all();
    cluster.tick(NodeId(1));
    cluster.deliver_all();
    assert_eq!(cluster.commit_index(NodeId(1)), LogIndex(2));
    assert_eq!(cluster.commit_index(NodeId(2)), LogIndex(2));
    assert_eq!(cluster.commit_index(NodeId(3)), LogIndex(2));

    let restart_state = cluster.bootstrap_state(NodeId(2));
    cluster
        .restart_node_from_bootstrap(NodeId(2), restart_state)
        .expect("follower restart hydrates");
    assert_eq!(
        cluster.log_entries_from(NodeId(2), LogIndex(1)),
        vec![
            LogEntry::noop(Term(1)),
            LogEntry::application(Term(1), b"old-committed".to_vec())
        ]
    );

    elect_node_two_without_reaching_node_one(&mut cluster);
    cluster.propose(NodeId(2), b"new-committed".to_vec());
    for _ in 0..4 {
        let _ = cluster.deliver_matching(deliver_append_entries(NodeId(2), NodeId(3)));
        let _ = cluster.deliver_matching(deliver_append_entries_response(NodeId(3), NodeId(2)));
    }

    assert_eq!(cluster.commit_index(NodeId(2)), LogIndex(4));
    assert_eq!(
        cluster.log_entries_from(NodeId(2), LogIndex(1)),
        vec![
            LogEntry::noop(Term(1)),
            LogEntry::application(Term(1), b"old-committed".to_vec()),
            LogEntry::noop(Term(2)),
            LogEntry::application(Term(2), b"new-committed".to_vec()),
        ]
    );
}

#[test]
fn simulator_transfers_leadership_and_preserves_committed_entries() {
    let mut cluster = three_node_cluster();
    elect_node_one(&mut cluster);
    cluster.propose(NodeId(1), b"before-transfer".to_vec());
    cluster.deliver_all();
    assert_eq!(cluster.commit_index(NodeId(1)), LogIndex(2));

    cluster.transfer_leadership(NodeId(1), NodeId(2));
    cluster.deliver_all();

    assert_eq!(cluster.role(NodeId(2)), Role::Leader);
    assert_eq!(cluster.role(NodeId(1)), Role::Follower);
    for term in 1..=cluster.current_term(NodeId(2)).0 {
        assert!(
            cluster.leaders_in_term(Term(term)).len() <= 1,
            "term {term} must have at most one leader"
        );
    }

    // The new leader still serves the committed history and new proposals.
    cluster.propose(NodeId(2), b"after-transfer".to_vec());
    cluster.deliver_all();
    assert_eq!(cluster.commit_index(NodeId(2)), LogIndex(4));
}

#[test]
fn simulator_isolated_leader_grants_no_reads_while_majority_moves_on() {
    let mut cluster = three_node_cluster();
    elect_node_one(&mut cluster);
    cluster.propose(NodeId(1), b"committed-in-term-one".to_vec());
    cluster.deliver_all();
    assert_eq!(cluster.commit_index(NodeId(1)), LogIndex(2));

    // A confirmed barrier works while the leader is healthy.
    cluster.read_index(NodeId(1), 1);
    cluster.tick(NodeId(1));
    cluster.deliver_all();
    assert!(cluster
        .read_grants()
        .iter()
        .any(|grant| grant.request_id == 1 && grant.read_index == LogIndex(2)));

    // Isolate the leader: everything it sends from now on is dropped.
    cluster.read_index(NodeId(1), 2);
    for _ in 0..12 {
        cluster.tick(NodeId(1));
        let _ = cluster
            .drop_matching(|envelope| envelope.from == NodeId(1) || envelope.to == NodeId(1));
    }

    // The isolated ex-leader must never confirm barrier 2, no matter how
    // long it keeps believing in itself.
    oracle_assert!(
        !cluster
            .read_grants()
            .iter()
            .any(|grant| grant.request_id == 2),
        "an isolated leader granted a read barrier without a quorum"
    );

    // The surviving majority elects a new leader and commits past the old
    // leader's view — exactly the write barrier 2 must not miss.
    for _ in 0..9 {
        cluster.tick(NodeId(2));
    }
    cluster.deliver_all();
    let _ = cluster.drop_matching(|envelope| envelope.to == NodeId(1));
    oracle_assert_eq!(cluster.role(NodeId(2)), Role::Leader);
    cluster.propose(NodeId(2), b"committed-in-term-two".to_vec());
    cluster.deliver_all();
    let _ = cluster.drop_matching(|envelope| envelope.to == NodeId(1));
    oracle_assert_eq!(cluster.commit_index(NodeId(2)), LogIndex(4));
    oracle_assert!(!cluster
        .read_grants()
        .iter()
        .any(|grant| grant.request_id == 2));
}
