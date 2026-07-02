use super::helpers::{
    deliver_append_entries, deliver_append_entries_response, direct_election_three_node_cluster,
    elect_node_one, elect_node_two_without_reaching_node_one, three_node_cluster,
};
use super::*;
use rafter::Message;

#[test]
fn three_node_cluster_elects_one_leader() {
    let mut cluster = three_node_cluster();

    elect_node_one(&mut cluster);

    assert_eq!(cluster.leaders(), vec![NodeId(1)]);
}

#[test]
fn proposal_commits_on_leader_after_quorum_replication() {
    let mut cluster = three_node_cluster();
    elect_node_one(&mut cluster);

    cluster.propose(NodeId(1), b"incident-opened".to_vec());
    cluster.deliver_all();

    assert_eq!(cluster.commit_index(NodeId(1)), LogIndex(2));
    assert_eq!(cluster.commit_index(NodeId(2)), LogIndex(1));
    assert_eq!(cluster.commit_index(NodeId(3)), LogIndex(1));
    assert_eq!(
        cluster.applied(),
        &[Applied {
            node_id: NodeId(1),
            index: LogIndex(2),
            payload: b"incident-opened".to_vec().into(),
        }]
    );
}

#[test]
fn committed_entry_reaches_followers_on_next_heartbeat() {
    let mut cluster = three_node_cluster();
    elect_node_one(&mut cluster);
    cluster.propose(NodeId(1), b"incident-opened".to_vec());
    cluster.deliver_all();

    cluster.tick(NodeId(1));
    cluster.deliver_all();

    assert_eq!(cluster.commit_index(NodeId(2)), LogIndex(2));
    assert_eq!(cluster.commit_index(NodeId(3)), LogIndex(2));
    assert_eq!(
        cluster.applied(),
        &[
            Applied {
                node_id: NodeId(1),
                index: LogIndex(2),
                payload: b"incident-opened".to_vec().into(),
            },
            Applied {
                node_id: NodeId(2),
                index: LogIndex(2),
                payload: b"incident-opened".to_vec().into(),
            },
            Applied {
                node_id: NodeId(3),
                index: LogIndex(2),
                payload: b"incident-opened".to_vec().into(),
            },
        ]
    );
}

#[test]
fn partitioned_election_has_no_two_leaders_in_same_term() {
    let mut cluster = direct_election_three_node_cluster();
    elect_node_one(&mut cluster);

    elect_node_two_without_reaching_node_one(&mut cluster);

    assert_eq!(cluster.role(NodeId(1)), Role::Leader);
    assert_eq!(cluster.role(NodeId(2)), Role::Leader);
    assert_eq!(cluster.leaders_in_term(Term(1)), vec![NodeId(1)]);
    assert_eq!(cluster.leaders_in_term(Term(2)), vec![NodeId(2)]);
}

#[test]
fn stale_leader_cannot_commit_after_new_term_wins() {
    let mut cluster = direct_election_three_node_cluster();
    elect_node_one(&mut cluster);
    elect_node_two_without_reaching_node_one(&mut cluster);

    cluster.propose(NodeId(1), b"stale-write".to_vec());
    assert_eq!(
        cluster.deliver_matching(|envelope| envelope.from == NodeId(1)
            && matches!(envelope.message, Message::AppendEntries(_))),
        2
    );
    assert_eq!(
        cluster.deliver_matching(|envelope| envelope.to == NodeId(1)
            && matches!(envelope.message, Message::AppendEntriesResponse(_))),
        2
    );

    assert_eq!(cluster.role(NodeId(1)), Role::Follower);
    assert_eq!(cluster.current_term(NodeId(1)), Term(2));
    assert!(!cluster
        .applied()
        .iter()
        .any(|applied| applied.payload == b"stale-write"));
}

#[test]
fn committed_prefix_is_stable_across_failover() {
    let mut cluster = direct_election_three_node_cluster();
    elect_node_one(&mut cluster);

    cluster.propose(NodeId(1), b"old-committed".to_vec());
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries(NodeId(1), NodeId(2))),
        1
    );
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries_response(NodeId(2), NodeId(1))),
        1
    );
    assert_eq!(
        cluster.drop_matching(deliver_append_entries(NodeId(1), NodeId(3))),
        1
    );
    assert_eq!(cluster.commit_index(NodeId(1)), LogIndex(2));
    assert_eq!(cluster.last_log_index(NodeId(2)), LogIndex(2));
    assert_eq!(cluster.last_log_index(NodeId(3)), LogIndex(1));

    elect_node_two_without_reaching_node_one(&mut cluster);

    assert_eq!(cluster.last_log_index(NodeId(3)), LogIndex(1));
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries(NodeId(2), NodeId(3))),
        1
    );
    assert_eq!(cluster.last_log_index(NodeId(3)), LogIndex(3));
    let _ = cluster.deliver_matching(deliver_append_entries_response(NodeId(3), NodeId(2)));

    cluster.propose(NodeId(2), b"new-committed".to_vec());
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries(NodeId(2), NodeId(3))),
        1
    );
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries_response(NodeId(3), NodeId(2))),
        1
    );
    assert_eq!(cluster.commit_index(NodeId(2)), LogIndex(4));

    cluster.tick(NodeId(2));
    assert_eq!(
        cluster.drop_matching(deliver_append_entries(NodeId(2), NodeId(1))),
        2
    );
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries(NodeId(2), NodeId(3))),
        1
    );

    assert_eq!(cluster.commit_index(NodeId(3)), LogIndex(4));
    assert_eq!(
        cluster.applied(),
        &[
            Applied {
                node_id: NodeId(1),
                index: LogIndex(2),
                payload: b"old-committed".to_vec().into(),
            },
            Applied {
                node_id: NodeId(2),
                index: LogIndex(2),
                payload: b"old-committed".to_vec().into(),
            },
            Applied {
                node_id: NodeId(3),
                index: LogIndex(2),
                payload: b"old-committed".to_vec().into(),
            },
            Applied {
                node_id: NodeId(2),
                index: LogIndex(4),
                payload: b"new-committed".to_vec().into(),
            },
            Applied {
                node_id: NodeId(3),
                index: LogIndex(4),
                payload: b"new-committed".to_vec().into(),
            },
        ]
    );
}

#[test]
fn lagging_follower_catches_up_from_leader_backtracking() {
    let mut cluster = direct_election_three_node_cluster();
    elect_node_one(&mut cluster);

    cluster.propose(NodeId(1), b"old-committed".to_vec());
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries(NodeId(1), NodeId(2))),
        1
    );
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries_response(NodeId(2), NodeId(1))),
        1
    );
    assert_eq!(
        cluster.drop_matching(deliver_append_entries(NodeId(1), NodeId(3))),
        1
    );
    assert_eq!(cluster.last_log_index(NodeId(2)), LogIndex(2));
    assert_eq!(cluster.last_log_index(NodeId(3)), LogIndex(1));

    elect_node_two_without_reaching_node_one(&mut cluster);

    assert_eq!(cluster.last_log_index(NodeId(3)), LogIndex(1));
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries(NodeId(2), NodeId(3))),
        1
    );
    assert_eq!(cluster.last_log_index(NodeId(3)), LogIndex(3));
}
