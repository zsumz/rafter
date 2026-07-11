use super::super::helpers::{
    deliver_append_entries, deliver_append_entries_response, pre_vote, pre_vote_response,
    request_vote, three_node_cluster,
};
use super::super::*;
use super::fixtures::{bootstrap_state, vote_response};
use rafter::Message;

#[test]
fn simulator_repairs_divergent_follower_suffix_after_failover() {
    let mut cluster = three_node_cluster();
    let prefix = b"old-committed".to_vec();
    let replacement = b"replacement".to_vec();

    cluster
        .restart_node_from_bootstrap(
            NodeId(1),
            bootstrap_state(
                Term(2),
                None,
                &[(Term(1), &prefix), (Term(2), &replacement)],
            ),
        )
        .expect("leader state is valid");
    cluster
        .restart_node_from_bootstrap(
            NodeId(2),
            bootstrap_state(
                Term(2),
                None,
                &[(Term(1), &prefix), (Term(1), b"old-uncommitted")],
            ),
        )
        .expect("follower state is valid");
    cluster
        .restart_node_from_bootstrap(
            NodeId(3),
            bootstrap_state(Term(2), None, &[(Term(1), &prefix)]),
        )
        .expect("voter state is valid");

    for _ in 0..3 {
        cluster.tick(NodeId(1));
    }
    assert_eq!(cluster.deliver_matching(pre_vote(NodeId(1), NodeId(3))), 1);
    assert_eq!(
        cluster.deliver_matching(pre_vote_response(NodeId(3), NodeId(1))),
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

    assert_eq!(
        cluster.deliver_matching(deliver_append_entries(NodeId(1), NodeId(2))),
        1
    );
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries_response(NodeId(2), NodeId(1))),
        1
    );
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries(NodeId(1), NodeId(2))),
        1
    );
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries_response(NodeId(2), NodeId(1))),
        1
    );
    assert_eq!(
        cluster.deliver_matching(deliver_append_entries(NodeId(1), NodeId(2))),
        0
    );

    assert_eq!(
        cluster.log_entries_from(NodeId(2), LogIndex(1)),
        vec![
            LogEntry::application(Term(1), prefix),
            LogEntry::application(Term(2), replacement),
            LogEntry::noop(Term(3)),
        ]
    );
}

#[test]
fn simulator_leadership_noop_repairs_follower_extra_tail() {
    let mut cluster = three_node_cluster();
    let prefix = b"old-committed".to_vec();

    cluster
        .restart_node_from_bootstrap(
            NodeId(1),
            bootstrap_state(Term(1), None, &[(Term(1), &prefix)]),
        )
        .expect("leader state is valid");
    cluster
        .restart_node_from_bootstrap(
            NodeId(2),
            bootstrap_state(
                Term(1),
                None,
                &[(Term(1), &prefix), (Term(1), b"extra-tail")],
            ),
        )
        .expect("follower state is valid");
    cluster
        .restart_node_from_bootstrap(
            NodeId(3),
            bootstrap_state(Term(1), None, &[(Term(1), &prefix)]),
        )
        .expect("voter state is valid");

    for _ in 0..3 {
        cluster.tick(NodeId(1));
    }
    assert_eq!(cluster.deliver_matching(pre_vote(NodeId(1), NodeId(3))), 1);
    assert_eq!(
        cluster.deliver_matching(pre_vote_response(NodeId(3), NodeId(1))),
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

    assert_eq!(
        cluster.deliver_matching(deliver_append_entries(NodeId(1), NodeId(2))),
        1
    );

    let response = cluster
        .pending()
        .find_map(|envelope| match &envelope.message {
            Message::AppendEntriesResponse(response)
                if envelope.from == NodeId(2) && envelope.to == NodeId(1) =>
            {
                Some(response)
            }
            _ => None,
        })
        .expect("follower replies to heartbeat");
    assert!(response.success);
    assert_eq!(response.match_index, LogIndex(2));
    assert_eq!(
        cluster.log_entries_from(NodeId(2), LogIndex(1)),
        vec![
            LogEntry::application(Term(1), prefix),
            LogEntry::noop(Term(2)),
        ]
    );
}
