use super::super::helpers::{pre_vote, pre_vote_response};
use super::super::*;
use super::fixtures::{
    bootstrap_state, bootstrap_with_snapshot, elect_node_one_with_node_three,
    force_snapshot_catchup_to_node_two, test_snapshot,
};
use rafter::{InstallSnapshot, Message};

#[test]
fn simulator_installs_snapshot_when_follower_is_behind_compacted_prefix() {
    let mut cluster = three_node_cluster();
    let (snapshot, payload) = test_snapshot(1, 2, 1, 2, b"snapshot through two");
    let suffix = b"after snapshot".to_vec();

    cluster.seed_snapshot_payload(NodeId(1), &snapshot, payload.clone());
    cluster
        .restart_node_from_bootstrap(
            NodeId(1),
            bootstrap_with_snapshot(Term(2), snapshot.clone(), &[(3, Term(2), &suffix)]),
        )
        .expect("leader bootstrap is valid");
    cluster
        .restart_node_from_bootstrap(
            NodeId(2),
            bootstrap_state(Term(2), &[(1, Term(1), b"old prefix")]),
        )
        .expect("behind follower bootstrap is valid");
    cluster.seed_snapshot_payload(NodeId(3), &snapshot, payload.clone());
    cluster
        .restart_node_from_bootstrap(
            NodeId(3),
            bootstrap_with_snapshot(Term(2), snapshot.clone(), &[(3, Term(2), &suffix)]),
        )
        .expect("voter bootstrap is valid");

    elect_node_one_with_node_three(&mut cluster);
    force_snapshot_catchup_to_node_two(&mut cluster);

    let follower_state = cluster.bootstrap_state(NodeId(2));
    assert_eq!(follower_state.snapshot, Some(snapshot.clone()));
    assert_eq!(
        cluster.snapshot_payload(NodeId(2), &snapshot),
        Some(payload.as_slice()),
        "the installed payload must be durable in the follower's snapshot store"
    );
    assert_eq!(
        cluster.snapshot_installs(),
        [SnapshotInstalled {
            node_id: NodeId(2),
            last_included_index: LogIndex(2),
            last_included_term: Term(1),
            committed_membership: snapshot.metadata.committed_membership().cloned(),
            payload: payload.clone(),
            applied_records_before_install: 0,
        }]
    );
    assert_eq!(
        cluster.log_entries_from(NodeId(2), LogIndex(3)),
        vec![
            LogEntry::application(Term(2), suffix),
            LogEntry::noop(Term(3))
        ]
    );
    assert!(
        cluster
            .applied()
            .iter()
            .all(|applied| applied.payload != payload),
        "installing a snapshot must not expose snapshot bytes as committed log entries"
    );
}

#[test]
fn simulator_discards_divergent_suffix_when_installing_snapshot() {
    let mut cluster = three_node_cluster();
    let (snapshot, payload) = test_snapshot(1, 2, 1, 2, b"snapshot through two");

    cluster.seed_snapshot_payload(NodeId(1), &snapshot, payload.clone());
    cluster
        .restart_node_from_bootstrap(
            NodeId(1),
            bootstrap_with_snapshot(Term(2), snapshot.clone(), &[]),
        )
        .expect("leader bootstrap is valid");
    cluster
        .restart_node_from_bootstrap(
            NodeId(2),
            bootstrap_state(
                Term(2),
                &[
                    (1, Term(1), b"old prefix"),
                    (2, Term(2), b"divergent boundary"),
                    (3, Term(2), b"divergent suffix"),
                ],
            ),
        )
        .expect("divergent follower bootstrap is valid");
    cluster.seed_snapshot_payload(NodeId(3), &snapshot, payload.clone());
    cluster
        .restart_node_from_bootstrap(
            NodeId(3),
            bootstrap_with_snapshot(Term(2), snapshot.clone(), &[]),
        )
        .expect("voter bootstrap is valid");

    elect_node_one_with_node_three(&mut cluster);

    force_snapshot_catchup_to_node_two(&mut cluster);

    assert_eq!(cluster.bootstrap_state(NodeId(2)).snapshot, Some(snapshot));
    assert_eq!(
        cluster.log_entries_from(NodeId(2), LogIndex(3)),
        vec![LogEntry::noop(Term(3))]
    );
}

#[test]
fn simulator_newer_snapshot_term_fences_stale_leader() {
    let mut cluster = three_node_cluster();
    for _ in 0..9 {
        cluster.tick(NodeId(2));
    }
    assert_eq!(cluster.deliver_matching(pre_vote(NodeId(2), NodeId(3))), 1);
    assert_eq!(
        cluster.deliver_matching(pre_vote_response(NodeId(3), NodeId(2))),
        1
    );
    assert_eq!(cluster.role(NodeId(2)), Role::Candidate);
    assert_eq!(cluster.current_term(NodeId(2)), Term(1));

    let (snapshot, payload) = test_snapshot(1, 2, 1, 2, b"newer snapshot");
    cluster.deliver_message(
        NodeId(1),
        NodeId(2),
        Message::InstallSnapshot(InstallSnapshot {
            term: Term(2),
            leader_id: NodeId(1),
            metadata: snapshot.metadata.clone(),
            application_payload: payload.clone(),
        }),
    );

    assert_eq!(cluster.role(NodeId(2)), Role::Follower);
    assert_eq!(cluster.current_term(NodeId(2)), Term(2));
    assert_eq!(
        cluster.bootstrap_state(NodeId(2)).snapshot,
        Some(snapshot.clone())
    );
    assert_eq!(
        cluster.snapshot_payload(NodeId(2), &snapshot),
        Some(payload.as_slice()),
        "a whole-message install must stage and promote the payload bytes"
    );

    cluster.deliver_message(
        NodeId(3),
        NodeId(2),
        Message::AppendEntries(rafter::AppendEntries {
            sequence: 0,
            term: Term(1),
            leader_id: NodeId(3),
            prev_log_index: LogIndex(2),
            prev_log_term: Term(1),
            entries: vec![LogEntry::application(
                Term(1),
                b"stale leader write".to_vec(),
            )]
            .into(),
            leader_commit: LogIndex(3),
        }),
    );

    let response = cluster
        .pending()
        .find_map(|envelope| match &envelope.message {
            Message::AppendEntriesResponse(response)
                if envelope.from == NodeId(2) && envelope.to == NodeId(3) =>
            {
                Some(*response)
            }
            _ => None,
        })
        .expect("stale leader receives rejection");
    assert_eq!(response.term, Term(2));
    assert!(!response.success);
    assert_eq!(cluster.log_entries_from(NodeId(2), LogIndex(3)), Vec::new());
}

fn three_node_cluster() -> Cluster {
    super::super::helpers::three_node_cluster()
}
