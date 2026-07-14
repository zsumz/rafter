use super::super::helpers::{deliver_append_entries, deliver_append_entries_response};
use super::super::*;
use super::fixtures::{
    bootstrap_state, bootstrap_with_snapshot, elect_node_one_with_node_three,
    install_snapshot_chunk, multi_chunk_payload, test_snapshot,
};
use rafter::Message;

#[test]
fn simulator_streams_multi_chunk_snapshot_and_installs_assembled_payload() {
    let mut cluster = three_node_cluster();
    let (snapshot, payload) = test_snapshot(1, 2, 1, 2, &multi_chunk_payload());

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
            bootstrap_state(Term(2), &[(1, Term(1), b"old prefix")]),
        )
        .expect("behind follower bootstrap is valid");
    cluster.seed_snapshot_payload(NodeId(3), &snapshot, payload.clone());
    cluster
        .restart_node_from_bootstrap(
            NodeId(3),
            bootstrap_with_snapshot(Term(2), snapshot.clone(), &[]),
        )
        .expect("voter bootstrap is valid");

    elect_node_one_with_node_three(&mut cluster);

    let mut chunk_deliveries = 0;
    loop {
        if cluster.deliver_one_matching(install_snapshot_chunk(NodeId(1), NodeId(2))) {
            chunk_deliveries += 1;
            continue;
        }
        if !cluster.deliver_one_matching(|envelope| {
            (envelope.from == NodeId(1) && envelope.to == NodeId(2))
                || (envelope.from == NodeId(2) && envelope.to == NodeId(1))
        }) {
            break;
        }
    }

    assert_eq!(
        chunk_deliveries, 2,
        "a 70 KiB payload must stream as one full 64 KiB chunk plus a final remainder"
    );
    assert_eq!(
        cluster.bootstrap_state(NodeId(2)).snapshot,
        Some(snapshot.clone())
    );
    assert_eq!(
        cluster.snapshot_payload(NodeId(2), &snapshot),
        Some(payload.as_slice()),
        "chunks staged in offset order must reassemble the leader's exact payload"
    );
    assert_eq!(
        cluster.snapshot_installs(),
        [SnapshotInstalled {
            node_id: NodeId(2),
            application_epoch: 0,
            commit_index_at_emit: LogIndex(2),
            last_included_index: LogIndex(2),
            last_included_term: Term(1),
            committed_membership: snapshot.metadata.committed_membership().cloned(),
            payload,
            applied_records_before_install: 0,
        }]
    );
}

#[test]
#[should_panic(expected = "snapshot content invariant violated")]
fn simulator_detects_installed_bytes_diverging_from_leader_store() {
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
            bootstrap_state(Term(2), &[(1, Term(1), b"old prefix")]),
        )
        .expect("behind follower bootstrap is valid");
    cluster.seed_snapshot_payload(NodeId(3), &snapshot, payload.clone());
    cluster
        .restart_node_from_bootstrap(
            NodeId(3),
            bootstrap_with_snapshot(Term(2), snapshot.clone(), &[]),
        )
        .expect("voter bootstrap is valid");

    elect_node_one_with_node_three(&mut cluster);

    // Drive append rounds until the leader resolves the chunk directive and
    // the wire message sits in the network with the original bytes.
    while !cluster.pending().any(|envelope| {
        envelope.from == NodeId(1)
            && envelope.to == NodeId(2)
            && matches!(envelope.message, Message::InstallSnapshotChunk(_))
    }) {
        assert_eq!(
            cluster.deliver_matching(deliver_append_entries(NodeId(1), NodeId(2))),
            1
        );
        assert_eq!(
            cluster.deliver_matching(deliver_append_entries_response(NodeId(2), NodeId(1))),
            1
        );
    }

    // Corrupt the leader's stored payload for the same descriptor, so the
    // bytes the follower assembles no longer match what the leader serves.
    let mut corrupted = payload;
    corrupted[0] ^= 0xff;
    cluster.seed_snapshot_payload(NodeId(1), &snapshot, corrupted);

    cluster.deliver_matching(install_snapshot_chunk(NodeId(1), NodeId(2)));
}

fn three_node_cluster() -> Cluster {
    super::super::helpers::three_node_cluster()
}
