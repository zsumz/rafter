use super::helpers::{
    deliver_append_entries, deliver_append_entries_response, pre_vote, pre_vote_response,
    request_vote, three_node_cluster,
};
use super::*;
use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    BootstrapLogEntry, BootstrapState, InstallSnapshot, Message, RaftSnapshot,
    RaftSnapshotMetadata, RequestVoteResponse, SnapshotGroupId,
};

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
            last_included_index: LogIndex(2),
            last_included_term: Term(1),
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

fn elect_node_one_with_node_three(cluster: &mut Cluster) {
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
}

fn force_snapshot_catchup_to_node_two(cluster: &mut Cluster) {
    for _ in 0..8 {
        if cluster.deliver_matching(install_snapshot_chunk(NodeId(1), NodeId(2))) == 1 {
            assert_eq!(
                cluster.deliver_matching(install_snapshot_response(NodeId(2), NodeId(1))),
                1
            );
            assert_eq!(
                cluster.deliver_matching(deliver_append_entries(NodeId(1), NodeId(2))),
                1
            );
            return;
        }
        assert_eq!(
            cluster.deliver_matching(deliver_append_entries(NodeId(1), NodeId(2))),
            1
        );
        assert_eq!(
            cluster.deliver_matching(deliver_append_entries_response(NodeId(2), NodeId(1))),
            1
        );
    }
    panic!("leader did not fall back to snapshot transfer");
}

fn install_snapshot_chunk(from: NodeId, to: NodeId) -> impl FnMut(&Envelope) -> bool {
    move |envelope| {
        envelope.from == from
            && envelope.to == to
            && matches!(envelope.message, Message::InstallSnapshotChunk(_))
    }
}

fn install_snapshot_response(from: NodeId, to: NodeId) -> impl FnMut(&Envelope) -> bool {
    move |envelope| {
        envelope.from == from
            && envelope.to == to
            && matches!(envelope.message, Message::InstallSnapshotResponse(_))
    }
}

fn vote_response(from: NodeId, to: NodeId) -> impl FnMut(&Envelope) -> bool {
    move |envelope| {
        envelope.from == from
            && envelope.to == to
            && matches!(
                envelope.message,
                Message::RequestVoteResponse(RequestVoteResponse { .. })
            )
    }
}

fn bootstrap_state(current_term: Term, entries: &[(u64, Term, &[u8])]) -> BootstrapState {
    BootstrapState {
        current_term,
        voted_for: None,
        commit_index: LogIndex::ZERO,
        committed_configuration: None,
        snapshot: None,
        log: entries
            .iter()
            .map(|(index, term, payload)| {
                BootstrapLogEntry::application(LogIndex(*index), *term, (*payload).to_vec())
            })
            .collect(),
    }
}

fn bootstrap_with_snapshot(
    current_term: Term,
    snapshot: RaftSnapshot,
    entries: &[(u64, Term, &[u8])],
) -> BootstrapState {
    BootstrapState {
        current_term,
        voted_for: None,
        commit_index: LogIndex::ZERO,
        committed_configuration: None,
        snapshot: Some(snapshot),
        log: entries
            .iter()
            .map(|(index, term, payload)| {
                BootstrapLogEntry::application(LogIndex(*index), *term, (*payload).to_vec())
            })
            .collect(),
    }
}

/// Builds a snapshot descriptor for `payload` and returns both; the caller
/// seeds the payload into each node whose store must hold the content.
fn test_snapshot(
    writer_id: u64,
    last_included_index: u64,
    last_included_term: u64,
    hard_state_term: u64,
    payload: &[u8],
) -> (RaftSnapshot, Vec<u8>) {
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("sim-data-group").expect("valid snapshot group id"),
        NodeId(writer_id),
        LogIndex(last_included_index),
        Term(last_included_term),
        Term(hard_state_term),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("stream_data").expect("valid snapshot kind"),
            ApplicationSnapshotVersion::new(1).expect("valid snapshot version"),
        ),
    )
    .expect("valid snapshot metadata");
    let snapshot = RaftSnapshot::from_payload(metadata, payload);
    (snapshot, payload.to_vec())
}

/// A payload larger than the kernel's 64 KiB chunk directive, so a transfer
/// takes more than one `InstallSnapshotChunk` message.
fn multi_chunk_payload() -> Vec<u8> {
    let mut payload = Vec::with_capacity(70 * 1024);
    while payload.len() < 70 * 1024 {
        payload.extend_from_slice(b"simulator-multi-chunk-snapshot-payload");
    }
    payload.truncate(70 * 1024);
    payload
}
