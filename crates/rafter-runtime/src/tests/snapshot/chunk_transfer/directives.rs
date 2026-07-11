use super::*;

#[test]
fn runtime_leader_resolves_chunk_directives_into_install_snapshot_chunk_messages() {
    let mut leader = elected_leader_with_snapshot_store(InMemoryRaftSnapshotStore::new());
    commit_with_follower_ack(&mut leader, b"create stream payload", 2);
    commit_with_follower_ack(&mut leader, b"append stream payload", 3);
    let snapshot = raft_snapshot_for_writer(3, 1, 1, 2, b"opaque application snapshot");
    leader
        .compact_log_with_snapshot(snapshot.clone())
        .expect("leader compacts through its durable snapshot");

    let outputs = report_follower_lag(&mut leader);

    assert!(
        outputs
            .iter()
            .all(|output| !matches!(output, RaftOutput::SendSnapshotChunk { .. })),
        "callers must never see unresolved snapshot chunk directives"
    );
    let chunk = outputs
        .iter()
        .find_map(|output| match output {
            RaftOutput::Send {
                to,
                message: Message::InstallSnapshotChunk(chunk),
            } if *to == RaftNodeId(4) => Some(chunk),
            _ => None,
        })
        .expect("leader streams a resolved snapshot chunk to the lagging follower");
    let installed = leader
        .snapshot()
        .expect("leader installed a local snapshot");
    assert_eq!(chunk.metadata, installed.metadata);
    assert_eq!(chunk.transfer_id, installed.transfer_id());
    assert_eq!(
        chunk.application_payload_crc32,
        installed.application_payload_crc32
    );
    assert_eq!(
        chunk.total_payload_len,
        snapshot.application_payload.len() as u64
    );
    assert_eq!(chunk.offset, 0);
    assert_eq!(chunk.chunk, snapshot.application_payload);
    assert!(chunk.done);
}

#[test]
fn runtime_drops_snapshot_chunk_directives_the_store_cannot_serve() {
    let mut leader = elected_leader_with_snapshot_store(UnservableChunkSourceStore(
        InMemoryRaftSnapshotStore::new(),
    ));
    commit_with_follower_ack(&mut leader, b"create stream payload", 2);
    commit_with_follower_ack(&mut leader, b"append stream payload", 3);
    leader
        .compact_log_with_snapshot(raft_snapshot_for_writer(3, 1, 1, 2, b"unservable payload"))
        .expect("leader compacts through its durable snapshot");

    let outputs = report_follower_lag(&mut leader);

    assert!(
        outputs.iter().all(|output| !matches!(
            output,
            RaftOutput::SendSnapshotChunk { .. }
                | RaftOutput::Send {
                    message: Message::InstallSnapshotChunk(_),
                    ..
                }
        )),
        "an unresolvable directive is dropped like a lost message"
    );
}
