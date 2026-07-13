//! Resolution of byte-free snapshot directives against payload sources.

use super::*;

#[test]
fn resolved_snapshot_chunk_mirrors_directive_and_slices_payload() {
    let payload = large_snapshot_payload();
    let (mut leader, source) = leader_with_snapshot_payload(payload.clone());
    leader
        .try_follower_progress_mut(NodeId(2))
        .expect("active follower")
        .next_index = LogIndex(3);

    let outputs = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: false,
            match_index: LogIndex::ZERO,
        }),
    });

    assert_eq!(outputs.len(), 1);
    let Output::SendSnapshotChunk { to, chunk } = &outputs[0] else {
        panic!("expected send snapshot chunk directive");
    };
    assert_eq!(*to, NodeId(2));
    assert_eq!(chunk.len, 65_536);
    let message = chunk.resolve(&source).expect("source serves the snapshot");
    assert_eq!(message.term, chunk.term);
    assert_eq!(message.leader_id, chunk.leader_id);
    assert_eq!(message.transfer_id, chunk.transfer_id);
    assert_eq!(message.metadata, chunk.metadata);
    assert_eq!(message.total_payload_len, chunk.total_payload_len);
    assert_eq!(
        message.application_payload_crc32,
        chunk.application_payload_crc32
    );
    assert_eq!(message.offset, chunk.offset);
    assert_eq!(message.done, chunk.done);
    assert_eq!(message.chunk, payload[..chunk.len as usize]);
}
#[test]
fn unresolvable_snapshot_chunk_directive_is_dropped() {
    struct WrongLengthSource;
    impl crate::SnapshotChunkSource for WrongLengthSource {
        fn snapshot_chunk(&self, request: crate::SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
            Some(vec![0; request.len as usize + 1])
        }
    }

    let (mut leader, _source) = leader_with_snapshot_payload(large_snapshot_payload());
    leader
        .try_follower_progress_mut(NodeId(2))
        .expect("active follower")
        .next_index = LogIndex(3);
    let chunk = snapshot_chunk_send_from_output(
        &leader.step(Input::Message {
            from: NodeId(2),
            message: Message::AppendEntriesResponse(AppendEntriesResponse {
                sequence: 0,
                term: leader.current_term(),
                follower_id: NodeId(2),
                success: false,
                match_index: LogIndex::ZERO,
            }),
        })[0],
    );

    assert!(
        chunk.resolve(&InMemorySnapshotChunkSource::new()).is_none(),
        "a source without the transfer's payload cannot materialize the chunk"
    );
    assert!(
        chunk.resolve(&WrongLengthSource).is_none(),
        "a chunk of the wrong length must be dropped, not sent"
    );
}
