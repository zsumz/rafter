use super::super::*;
use super::helpers::{bootstrap_entry, node};
use super::replication_snapshot_support::{
    install_snapshot_response_from_outputs, snapshot_chunk_send_from_output, test_snapshot,
};
use crate::{
    AppendEntriesResponse, InstallSnapshotResponse, MembershipConfig, MembershipSet,
    PendingSnapshotTransfer, PendingSnapshotTransferResumeError, RaftSnapshot,
    SnapshotChunkRequest, SnapshotChunkSource, SnapshotTransferId,
};

const SNAPSHOT_CHUNK_BYTES: u64 = 64 * 1024;
const PEAK_RESIDENT_PAYLOAD_LIMIT_BYTES: u64 = 3 * SNAPSHOT_CHUNK_BYTES;
const SYNTHETIC_BLOCK_MULTIPLIER: u64 = 0x9E37_79B9_7F4A_7C15;
const SYNTHETIC_BLOCK_INCREMENT: u64 = 0xD1B5_4A32_D192_ED03;

/// Serves a declared payload without backing storage: every byte is a
/// deterministic function of its absolute offset, so multi-gigabyte
/// transfers cost chunk-sized allocations only.
#[derive(Debug)]
struct SyntheticPayload {
    transfer_id: SnapshotTransferId,
    total_payload_len: u64,
    application_payload_crc32: u32,
}

impl SyntheticPayload {
    fn new(snapshot: &RaftSnapshot) -> Self {
        Self {
            transfer_id: snapshot.transfer_id(),
            total_payload_len: snapshot.application_payload_len,
            application_payload_crc32: snapshot.application_payload_crc32,
        }
    }
}

impl SnapshotChunkSource for SyntheticPayload {
    fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        if request.transfer_id != self.transfer_id
            || request.total_payload_len != self.total_payload_len
            || request.application_payload_crc32 != self.application_payload_crc32
        {
            return None;
        }
        let end = request.offset.checked_add(u64::from(request.len))?;
        if end > self.total_payload_len {
            return None;
        }
        let mut bytes = vec![0_u8; request.len as usize];
        fill_synthetic(request.offset, &mut bytes);
        Some(bytes)
    }
}

fn synthetic_block(block_index: u64) -> [u8; 8] {
    block_index
        .wrapping_mul(SYNTHETIC_BLOCK_MULTIPLIER)
        .wrapping_add(SYNTHETIC_BLOCK_INCREMENT)
        .to_le_bytes()
}

fn fill_synthetic(offset: u64, bytes: &mut [u8]) {
    let mut cursor = 0_usize;
    while cursor < bytes.len() {
        let position = offset + cursor as u64;
        let block = synthetic_block(position / 8);
        let within = (position % 8) as usize;
        let take = (8 - within).min(bytes.len() - cursor);
        bytes[cursor..cursor + take].copy_from_slice(&block[within..within + take]);
        cursor += take;
    }
}

fn verify_synthetic(offset: u64, bytes: &[u8]) -> bool {
    let mut cursor = 0_usize;
    while cursor < bytes.len() {
        let position = offset + cursor as u64;
        let block = synthetic_block(position / 8);
        let within = (position % 8) as usize;
        let take = (8 - within).min(bytes.len() - cursor);
        if bytes[cursor..cursor + take] != block[within..within + take] {
            return false;
        }
        cursor += take;
    }
    true
}

#[derive(Debug, Default)]
struct TransferAccounting {
    staged_chunk_count: u64,
    total_staged_bytes: u64,
    max_staged_chunk_bytes: u64,
    peak_resident_payload_bytes: u64,
}

fn synthetic_snapshot(total_payload_len: u64) -> RaftSnapshot {
    RaftSnapshot::new(test_snapshot(3, 4, 5, &[]).metadata, total_payload_len, 0)
}

fn streaming_leader(total_payload_len: u64) -> (Node, SyntheticPayload, RaftSnapshot) {
    let snapshot = synthetic_snapshot(total_payload_len);
    let source = SyntheticPayload::new(&snapshot);
    let mut leader = Node::from_bootstrap(
        NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3).expect("test config is valid"),
        BootstrapState {
            current_term: Term(5),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: Some(snapshot.clone()),
            log: vec![bootstrap_entry(4, 5, b"suffix-four")],
        },
    )
    .expect("leader hydrates from snapshot");
    leader.become_leader();
    (leader, source, snapshot)
}

fn restarted_follower() -> Node {
    Node::from_bootstrap(
        NodeConfig::new(NodeId(2), vec![NodeId(1), NodeId(3)], 3).expect("test config is valid"),
        BootstrapState {
            current_term: Term(5),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: None,
            log: Vec::new(),
        },
    )
    .expect("follower hydrates from persisted hard state")
}

fn restarted_dynamic_receiver_without_leader_bootstrap_peer() -> Node {
    Node::from_bootstrap(
        NodeConfig::new_non_voter(NodeId(4), vec![NodeId(2), NodeId(3)], 3)
            .expect("test config is valid"),
        BootstrapState {
            current_term: Term(5),
            voted_for: None,
            commit_index: LogIndex::ZERO,
            committed_configuration: None,
            snapshot: None,
            log: Vec::new(),
        },
    )
    .expect("dynamic receiver hydrates from persisted hard state")
}

fn dynamic_snapshot(payload: &[u8]) -> RaftSnapshot {
    let membership = MembershipConfig::stable(
        MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], vec![NodeId(4)])
            .expect("dynamic snapshot membership is valid"),
    );
    let metadata = test_snapshot(3, 4, 5, payload)
        .metadata
        .with_committed_membership(membership);
    RaftSnapshot::from_payload(metadata, payload)
}

fn pending_snapshot_transfer(
    leader_id: NodeId,
    snapshot: &RaftSnapshot,
    received_len: u64,
) -> PendingSnapshotTransfer {
    PendingSnapshotTransfer {
        leader_id,
        transfer_id: snapshot.transfer_id(),
        metadata: snapshot.metadata.clone(),
        total_payload_len: snapshot.application_payload_len,
        application_payload_crc32: snapshot.application_payload_crc32,
        received_len,
    }
}

fn start_snapshot_stream(leader: &mut Node) -> Vec<Output> {
    leader
        .try_follower_progress_mut(NodeId(2))
        .expect("active follower")
        .next_index = LogIndex(3);
    leader.step(Input::Message {
        from: NodeId(2),
        message: Message::AppendEntriesResponse(AppendEntriesResponse {
            sequence: 0,
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: false,
            match_index: LogIndex::ZERO,
        }),
    })
}

fn pump_snapshot_transfer(
    leader: &mut Node,
    follower: &mut Node,
    source: &SyntheticPayload,
    mut leader_outputs: Vec<Output>,
    first_offset: u64,
) -> (TransferAccounting, RaftSnapshot) {
    let mut accounting = TransferAccounting::default();
    let mut expected_offset = first_offset;
    loop {
        assert_eq!(
            leader_outputs.len(),
            1,
            "mid-transfer the leader emits exactly one chunk directive"
        );
        let directive = snapshot_chunk_send_from_output(&leader_outputs[0]);
        assert_eq!(directive.offset, expected_offset);
        let message = directive
            .resolve(source)
            .expect("synthetic source serves every chunk directive");
        let in_flight_bytes = message.chunk.len() as u64;
        assert_eq!(in_flight_bytes, u64::from(directive.len));
        assert!(in_flight_bytes <= SNAPSHOT_CHUNK_BYTES);
        let offset = message.offset;
        let done = message.done;
        expected_offset = offset + in_flight_bytes;
        if done {
            assert_eq!(expected_offset, directive.total_payload_len);
        }

        let follower_outputs = follower.step(Input::Message {
            from: leader.id(),
            message: Message::InstallSnapshotChunk(message),
        });

        let mut staged_bytes = 0_u64;
        for output in &follower_outputs {
            let Output::StageSnapshotChunk { chunk } = output else {
                continue;
            };
            assert_eq!(chunk.transfer_id, directive.transfer_id);
            assert_eq!(chunk.offset, offset);
            assert_eq!(chunk.done, done);
            assert!(
                verify_synthetic(chunk.offset, &chunk.bytes),
                "staged bytes at offset {offset} diverge from the synthetic payload"
            );
            let chunk_bytes = chunk.bytes.len() as u64;
            staged_bytes += chunk_bytes;
            accounting.staged_chunk_count += 1;
            accounting.total_staged_bytes += chunk_bytes;
            accounting.max_staged_chunk_bytes = accounting.max_staged_chunk_bytes.max(chunk_bytes);
        }
        assert_eq!(
            staged_bytes, in_flight_bytes,
            "every accepted chunk stages exactly the bytes sent"
        );
        accounting.peak_resident_payload_bytes = accounting
            .peak_resident_payload_bytes
            .max(in_flight_bytes + staged_bytes);

        let response = install_snapshot_response_from_outputs(&follower_outputs);
        assert!(response.success);
        assert_eq!(response.transfer_id, Some(directive.transfer_id));
        assert_eq!(response.next_offset, expected_offset);

        if done {
            let applied = follower_outputs
                .iter()
                .find_map(|output| {
                    let Output::ApplySnapshot { snapshot } = output else {
                        return None;
                    };
                    Some(snapshot.clone())
                })
                .expect("the final chunk installs the snapshot");
            drop(follower_outputs);
            let resumed = leader.step(Input::Message {
                from: follower.id(),
                message: Message::InstallSnapshotResponse(response),
            });
            assert!(
                resumed.iter().any(|output| matches!(
                    output,
                    Output::Send {
                        message: Message::AppendEntries(_),
                        ..
                    }
                )),
                "a completed transfer resumes log replication"
            );
            assert!(leader.snapshot_transfer_status().leader.is_empty());
            return (accounting, applied);
        }

        drop(follower_outputs);
        leader_outputs = leader.step(Input::Message {
            from: follower.id(),
            message: Message::InstallSnapshotResponse(response),
        });
    }
}

fn assert_full_transfer_stays_bounded(total_payload_len: u64) {
    let (mut leader, source, snapshot) = streaming_leader(total_payload_len);
    let mut follower = node(2, &[1, 3]);
    let kickoff = start_snapshot_stream(&mut leader);

    let (accounting, applied) =
        pump_snapshot_transfer(&mut leader, &mut follower, &source, kickoff, 0);

    assert_eq!(
        accounting.staged_chunk_count,
        total_payload_len.div_ceil(SNAPSHOT_CHUNK_BYTES)
    );
    assert!(accounting.max_staged_chunk_bytes <= SNAPSHOT_CHUNK_BYTES);
    assert!(
        accounting.peak_resident_payload_bytes <= PEAK_RESIDENT_PAYLOAD_LIMIT_BYTES,
        "peak resident payload of {} bytes exceeds the {} byte bound",
        accounting.peak_resident_payload_bytes,
        PEAK_RESIDENT_PAYLOAD_LIMIT_BYTES
    );
    assert_eq!(accounting.total_staged_bytes, total_payload_len);
    assert_eq!(applied.application_payload_len, total_payload_len);
    assert_eq!(applied.transfer_id(), snapshot.transfer_id());
    assert_eq!(applied.metadata, snapshot.metadata);
    assert!(follower.pending_snapshot_transfer().is_none());
    assert_eq!(follower.snapshot_index(), LogIndex(3));
}

#[test]
fn snapshot_transfer_peak_payload_memory_is_bounded() {
    assert_full_transfer_stays_bounded(256 * 1024 * 1024);
}

#[test]
fn snapshot_transfer_resumes_multi_gigabyte_offset_after_restart() {
    let total_payload_len = 4_800_000_000_u64;
    let received_len = total_payload_len - 5 * SNAPSHOT_CHUNK_BYTES - 12_285;
    assert!(received_len > u64::from(u32::MAX));
    let (mut leader, source, snapshot) = streaming_leader(total_payload_len);
    let mut follower = restarted_follower();
    follower
        .resume_pending_snapshot_transfer(pending_snapshot_transfer(
            NodeId(1),
            &snapshot,
            received_len,
        ))
        .expect("a durable partial transfer resumes after restart");
    assert_eq!(
        follower
            .pending_snapshot_transfer()
            .expect("resumed transfer is pending")
            .received_len,
        received_len
    );

    let kickoff = leader.step(Input::Message {
        from: NodeId(2),
        message: Message::InstallSnapshotResponse(InstallSnapshotResponse {
            term: leader.current_term(),
            follower_id: NodeId(2),
            success: true,
            last_included_index: LogIndex::ZERO,
            transfer_id: Some(snapshot.transfer_id()),
            next_offset: received_len,
        }),
    });

    let (accounting, applied) =
        pump_snapshot_transfer(&mut leader, &mut follower, &source, kickoff, received_len);

    let streamed_len = total_payload_len - received_len;
    assert_eq!(
        accounting.staged_chunk_count,
        streamed_len.div_ceil(SNAPSHOT_CHUNK_BYTES)
    );
    assert_eq!(accounting.total_staged_bytes, streamed_len);
    assert!(accounting.max_staged_chunk_bytes <= SNAPSHOT_CHUNK_BYTES);
    assert!(accounting.peak_resident_payload_bytes <= PEAK_RESIDENT_PAYLOAD_LIMIT_BYTES);
    assert_eq!(applied.application_payload_len, total_payload_len);
    assert_eq!(applied.transfer_id(), snapshot.transfer_id());
    assert!(follower.pending_snapshot_transfer().is_none());
    assert_eq!(follower.snapshot_index(), LogIndex(3));
}

#[test]
fn pending_snapshot_resume_accepts_dynamic_leader_from_snapshot_membership() {
    let payload = b"dynamic snapshot bytes";
    let snapshot = dynamic_snapshot(payload);
    let received_len = 7;
    let mut follower = restarted_dynamic_receiver_without_leader_bootstrap_peer();

    follower
        .resume_pending_snapshot_transfer(pending_snapshot_transfer(
            NodeId(1),
            &snapshot,
            received_len,
        ))
        .expect("snapshot metadata authorizes the dynamic leader");

    let pending = follower
        .pending_snapshot_transfer()
        .expect("dynamic transfer resumes");
    assert_eq!(pending.leader_id, NodeId(1));
    assert_eq!(pending.received_len, received_len);
    assert_eq!(
        pending.metadata.committed_configuration,
        snapshot.metadata.committed_configuration
    );
}

#[test]
fn pending_snapshot_resume_rejects_leader_outside_snapshot_membership() {
    let payload = b"dynamic snapshot bytes";
    let snapshot = dynamic_snapshot(payload);
    let mut follower = restarted_dynamic_receiver_without_leader_bootstrap_peer();

    let error = follower
        .resume_pending_snapshot_transfer(pending_snapshot_transfer(NodeId(9), &snapshot, 7))
        .expect_err("leader outside the snapshot membership is rejected");

    assert_eq!(
        error,
        PendingSnapshotTransferResumeError::LeaderNotAuthorized {
            leader_id: NodeId(9)
        }
    );
    assert!(follower.pending_snapshot_transfer().is_none());
}

#[test]
#[ignore = "nightly-scale: streams a 4.5 GiB synthetic payload end to end"]
fn multi_gigabyte_snapshot_transfer_stays_bounded() {
    let total_payload_len = 4_831_850_496_u64;
    assert!(total_payload_len > u64::from(u32::MAX));
    assert_ne!(total_payload_len % SNAPSHOT_CHUNK_BYTES, 0);
    assert_full_transfer_stays_bounded(total_payload_len);
}
