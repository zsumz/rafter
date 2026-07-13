//! Bounded resident memory and restart from offsets beyond 32 bits.

use super::support::*;
use super::*;

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
