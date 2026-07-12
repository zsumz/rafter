//! Dynamic snapshot-boundary authorization during transfer resume.

use super::support::*;
use super::*;

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
