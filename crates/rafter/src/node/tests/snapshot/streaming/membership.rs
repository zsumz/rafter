//! Sender authorization when a durable transfer is resumed after restart.
//!
//! Resume reuses the live receive path's header validation, so it pins the
//! same sender rule: a leader is authorized by a membership this node can see
//! *now*, and never has to appear in the historical boundary it is relaying.

use super::super::support::test_snapshot_with_committed_voters;
use super::support::*;
use super::*;

/// A restarted follower whose bootstrap configuration already names node 4.
fn restarted_receiver_knowing_node_four() -> Node {
    Node::from_bootstrap(
        NodeConfig::new(NodeId(2), vec![NodeId(1), NodeId(3), NodeId(4)], 3)
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
    .expect("receiver hydrates from persisted hard state")
}

/// The wedge, inverted. A leader that merely *installed* an older snapshot
/// holds a descriptor whose boundary membership predates its own admission. It
/// must still be able to relay it, because the leader-side response path rewinds
/// a rejected transfer to offset zero and restreams it with no give-up.
#[test]
fn pending_snapshot_resume_accepts_a_leader_absent_from_the_snapshot_boundary() {
    let payload = b"older boundary snapshot";
    let snapshot = test_snapshot_with_committed_voters(3, 4, 5, payload, &[1, 2, 3]);
    let mut follower = restarted_receiver_knowing_node_four();

    follower
        .resume_pending_snapshot_transfer(pending_snapshot_transfer(NodeId(4), &snapshot, 7))
        .expect("a current member may relay a boundary that predates it");

    let pending = follower
        .pending_snapshot_transfer()
        .expect("the transfer resumes");
    assert_eq!(pending.leader_id, NodeId(4));
    assert_eq!(pending.received_len, 7);
}

/// The joining replica's case, unchanged. A node whose bootstrap peers predate
/// the current leader has no current membership that names it — and the log
/// that would say so is exactly the log this snapshot replaces — so the
/// descriptor's own boundary membership is the one thing it can recognize the
/// sender from.
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

/// A leader in neither membership this node can see is still refused: relaxing
/// the boundary requirement removed a rule, not the check.
#[test]
fn pending_snapshot_resume_rejects_leader_outside_every_visible_membership() {
    let payload = b"dynamic snapshot bytes";
    let snapshot = dynamic_snapshot(payload);
    let mut follower = restarted_dynamic_receiver_without_leader_bootstrap_peer();

    let error = follower
        .resume_pending_snapshot_transfer(pending_snapshot_transfer(NodeId(9), &snapshot, 7))
        .expect_err("leader outside every visible membership is rejected");

    assert_eq!(
        error,
        PendingSnapshotTransferResumeError::LeaderNotAuthorized {
            leader_id: NodeId(9)
        }
    );
    assert!(follower.pending_snapshot_transfer().is_none());
}
