#![allow(clippy::wildcard_imports)]

mod support;

use support::*;

#[test]
fn in_memory_driver_transfer_success_is_request_level() {
    let driver = elected_driver();
    let handle = driver.handle();

    block_on(handle.transfer_leadership(NodeId(2))).expect("transfer request accepted");

    // Request-level success does not assert that the target is now the
    // observed leader. Callers that need that condition should watch metrics
    // under their own deadline.
    assert_eq!(
        handle.metrics().expect("metrics").current().node_id,
        NodeId(1)
    );
}

#[test]
fn in_memory_driver_reports_self_transfer_rejection() {
    let driver = elected_driver();
    let handle = driver.handle();

    let error = block_on(handle.transfer_leadership(NodeId(1)))
        .expect_err("a leader cannot transfer to itself");

    assert!(
        matches!(
            error,
            TransferLeadershipError::Rejected {
                reason: LeadershipTransferRejection::TargetIsSelf,
                leader_hint: Some(NodeId(1)),
            }
        ),
        "got {error:?}"
    );
}

#[test]
fn in_memory_driver_reports_non_voter_transfer_rejection() {
    let driver = elected_driver();
    let handle = driver.handle();

    let error = block_on(handle.transfer_leadership(NodeId(99)))
        .expect_err("a non-voter cannot receive leadership");

    assert!(
        matches!(
            error,
            TransferLeadershipError::Rejected {
                reason: LeadershipTransferRejection::TargetNotVoter,
                leader_hint: Some(NodeId(1)),
            }
        ),
        "got {error:?}"
    );
}

#[test]
fn in_memory_driver_reports_non_leader_transfer_rejection() {
    let driver = KvDriver::new(NodeId(1), groups()).expect("driver builds");
    let handle = driver.handle();

    let error = block_on(handle.transfer_leadership(NodeId(2)))
        .expect_err("a follower cannot hand leadership on");

    assert!(
        matches!(
            error,
            TransferLeadershipError::Rejected {
                reason: LeadershipTransferRejection::NotLeader,
                leader_hint: None,
            }
        ),
        "got {error:?}"
    );
}
