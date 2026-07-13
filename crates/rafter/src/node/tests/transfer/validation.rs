//! Role and target validation for leadership-transfer requests.

use super::support::*;

#[test]
fn transfer_rejections_cover_non_leader_self_and_non_voter() {
    let mut follower = node(2, &[1, 3]);
    let outputs = follower.step(Input::TransferLeadership { target: NodeId(1) });
    assert!(matches!(
        outputs.as_slice(),
        [Output::LeadershipTransferRejected {
            reason: LeadershipTransferRejection::NotLeader,
            ..
        }]
    ));

    let mut leader = leader_with_acknowledged_follower();
    let outputs = leader.step(Input::TransferLeadership { target: NodeId(1) });
    assert!(matches!(
        outputs.as_slice(),
        [Output::LeadershipTransferRejected {
            reason: LeadershipTransferRejection::TargetIsSelf,
            ..
        }]
    ));

    let outputs = leader.step(Input::TransferLeadership { target: NodeId(9) });
    assert!(matches!(
        outputs.as_slice(),
        [Output::LeadershipTransferRejected {
            reason: LeadershipTransferRejection::TargetNotVoter,
            ..
        }]
    ));
}
