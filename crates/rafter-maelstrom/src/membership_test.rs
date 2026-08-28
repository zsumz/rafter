//! The harness's scripted membership target and the actions it drives.
//!
//! Removing the last voter must drop the highest protocol identity, and the
//! drive actions must follow the stable, joint, stable order, so a transition
//! is proposed only when the reported configurations agree it is next.

use super::*;

#[test]
fn remove_last_voter_target_drops_highest_protocol_id() {
    let map = BTreeMap::from([
        ("n0".to_string(), NodeId(1)),
        ("n1".to_string(), NodeId(2)),
        ("n2".to_string(), NodeId(3)),
        ("n3".to_string(), NodeId(4)),
    ]);

    let target = membership_target_for_plan(MembershipPlan::RemoveLastVoter, &map)
        .expect("membership target builds")
        .expect("plan has target");

    assert_eq!(target, membership(&[1, 2, 3]));
}

#[test]
fn drive_actions_follow_stable_joint_stable_transition() {
    let old = membership(&[1, 2, 3, 4]);
    let target = membership(&[1, 2, 3]);
    let stable_old = MembershipConfig::stable(old.clone());
    let stable_target = MembershipConfig::stable(target.clone());
    let joint_target =
        MembershipConfig::Joint(rafter::JointMembership::new(old.clone(), target.clone()));

    assert_eq!(
        membership_drive_action(&stable_old, &stable_old, &target),
        MembershipDriveAction::EnterJoint
    );
    assert_eq!(
        membership_drive_action(&joint_target, &stable_old, &target),
        MembershipDriveAction::Wait
    );
    assert_eq!(
        membership_drive_action(&joint_target, &joint_target, &target),
        MembershipDriveAction::LeaveJoint
    );
    assert_eq!(
        membership_drive_action(&stable_target, &joint_target, &target),
        MembershipDriveAction::Wait
    );
    assert_eq!(
        membership_drive_action(&stable_target, &stable_target, &target),
        MembershipDriveAction::Complete
    );
}

fn membership(voters: &[u64]) -> MembershipSet {
    MembershipSet::new(
        voters.iter().copied().map(NodeId).collect::<Vec<_>>(),
        Vec::new(),
    )
    .expect("membership is valid")
}
