//! Target process-group receipt and ownership-transfer scenarios.

use super::super::super::{
    parse_target_group_frame, validate_ready_target_group_with,
    validate_target_group_candidate_with, TargetGroupFrame,
};

#[test]
fn process_group_receipt_requires_complete_frames() {
    assert_eq!(
        parse_target_group_frame("41").expect("partial PID remains pending"),
        TargetGroupFrame::Pending
    );
    assert_eq!(
        parse_target_group_frame("41\n").expect("complete PID frame"),
        TargetGroupFrame::Planned(41)
    );
    assert_eq!(
        parse_target_group_frame("41\nready").expect("partial readiness remains pending"),
        TargetGroupFrame::Planned(41)
    );
    assert_eq!(
        parse_target_group_frame("41\nready\n").expect("complete readiness frame"),
        TargetGroupFrame::Ready(41)
    );
    assert!(parse_target_group_frame("4x\n").is_err());
    assert!(parse_target_group_frame("41\nready\nextra\n").is_err());
}

#[test]
fn process_group_receipt_rejects_foreign_processes_before_acknowledgement() {
    let foreign = validate_target_group_candidate_with(41, 40, |_| Ok(99))
        .expect_err("a foreign PID cannot transfer target-group ownership");
    assert!(foreign.to_string().contains("expected wrapper group 40"));

    let wrapper = validate_target_group_candidate_with(40, 40, |_| Ok(40))
        .expect_err("the wrapper PID cannot masquerade as its target");
    assert!(wrapper.to_string().contains("wrapper process group"));

    validate_target_group_candidate_with(41, 40, |_| Ok(40))
        .expect("a child still in the wrapper group can transfer ownership");
}

#[test]
fn ready_process_group_requires_the_published_process_in_the_anchored_group() {
    validate_ready_target_group_with(41, 40, |_| Ok(40))
        .expect("ready launcher remains in the anchored wrapper group");
    let error = validate_ready_target_group_with(41, 40, |_| Ok(7))
        .expect_err("a ready frame cannot promote a foreign process group");
    assert!(error.to_string().contains(
        "ready target process 41 belongs to process group 7, expected anchored group 40"
    ));
}
