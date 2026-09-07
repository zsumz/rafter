//! Acceptance of target quiescence from lease and member observations.
//!
//! Quiescence needs the lifetime lease released, the wrapper exited, and the
//! observer reporting no live members; any pair of observations that cannot
//! both be true is reported as an observer fault rather than a quiet state.

use super::{TargetLeaseState, TargetMemberState};

pub(super) fn classify_target_quiescence(
    lease_before: TargetLeaseState,
    lease_after: TargetLeaseState,
    wrapper_exited: bool,
    target_members: TargetMemberState,
) -> Result<bool, Box<dyn std::error::Error>> {
    match (lease_before, lease_after) {
        (TargetLeaseState::Released, TargetLeaseState::Held) => {
            Err("target lifetime lease returned from EOF to a held state".into())
        }
        (TargetLeaseState::Released, TargetLeaseState::Released)
            if target_members == TargetMemberState::Live =>
        {
            Err(
                "target lifetime lease was released while the process observer reported live target members"
                    .into(),
            )
        }
        (TargetLeaseState::Held, TargetLeaseState::Held)
            if wrapper_exited && target_members == TargetMemberState::Quiescent =>
        {
            Err(
                "process observer omitted live target members while the target lifetime lease remained held after wrapper exit"
                    .into(),
            )
        }
        (TargetLeaseState::Released, TargetLeaseState::Released)
            if wrapper_exited && target_members == TargetMemberState::Quiescent =>
        {
            Ok(true)
        }
        (
            TargetLeaseState::Held,
            TargetLeaseState::Held | TargetLeaseState::Released,
        )
        | (
            TargetLeaseState::Released,
            TargetLeaseState::Released,
        ) => Ok(false),
    }
}

#[cfg(test)]
pub(crate) fn classify_target_quiescence_for_test(
    lease_before: TargetLeaseState,
    lease_after: TargetLeaseState,
    wrapper_exited: bool,
    target_members: TargetMemberState,
) -> Result<bool, Box<dyn std::error::Error>> {
    classify_target_quiescence(lease_before, lease_after, wrapper_exited, target_members)
}
