//! The harness's scripted membership change.
//!
//! A plan names one target voter set, and the next drive action is derived
//! only from what the replica currently reports, so a transition is proposed
//! only when the effective and committed configurations agree it is next.
//! Nothing here performs the change; the kernel still judges every transition.

use std::{collections::BTreeMap, error::Error};

use rafter::{MembershipConfig, MembershipSet, NodeId};

const MEMBERSHIP_PLAN_ENV: &str = "RAFTER_MAELSTROM_MEMBERSHIP_PLAN";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MembershipPlan {
    Disabled,
    RemoveLastVoter,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MembershipDriveAction {
    EnterJoint,
    LeaveJoint,
    Wait,
    Complete,
}

pub(crate) fn membership_plan_from_env() -> Result<MembershipPlan, Box<dyn Error>> {
    match std::env::var(MEMBERSHIP_PLAN_ENV) {
        Ok(raw) => match raw.as_str() {
            "" | "none" | "off" | "disabled" => Ok(MembershipPlan::Disabled),
            "remove-last-voter" | "remove_last_voter" => Ok(MembershipPlan::RemoveLastVoter),
            other => Err(format!(
                "{MEMBERSHIP_PLAN_ENV} must be one of none, disabled, or remove-last-voter; got {other:?}"
            )
            .into()),
        },
        Err(std::env::VarError::NotPresent) => Ok(MembershipPlan::Disabled),
        Err(error) => Err(format!("{MEMBERSHIP_PLAN_ENV} is not valid UTF-8: {error}").into()),
    }
}

pub(crate) fn membership_target_for_plan(
    plan: MembershipPlan,
    name_to_id: &BTreeMap<String, NodeId>,
) -> Result<Option<MembershipSet>, Box<dyn Error>> {
    match plan {
        MembershipPlan::Disabled => Ok(None),
        MembershipPlan::RemoveLastVoter => {
            let removed = name_to_id
                .values()
                .copied()
                .max_by_key(|node_id| node_id.0)
                .ok_or("membership plan requires at least one node")?;
            let voters = name_to_id
                .values()
                .copied()
                .filter(|node_id| *node_id != removed)
                .collect::<Vec<_>>();
            if voters.is_empty() {
                return Err("remove-last-voter would leave no voters".into());
            }
            Ok(Some(MembershipSet::new(voters, Vec::new())?))
        }
    }
}

pub(crate) fn membership_drive_action(
    effective: &MembershipConfig,
    committed: &MembershipConfig,
    target: &MembershipSet,
) -> MembershipDriveAction {
    if stable_membership_matches(effective, target) && stable_membership_matches(committed, target)
    {
        return MembershipDriveAction::Complete;
    }

    match effective {
        MembershipConfig::Stable(current) if current != target && committed == effective => {
            MembershipDriveAction::EnterJoint
        }
        MembershipConfig::Joint(joint)
            if joint.new_membership() == target && committed == effective =>
        {
            MembershipDriveAction::LeaveJoint
        }
        MembershipConfig::Stable(_) | MembershipConfig::Joint(_) => MembershipDriveAction::Wait,
    }
}

fn stable_membership_matches(config: &MembershipConfig, target: &MembershipSet) -> bool {
    matches!(config, MembershipConfig::Stable(membership) if membership == target)
}

#[cfg(test)]
#[path = "membership_test.rs"]
mod tests;
