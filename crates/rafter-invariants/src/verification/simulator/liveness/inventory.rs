//! Verifier-owned report inventory derived from the execution contract.

use std::collections::{BTreeMap, BTreeSet};

use super::error::{malformed, LivenessReportError};
use crate::contract::profile::{SimulatorExecutionContract, SimulatorLivenessContract};

pub(super) fn expected_report_contracts<'a>(
    available: &'a [SimulatorLivenessContract],
    execution: &SimulatorExecutionContract,
) -> Result<BTreeMap<String, &'a SimulatorLivenessContract>, LivenessReportError> {
    let mut expected = BTreeMap::new();
    for contract in available {
        let required = feature_is_required(&contract.feature_id, execution)?;
        if required {
            if let Some(previous) = expected.insert(contract.feature_id.clone(), contract) {
                if previous != contract {
                    return Err(malformed(format!(
                        "registry contains conflicting liveness contracts for `{}`",
                        contract.feature_id
                    )));
                }
            }
        }
    }
    if expected.keys().map(String::as_str).collect::<BTreeSet<_>>() != required_features(execution)
    {
        return Err(malformed(
            "registry does not define the complete expected liveness feature set",
        ));
    }
    Ok(expected)
}

fn feature_is_required(
    feature: &str,
    execution: &SimulatorExecutionContract,
) -> Result<bool, LivenessReportError> {
    match feature {
        "leader-convergence"
        | "leader-usability"
        | "quorum-only-leader-convergence"
        | "quorum-only-leader-usability"
        | "proposal-progress"
        | "proposal-termination" => Ok(true),
        "read-barrier" => Ok(execution.max_read_indexes > 0),
        "membership-transition" => Ok(execution.max_membership_changes > 0),
        "leadership-transfer" => Ok(execution.max_transfers > 0),
        "snapshot-catch-up" => Ok(execution.snapshot_catchup_probe),
        _ => Err(malformed(format!(
            "registry contains unknown liveness feature `{feature}`"
        ))),
    }
}

fn required_features(execution: &SimulatorExecutionContract) -> BTreeSet<&'static str> {
    let mut required = BTreeSet::from([
        "leader-convergence",
        "leader-usability",
        "quorum-only-leader-convergence",
        "quorum-only-leader-usability",
        "proposal-progress",
        "proposal-termination",
    ]);
    if execution.max_read_indexes > 0 {
        required.insert("read-barrier");
    }
    if execution.max_membership_changes > 0 {
        required.insert("membership-transition");
    }
    if execution.max_transfers > 0 {
        required.insert("leadership-transfer");
    }
    if execution.snapshot_catchup_probe {
        required.insert("snapshot-catch-up");
    }
    required
}
