//! End-to-end reconciliation of claimed and re-derived simulator observations.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::{
    contract::{profile::SimulatorLivenessContract, SimulatorIdentity},
    evidence::{CheckReceipt, ResultBundle},
    verification::AggregateError,
};

use super::{
    contract::{verify_composite_observation, verify_profile_issue_outcome},
    derive_simulator_observation_counts, verify_liveness_observations,
};

pub(crate) fn verify_simulator_observations(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    identity: &SimulatorIdentity,
    liveness_contracts: &[SimulatorLivenessContract],
    events: &BTreeMap<String, Vec<Value>>,
) -> Result<(), AggregateError> {
    if identity.checks.is_empty() {
        return Err(AggregateError::new(format!(
            "simulator receipt {} names no executed model check",
            check.check_id
        )));
    }
    let (mut derived, profile_issue) =
        derive_simulator_observation_counts(bundle, identity, events)?;
    verify_profile_issue_outcome(bundle, check, profile_issue)?;
    verify_liveness_observations(
        bundle,
        check,
        identity,
        liveness_contracts,
        events,
        &mut derived,
    )?;
    verify_composite_observation(bundle, check, identity, events)?;
    let claimed = check
        .observations
        .iter()
        .filter(|(name, _)| name.as_str() != "detector_qualified")
        .map(|(name, value)| (name.clone(), *value))
        .collect::<BTreeMap<_, _>>();
    if claimed != derived {
        return Err(AggregateError::new(format!(
            "simulator receipt observations disagree with logs for {}",
            check.check_id
        )));
    }
    Ok(())
}
