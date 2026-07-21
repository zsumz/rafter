//! Reconciliation of raw liveness reports and receipt bindings.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::{
    contract::{profile::SimulatorLivenessContract, SimulatorIdentity},
    evidence::{CheckReceipt, ResultBundle},
    verification::AggregateError,
};

use super::super::event::execution_is_passing;

pub(crate) fn verify_liveness_observations(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    identity: &SimulatorIdentity,
    liveness_contracts: &[SimulatorLivenessContract],
    events: &BTreeMap<String, Vec<Value>>,
    derived: &mut BTreeMap<String, u64>,
) -> Result<(), AggregateError> {
    if identity.liveness_report.is_none() {
        if check.simulator_liveness.is_some() {
            return Err(AggregateError::new(format!(
                "simulator safety check {} retains a liveness binding",
                check.check_id
            )));
        }
        return Ok(());
    }

    if execution_is_passing(bundle, &check.execution_id) {
        let binding = super::super::derive_verified_liveness_binding(
            &bundle.profile,
            identity,
            liveness_contracts,
            events,
        )
        .map_err(|error| {
            AggregateError::new(format!(
                "simulator raw liveness reports are invalid for {}: {}",
                check.check_id, error.message
            ))
        })?;
        derived.insert(
            identity.required_observation.clone(),
            binding.reports.len() as u64,
        );
        if check.simulator_liveness.as_ref() != Some(&binding) {
            return Err(AggregateError::new(format!(
                "simulator liveness binding disagrees with raw logs for {}",
                check.check_id
            )));
        }
        return Ok(());
    }

    super::super::verify_present_liveness_reports(
        &bundle.profile,
        identity,
        liveness_contracts,
        events,
    )
    .map_err(|error| {
        AggregateError::new(format!(
            "simulator raw liveness reports are invalid for {}: {}",
            check.check_id, error.message
        ))
    })?;
    derived.insert(identity.required_observation.clone(), 0);
    if check.simulator_liveness.is_some() {
        return Err(AggregateError::new(format!(
            "non-passing simulator check {} retains a liveness binding",
            check.check_id
        )));
    }
    Ok(())
}
