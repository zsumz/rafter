//! Artifact-level reconciliation of raw liveness events and receipt bindings.

use std::collections::BTreeMap;

use serde_json::Value;

use super::super::test_logs::is_passing;
use crate::{verification::AggregateError, ResultBundle};

pub(in crate::artifact_verify) fn verify_liveness_observations(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    identity: &crate::SimulatorIdentity,
    liveness_contracts: &[crate::contract::profile::SimulatorLivenessContract],
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

    if is_passing(bundle, &check.execution_id) {
        let binding = crate::verification::simulator::derive_verified_liveness_binding(
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

    crate::verification::simulator::verify_present_liveness_reports(
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
