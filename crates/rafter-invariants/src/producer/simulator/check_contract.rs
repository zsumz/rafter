//! Per-check simulator profile and liveness inventory contract.

use std::{collections::BTreeMap, error::Error};

use serde_json::Value;

use crate::contract::{
    catalog::EvidenceDescriptor,
    profile::{SimulatorCheckContract, SimulatorLivenessContract},
};

use super::{events::passing_simulator_event_contract, issue::SimulatorIssue};

pub(super) fn liveness_contracts(
    descriptors: &[EvidenceDescriptor],
) -> Result<Vec<SimulatorLivenessContract>, Box<dyn Error>> {
    let mut by_feature = BTreeMap::new();
    for contract in descriptors
        .iter()
        .filter_map(|descriptor| descriptor.simulator.as_ref()?.liveness_report.as_ref())
    {
        if let Some(previous) = by_feature.insert(contract.feature_id.clone(), contract.clone()) {
            if previous != *contract {
                return Err(format!(
                    "conflicting simulator liveness contracts for {}",
                    contract.feature_id
                )
                .into());
            }
        }
    }
    Ok(by_feature.into_values().collect())
}

pub(super) fn simulator_check_contract_issue(
    check: &str,
    events: &[Value],
    contract: &SimulatorCheckContract,
    observations: &mut BTreeMap<String, u64>,
) -> Option<SimulatorIssue> {
    let protocol_key = crate::contract::profile::per_check_protocol_states_key(check);
    let verifier_key = crate::contract::profile::per_check_verifier_states_key(check);
    observations.insert(protocol_key, 0);
    observations.insert(verifier_key, 0);
    for observation in &contract.required_observations {
        observations.insert(
            crate::contract::profile::per_check_observation_key(check, observation),
            0,
        );
    }
    let [event] = events else {
        return Some(if events.is_empty() {
            SimulatorIssue::CoverageNotReached(format!(
                "profile contract did not observe simulator check `{check}`"
            ))
        } else {
            SimulatorIssue::HarnessError(format!(
                "profile contract requires exactly one event for simulator check `{check}`, found {}",
                events.len()
            ))
        });
    };
    let protocol_states = event
        .get("unique_protocol_states")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    let verifier_states = event
        .get("unique_verifier_states")
        .and_then(Value::as_u64)
        .unwrap_or_default();
    observations.insert(
        crate::contract::profile::per_check_protocol_states_key(check),
        protocol_states,
    );
    observations.insert(
        crate::contract::profile::per_check_verifier_states_key(check),
        verifier_states,
    );
    for observation in &contract.required_observations {
        observations.insert(
            crate::contract::profile::per_check_observation_key(check, observation),
            event["observations"][observation]
                .as_u64()
                .unwrap_or_default(),
        );
    }
    if event.get("status").and_then(Value::as_str) != Some("pass") {
        return None;
    }
    if passing_simulator_event_contract(check, event).is_err() {
        return Some(SimulatorIssue::HarnessError(format!(
            "simulator check `{check}` has a malformed per-check profile receipt"
        )));
    }
    let missing_observations = contract
        .required_observations
        .iter()
        .filter(|observation| {
            event["observations"][observation.as_str()]
                .as_u64()
                .unwrap_or_default()
                == 0
        })
        .cloned()
        .collect::<Vec<_>>();
    if protocol_states < contract.minimum_protocol_states
        || verifier_states < contract.minimum_verifier_states
        || !missing_observations.is_empty()
    {
        return Some(SimulatorIssue::CoverageNotReached(format!(
            "simulator check `{check}` missed its profile contract: protocol states {protocol_states}/{}, verifier states {verifier_states}/{}, missing observations [{}]",
            contract.minimum_protocol_states,
            contract.minimum_verifier_states,
            missing_observations.join(", ")
        )));
    }
    None
}
