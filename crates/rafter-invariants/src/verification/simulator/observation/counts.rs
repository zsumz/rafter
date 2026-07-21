//! Count derivation from structurally valid simulator events.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::{contract::SimulatorIdentity, evidence::ResultBundle, verification::AggregateError};

use super::super::event::{
    merge_raw_issue, verified_passing_simulator_event_contract, RawEventIssue,
};

pub(crate) fn derive_simulator_observation_counts(
    bundle: &ResultBundle,
    identity: &SimulatorIdentity,
    events: &BTreeMap<String, Vec<Value>>,
) -> Result<(BTreeMap<String, u64>, Option<RawEventIssue>), AggregateError> {
    let check_contracts = &bundle
        .execution
        .plan
        .contract
        .runners
        .get("simulator")
        .ok_or_else(|| {
            AggregateError::new("simulator plan omitted its runner contract".to_owned())
        })?
        .simulator_checks;
    let mut derived = BTreeMap::new();
    let mut profile_issue = None;
    for name in &identity.checks {
        let matching = events.get(name).map(Vec::as_slice).unwrap_or_default();
        derived.insert(format!("runs:{name}"), matching.len() as u64);
        derived.insert(
            format!("passes:{name}"),
            matching
                .iter()
                .filter(|event| verified_passing_simulator_event_contract(name, event).is_ok())
                .count() as u64,
        );
        derived.insert(
            format!("steps:{name}"),
            matching
                .iter()
                .filter_map(|event| event["steps"].as_u64())
                .min()
                .unwrap_or_default(),
        );
        if let Some(contract) = check_contracts.get(name) {
            merge_raw_issue(
                &mut profile_issue,
                super::contract::derive_check_contract_issue(
                    name,
                    matching,
                    contract,
                    &mut derived,
                ),
            );
        }
        if identity.liveness_report.is_none() {
            for event in matching {
                if verified_passing_simulator_event_contract(name, event).is_ok() {
                    merge_event_observations(event, &mut derived);
                }
            }
        }
    }
    Ok((derived, profile_issue))
}

fn merge_event_observations(event: &Value, observations: &mut BTreeMap<String, u64>) {
    for field in ["unique_protocol_states", "unique_verifier_states"] {
        if let Some(value) = event[field].as_u64() {
            observations
                .entry(field.to_owned())
                .and_modify(|current| *current = (*current).max(value))
                .or_insert(value);
        }
    }
    if let Some(values) = event["observations"].as_object() {
        for (name, value) in values {
            if let Some(value) = value.as_u64() {
                *observations.entry(name.clone()).or_default() += value;
            }
        }
    }
}
