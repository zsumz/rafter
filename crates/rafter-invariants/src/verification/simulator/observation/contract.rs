//! Profile-floor and composite-witness observation contracts.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::{
    contract::{profile::SimulatorCheckContract, SimulatorIdentity},
    evidence::{CheckReceipt, ResultBundle},
    verification::AggregateError,
};

use super::super::event::{
    execution_is_passing, receipt_issue, verified_passing_simulator_event_contract, RawEventIssue,
};

pub(crate) fn verify_profile_issue_outcome(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    profile_issue: Option<RawEventIssue>,
) -> Result<(), AggregateError> {
    let Some(expected) = profile_issue else {
        return Ok(());
    };
    let outcomes = bundle
        .results
        .iter()
        .filter(|result| result.execution_id == check.execution_id)
        .map(|result| (result.status, result.classification))
        .collect::<Vec<_>>();
    if outcomes.is_empty()
        || outcomes.iter().any(|outcome| {
            receipt_issue(*outcome).is_none_or(|issue| issue.rank() < expected.rank())
        })
    {
        return Err(AggregateError::new(format!(
            "simulator check {} receipt downgrades its per-check profile failure",
            check.check_id
        )));
    }
    Ok(())
}

pub(crate) fn verify_composite_observation(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    identity: &SimulatorIdentity,
    events: &BTreeMap<String, Vec<Value>>,
) -> Result<(), AggregateError> {
    if identity.liveness_report.is_some() || !execution_is_passing(bundle, &check.execution_id) {
        return Ok(());
    }
    let independently_reached = identity.checks.iter().any(|name| {
        events
            .get(name)
            .map(Vec::as_slice)
            .unwrap_or_default()
            .iter()
            .filter(|event| verified_passing_simulator_event_contract(name, event).is_ok())
            .filter_map(|event| event["observations"][&identity.required_observation].as_u64())
            .sum::<u64>()
            >= identity.minimum_observation as u64
    });
    if !independently_reached {
        return Err(AggregateError::new(format!(
            "simulator check {} claims passing composite evidence, but no model check independently reached observation {}",
            check.check_id, identity.required_observation
        )));
    }
    Ok(())
}

pub(crate) fn derive_check_contract_issue(
    check: &str,
    events: &[Value],
    contract: &SimulatorCheckContract,
    observations: &mut BTreeMap<String, u64>,
) -> Option<RawEventIssue> {
    observations.insert(
        crate::contract::profile::per_check_protocol_states_key(check),
        0,
    );
    observations.insert(
        crate::contract::profile::per_check_verifier_states_key(check),
        0,
    );
    for observation in &contract.required_observations {
        observations.insert(
            crate::contract::profile::per_check_observation_key(check, observation),
            0,
        );
    }
    let [event] = events else {
        return Some(if events.is_empty() {
            RawEventIssue::CoverageNotReached
        } else {
            RawEventIssue::HarnessError
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
    if verified_passing_simulator_event_contract(check, event).is_err() {
        return Some(RawEventIssue::HarnessError);
    }
    let observations_reached = contract.required_observations.iter().all(|observation| {
        event["observations"][observation]
            .as_u64()
            .unwrap_or_default()
            > 0
    });
    (protocol_states < contract.minimum_protocol_states
        || verifier_states < contract.minimum_verifier_states
        || !observations_reached)
        .then_some(RawEventIssue::CoverageNotReached)
}
