//! Semantic simulator observation extraction and coverage contract.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::{
    contract::{
        profile::{SimulatorCheckContract, SimulatorLivenessContract},
        SimulatorIdentity,
    },
    evidence::SimulatorLivenessBinding,
};

use super::{
    check_contract::simulator_check_contract_issue,
    events::{passing_simulator_event_contract, simulator_event_issue},
    issue::{merge_issue, SimulatorIssue},
    liveness,
};

pub(super) struct ModelEvidence {
    pub(super) observations: BTreeMap<String, u64>,
    pub(super) per_check_required_observations: BTreeMap<String, u64>,
    pub(super) simulator_liveness: Option<SimulatorLivenessBinding>,
    pub(super) issue: Option<SimulatorIssue>,
}

pub(super) fn model_observations(
    profile: &str,
    invariant_id: &str,
    identity: &SimulatorIdentity,
    check_contracts: &BTreeMap<String, SimulatorCheckContract>,
    liveness_contracts: &[SimulatorLivenessContract],
    events: &BTreeMap<String, Vec<Value>>,
) -> ModelEvidence {
    let mut observations = BTreeMap::new();
    let mut per_check_required_observations = BTreeMap::new();
    let mut issue = None;
    for check in &identity.checks {
        let matching = events.get(check).map(Vec::as_slice).unwrap_or_default();
        per_check_required_observations.insert(
            check.clone(),
            matching
                .iter()
                .filter(|event| passing_simulator_event_contract(check, event).is_ok())
                .filter_map(|event| event["observations"][&identity.required_observation].as_u64())
                .sum(),
        );
        observations.insert(format!("runs:{check}"), matching.len() as u64);
        observations.insert(
            format!("passes:{check}"),
            matching
                .iter()
                .filter(|event| passing_simulator_event_contract(check, event).is_ok())
                .count() as u64,
        );
        let minimum_steps = matching
            .iter()
            .filter_map(|event| event["steps"].as_u64())
            .min()
            .unwrap_or_default();
        observations.insert(format!("steps:{check}"), minimum_steps);
        if let Some(contract) = check_contracts.get(check) {
            merge_issue(
                &mut issue,
                simulator_check_contract_issue(check, matching, contract, &mut observations),
            );
        }
        for event in matching {
            merge_issue(
                &mut issue,
                simulator_event_issue(check, invariant_id, event),
            );
            if identity.liveness_report.is_none()
                && passing_simulator_event_contract(check, event).is_ok()
            {
                merge_event_observations(event, &mut observations);
            }
        }
    }
    if identity.liveness_report.is_none() {
        return ModelEvidence {
            observations,
            per_check_required_observations,
            simulator_liveness: None,
            issue,
        };
    }
    observations.insert(identity.required_observation.clone(), 0);
    let simulator_liveness = if issue.is_none() {
        match liveness::derive_liveness_binding(profile, identity, liveness_contracts, events) {
            Ok(binding) => {
                observations.insert(
                    identity.required_observation.clone(),
                    binding.reports.len() as u64,
                );
                Some(binding)
            }
            Err(error) => {
                issue = Some(match error.kind {
                    liveness::LivenessReportErrorKind::Missing => {
                        SimulatorIssue::CoverageNotReached(error.message)
                    }
                    liveness::LivenessReportErrorKind::Malformed => {
                        SimulatorIssue::HarnessError(error.message)
                    }
                });
                None
            }
        }
    } else {
        None
    };
    ModelEvidence {
        observations,
        per_check_required_observations,
        simulator_liveness,
        issue,
    }
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

pub(super) fn coverage_reached(
    identity: &SimulatorIdentity,
    observations: &BTreeMap<String, u64>,
    per_check_required_observations: &BTreeMap<String, u64>,
) -> bool {
    let witness = observations
        .get(&identity.required_observation)
        .copied()
        .unwrap_or_default()
        >= identity.minimum_observation as u64;
    if let Some(contract) = &identity.liveness_report {
        return witness
            && identity.checks.iter().all(|check| {
                let required_runs = identity.minimum_runs_per_check.unwrap_or_default() as u64;
                observations
                    .get(&format!("runs:{check}"))
                    .copied()
                    .unwrap_or_default()
                    >= required_runs
                    && observations
                        .get(&format!("passes:{check}"))
                        .copied()
                        .unwrap_or_default()
                        >= required_runs
                    && observations
                        .get(&format!("steps:{check}"))
                        .copied()
                        .unwrap_or_default()
                        >= identity.minimum_steps.unwrap_or_default() as u64
            })
            && !contract.feature_id.is_empty();
    }
    witness
        && identity.checks.iter().any(|check| {
            per_check_required_observations
                .get(check)
                .copied()
                .unwrap_or_default()
                >= identity.minimum_observation as u64
        })
        && identity.checks.iter().all(|check| {
            observations
                .get(&format!("passes:{check}"))
                .copied()
                .unwrap_or_default()
                >= 1
        })
        && observations
            .get("unique_protocol_states")
            .copied()
            .unwrap_or_default()
            >= identity.minimum_protocol_states.unwrap_or_default() as u64
        && observations
            .get("unique_verifier_states")
            .copied()
            .unwrap_or_default()
            >= identity.minimum_verifier_states.unwrap_or_default() as u64
}
