use std::{collections::BTreeMap, fs, path::Path};

use serde_json::Value;

use crate::{aggregate::AggregateError, ResultBundle};

use super::{
    compile_test::{is_passing, require_exact_test_pass, verify_test_invocations},
    simulator_schedule::verify_simulator_schedule,
    EVENT_PREFIX,
};

pub(super) fn verify_simulator_logs(
    bundle: &ResultBundle,
    root: &Path,
) -> Result<(), AggregateError> {
    verify_simulator_schedule(bundle, root)?;
    let events = simulator_events(bundle, root)?;
    let catalog =
        crate::Catalog::load(root.join(&bundle.execution.plan.registry.path).as_path())
            .map_err(|error| AggregateError::new(format!("reload simulator registry: {error}")))?;
    let descriptors = catalog
        .evidence
        .iter()
        .map(|descriptor| (descriptor.evidence_id(), descriptor))
        .collect::<BTreeMap<_, _>>();
    let liveness_contracts = catalog
        .evidence
        .iter()
        .filter_map(|descriptor| {
            descriptor
                .simulator
                .as_ref()?
                .liveness_report
                .as_ref()
                .map(|contract| (contract.feature_id.clone(), contract.clone()))
        })
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect::<Vec<_>>();
    let mut test_logs = BTreeMap::<String, String>::new();
    for check in &bundle.execution.checks {
        let [evidence_id] = check.evidence_ids.as_slice() else {
            return Err(AggregateError::new(format!(
                "simulator check {} must bind exactly one evidence record",
                check.check_id
            )));
        };
        let descriptor = descriptors.get(evidence_id).ok_or_else(|| {
            AggregateError::new(format!(
                "simulator check {} names unknown evidence {evidence_id}",
                check.check_id
            ))
        })?;
        let identity = descriptor.simulator.as_ref().ok_or_else(|| {
            AggregateError::new(format!(
                "simulator check {} names non-simulator evidence",
                check.check_id
            ))
        })?;
        verify_simulator_observations(bundle, check, identity, &liveness_contracts, &events)?;
        if !is_passing(bundle, &check.execution_id) {
            continue;
        }
        let Some((_, fixture)) = check.evidence_ids[0].rsplit_once('@') else {
            continue;
        };
        let artifact = check
            .artifacts
            .iter()
            .find(|artifact| artifact.kind == "test-log")
            .ok_or_else(|| {
                AggregateError::new(format!("detector log missing for {}", check.check_id))
            })?;
        let source = if let Some(source) = test_logs.get(&artifact.path) {
            source.clone()
        } else {
            let source = fs::read_to_string(root.join(&artifact.path)).map_err(|error| {
                AggregateError::new(format!("read detector log {}: {error}", artifact.path))
            })?;
            test_logs.insert(artifact.path.clone(), source.clone());
            source
        };
        verify_test_invocations(bundle, check, &source, fixture, root)?;
        require_exact_test_pass(&source, fixture, &check.check_id)?;
    }
    Ok(())
}

fn simulator_events(
    bundle: &ResultBundle,
    root: &Path,
) -> Result<BTreeMap<String, Vec<Value>>, AggregateError> {
    let logs = bundle
        .execution
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == "simulator-log")
        .collect::<Vec<_>>();
    if logs.is_empty() {
        return Err(AggregateError::new(
            "simulator execution has no machine-readable logs".to_owned(),
        ));
    }
    let mut events = BTreeMap::<String, Vec<Value>>::new();
    for log in logs {
        let source = fs::read_to_string(root.join(&log.path)).map_err(|error| {
            AggregateError::new(format!("read simulator log {}: {error}", log.path))
        })?;
        for line in source
            .lines()
            .filter_map(|line| line.strip_prefix(EVENT_PREFIX))
        {
            let event: Value = serde_json::from_str(line).map_err(|error| {
                AggregateError::new(format!("parse simulator event in {}: {error}", log.path))
            })?;
            let check_id = event["check_id"].as_str().ok_or_else(|| {
                AggregateError::new(format!("simulator event in {} lacks check_id", log.path))
            })?;
            events
                .entry(check_id.to_owned())
                .or_default()
                .push(event.clone());
            if let Some(canonical) = crate::producer::canonical_check_id(&bundle.profile, check_id)
            {
                events.entry(canonical).or_default().push(event);
            }
        }
    }
    Ok(events)
}

pub(super) fn verify_simulator_observations(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    identity: &crate::SimulatorIdentity,
    liveness_contracts: &[crate::types::SimulatorLivenessContract],
    events: &BTreeMap<String, Vec<Value>>,
) -> Result<(), AggregateError> {
    if identity.checks.is_empty() {
        return Err(AggregateError::new(format!(
            "simulator receipt {} names no executed model check",
            check.check_id
        )));
    }
    let mut derived = BTreeMap::new();
    for name in &identity.checks {
        let matching = events.get(name).map(Vec::as_slice).unwrap_or_default();
        derived.insert(format!("runs:{name}"), matching.len() as u64);
        derived.insert(
            format!("passes:{name}"),
            matching
                .iter()
                .filter(|event| event["status"] == "pass")
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
        if identity.liveness_report.is_none() {
            for event in matching {
                merge_event_observations(event, &mut derived);
            }
        }
    }
    if identity.liveness_report.is_some() {
        if is_passing(bundle, &check.execution_id) {
            let binding = crate::catalog::derive_liveness_binding(
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
        } else {
            derived.insert(identity.required_observation.clone(), 0);
            if check.simulator_liveness.is_some() {
                return Err(AggregateError::new(format!(
                    "non-passing simulator check {} retains a liveness binding",
                    check.check_id
                )));
            }
        }
    } else if check.simulator_liveness.is_some() {
        return Err(AggregateError::new(format!(
            "simulator safety check {} retains a liveness binding",
            check.check_id
        )));
    }
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
