use std::{collections::BTreeMap, fs, path::Path};

use serde_json::Value;

use crate::{aggregate::AggregateError, ResultBundle};

use super::{
    simulator_schedule::verify_simulator_schedule,
    test_logs::{is_passing, require_exact_test_pass, verify_test_invocations},
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
        verify_nonpassing_event_classification(bundle, check, identity, &events)?;
        verify_simulator_observations(bundle, check, identity, &liveness_contracts, &events)?;
        if !is_passing(bundle, &check.execution_id) {
            continue;
        }
        let Some(negative_test) = identity.negative_test.as_ref() else {
            continue;
        };
        let fixture = descriptor.negative_fixture.as_deref().ok_or_else(|| {
            AggregateError::new(format!(
                "simulator check {} has a registered negative test without a fixture",
                check.check_id
            ))
        })?;
        if negative_test.test_name.rsplit("::").next() != Some(fixture) {
            return Err(AggregateError::new(format!(
                "simulator check {} fixture does not match registered test identity {}",
                check.check_id, negative_test.test_name
            )));
        }
        verify_negative_fixture_binding(root, descriptor, fixture, &check.check_id)?;
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
        verify_test_invocations(
            bundle,
            check,
            &source,
            &negative_test.test_name,
            &negative_test.check_id(),
            root,
        )?;
        require_exact_test_pass(&source, &negative_test.test_name, &check.check_id)?;
    }
    Ok(())
}

fn verify_nonpassing_event_classification(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    identity: &crate::SimulatorIdentity,
    events: &BTreeMap<String, Vec<Value>>,
) -> Result<(), AggregateError> {
    let mut expected = None;
    for event in identity
        .checks
        .iter()
        .flat_map(|name| events.get(name).into_iter().flatten())
        .filter(|event| event["status"] != "pass")
    {
        let candidate = match event.get("classification").and_then(Value::as_str) {
            Some("invariant-violation") => (
                crate::EvidenceStatus::Fail,
                crate::FailureClassification::InvariantViolation,
                3,
            ),
            Some("harness-error") => (
                crate::EvidenceStatus::Error,
                crate::FailureClassification::HarnessError,
                2,
            ),
            Some("coverage-not-reached") => (
                crate::EvidenceStatus::Incomplete,
                crate::FailureClassification::CoverageNotReached,
                1,
            ),
            _ => {
                return Err(AggregateError::new(format!(
                    "simulator check {} has an invalid raw failure classification",
                    check.check_id
                )))
            }
        };
        if expected.is_none_or(|(_, _, rank)| candidate.2 > rank) {
            expected = Some(candidate);
        }
    }
    let Some((expected_status, expected_classification, _)) = expected else {
        return Ok(());
    };
    let outcomes = bundle
        .results
        .iter()
        .filter(|result| result.execution_id == check.execution_id)
        .map(|result| (result.status, result.classification))
        .collect::<Vec<_>>();
    if outcomes.is_empty()
        || outcomes
            .iter()
            .any(|outcome| *outcome != (expected_status, Some(expected_classification)))
    {
        return Err(AggregateError::new(format!(
            "simulator check {} receipt does not preserve its raw semantic failure classification",
            check.check_id
        )));
    }
    Ok(())
}

fn verify_negative_fixture_binding(
    root: &Path,
    descriptor: &crate::EvidenceDescriptor,
    fixture: &str,
    check_id: &str,
) -> Result<(), AggregateError> {
    let fixture_path = descriptor.negative_fixture_path.as_deref().ok_or_else(|| {
        AggregateError::new(format!(
            "simulator check {check_id} has no registered negative fixture path"
        ))
    })?;
    let detector = descriptor
        .negative_fixture_detector
        .as_deref()
        .ok_or_else(|| {
            AggregateError::new(format!(
                "simulator check {check_id} has no registered detector identity"
            ))
        })?;
    let canonical_root = fs::canonicalize(root)
        .map_err(|error| AggregateError::new(format!("canonicalize source root: {error}")))?;
    let canonical_fixture = fs::canonicalize(root.join(fixture_path)).map_err(|error| {
        AggregateError::new(format!(
            "read simulator fixture source {fixture_path}: {error}"
        ))
    })?;
    if !canonical_fixture.starts_with(&canonical_root) {
        return Err(AggregateError::new(format!(
            "simulator fixture path escapes the source root: {fixture_path}"
        )));
    }
    let fixture_source = fs::read_to_string(&canonical_fixture).map_err(|error| {
        AggregateError::new(format!(
            "read simulator fixture source {fixture_path}: {error}"
        ))
    })?;
    let detector_source = fs::read_to_string(root.join(&descriptor.path)).map_err(|error| {
        AggregateError::new(format!(
            "read simulator detector source {}: {error}",
            descriptor.path
        ))
    })?;
    let fixture_declaration = format!("fn {fixture}");
    if !fixture_source.contains(&fixture_declaration)
        || (!fixture_source.contains(detector) && !detector_source.contains(detector))
    {
        return Err(AggregateError::new(format!(
            "simulator check {check_id} does not bind fixture {fixture} to detector {detector} in the registered source paths"
        )));
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
