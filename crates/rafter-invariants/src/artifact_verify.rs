use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::{Component, Path},
};

use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{aggregate::AggregateError, EvidenceStatus, ResultBundle};

const EVENT_PREFIX: &str = "RAFTER_EVENT ";

pub(super) fn verify(bundle: &ResultBundle, root: &Path) -> Result<(), AggregateError> {
    let mut artifacts = bundle.execution.artifacts.iter().collect::<BTreeSet<_>>();
    artifacts.extend(
        bundle
            .execution
            .checks
            .iter()
            .flat_map(|check| check.artifacts.iter()),
    );
    artifacts.extend(
        bundle
            .results
            .iter()
            .flat_map(|result| result.artifacts.iter()),
    );
    for artifact in artifacts {
        let path = Path::new(&artifact.path);
        if path.is_absolute()
            || path
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(AggregateError::new(format!(
                "artifact path must be repository-relative: {}",
                artifact.path
            )));
        }
        let bytes = fs::read(root.join(path)).map_err(|error| {
            AggregateError::new(format!("read artifact {}: {error}", artifact.path))
        })?;
        let digest = format!("{:x}", Sha256::digest(&bytes));
        if artifact.size_bytes != bytes.len() as u64 || artifact.sha256 != digest {
            return Err(AggregateError::new(format!(
                "artifact integrity mismatch: {}",
                artifact.path
            )));
        }
    }
    match bundle.runner.as_str() {
        "tests" => verify_test_logs(bundle, root),
        "simulator" => verify_simulator_logs(bundle, root),
        "tla" => crate::artifact_verify_tla::verify(bundle, root),
        _ => Ok(()),
    }
}

fn verify_test_logs(bundle: &ResultBundle, root: &Path) -> Result<(), AggregateError> {
    for check in &bundle.execution.checks {
        if !is_passing(bundle, &check.execution_id) {
            continue;
        }
        let test_name = check
            .check_id
            .rsplit_once('#')
            .map(|(_, test_name)| test_name)
            .ok_or_else(|| {
                AggregateError::new(format!("invalid tests check ID {}", check.check_id))
            })?;
        let source = read_artifact_kind(check, "test-log", root)?;
        require_exact_test_pass(&source, test_name, &check.check_id)?;
    }
    Ok(())
}

fn verify_simulator_logs(bundle: &ResultBundle, root: &Path) -> Result<(), AggregateError> {
    let events = simulator_events(bundle, root)?;
    let mut test_logs = BTreeMap::<String, String>::new();
    for check in &bundle.execution.checks {
        verify_simulator_observations(check, &events)?;
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
            events.entry(check_id.to_owned()).or_default().push(event);
        }
    }
    Ok(events)
}

fn verify_simulator_observations(
    check: &crate::CheckReceipt,
    events: &BTreeMap<String, Vec<Value>>,
) -> Result<(), AggregateError> {
    let names = check
        .observations
        .keys()
        .filter_map(|key| key.strip_prefix("runs:"))
        .collect::<Vec<_>>();
    if names.is_empty() {
        return Err(AggregateError::new(format!(
            "simulator receipt {} names no executed model check",
            check.check_id
        )));
    }
    let mut derived = BTreeMap::new();
    for name in names {
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
        for event in matching {
            merge_event_observations(event, &mut derived);
        }
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

fn read_artifact_kind(
    check: &crate::CheckReceipt,
    kind: &str,
    root: &Path,
) -> Result<String, AggregateError> {
    let artifact = check
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == kind)
        .ok_or_else(|| AggregateError::new(format!("{kind} missing for {}", check.check_id)))?;
    fs::read_to_string(root.join(&artifact.path))
        .map_err(|error| AggregateError::new(format!("read {kind} {}: {error}", artifact.path)))
}

fn require_exact_test_pass(
    source: &str,
    test_name: &str,
    check_id: &str,
) -> Result<(), AggregateError> {
    let full_result = format!("test {test_name} ... ok");
    let exact_result = format!("::{test_name} ... ok");
    if !source.lines().any(|line| line.trim() == "running 1 test")
        || !source
            .lines()
            .any(|line| line.trim() == full_result || line.trim_end().ends_with(&exact_result))
        || !source
            .lines()
            .any(|line| line.contains("1 passed; 0 failed; 0 ignored"))
    {
        return Err(AggregateError::new(format!(
            "test log does not prove one exact pass for {check_id}"
        )));
    }
    Ok(())
}

fn is_passing(bundle: &ResultBundle, execution_id: &str) -> bool {
    bundle
        .results
        .iter()
        .any(|result| result.execution_id == execution_id && result.status == EvidenceStatus::Pass)
}
