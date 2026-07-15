use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    path::Path,
};

use serde_json::Value;

use crate::{aggregate::AggregateError, ResultBundle};

use super::{
    simulator_schedule::verify_simulator_schedule,
    test_logs::{
        is_passing, require_detector_witness, require_exact_test_pass, verify_test_invocations,
    },
};

pub(super) fn verify_simulator_logs(
    bundle: &ResultBundle,
    root: &Path,
) -> Result<Vec<String>, AggregateError> {
    let mut diagnostics = verify_simulator_schedule(bundle, root)?;
    let scanned = simulator_events(bundle, root)?;
    diagnostics.extend(scanned.diagnostics);
    let events = scanned.events;
    let catalog =
        crate::Catalog::load(root.join(&bundle.execution.plan.registry.path).as_path())
            .map_err(|error| AggregateError::new(format!("reload simulator registry: {error}")))?;
    let profile_descriptors = catalog
        .required_evidence(&bundle.execution.plan.contract)
        .into_values()
        .flatten()
        .filter(|descriptor| descriptor.layer == "simulator")
        .collect::<Vec<_>>();
    let inspection = inspect_machine_events(&bundle.profile, &profile_descriptors, &events);
    diagnostics.extend(inspection.diagnostics);
    let descriptors = profile_descriptors
        .iter()
        .map(|descriptor| (descriptor.evidence_id(), descriptor))
        .collect::<BTreeMap<_, _>>();
    let liveness_contracts = profile_descriptors
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
        verify_nonpassing_event_classification(
            bundle,
            check,
            identity,
            &events,
            inspection.global_issue,
        )?;
        verify_simulator_observations(bundle, check, identity, &liveness_contracts, &events)?;
        verify_passing_negative_detector(
            bundle,
            root,
            check,
            descriptor,
            identity,
            &mut test_logs,
        )?;
    }
    diagnostics.sort();
    diagnostics.dedup();
    Ok(diagnostics)
}

fn verify_passing_negative_detector(
    bundle: &ResultBundle,
    root: &Path,
    check: &crate::CheckReceipt,
    descriptor: &crate::EvidenceDescriptor,
    identity: &crate::SimulatorIdentity,
    test_logs: &mut BTreeMap<String, String>,
) -> Result<(), AggregateError> {
    if !is_passing(bundle, &check.execution_id) {
        return Ok(());
    }
    let Some(negative_test) = identity.negative_test.as_ref() else {
        return Ok(());
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
    let detector = descriptor
        .negative_fixture_detector
        .as_deref()
        .ok_or_else(|| {
            AggregateError::new(format!(
                "simulator check {} has no registered detector identity",
                check.check_id
            ))
        })?;
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
    require_detector_witness(bundle, &source, &negative_test.check_id(), detector)?;
    require_exact_test_pass(&source, &negative_test.test_name, &check.check_id)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RawEventIssue {
    InvariantViolation,
    HarnessError,
    CoverageNotReached,
}

impl RawEventIssue {
    const fn rank(self) -> u8 {
        match self {
            Self::InvariantViolation => 3,
            Self::HarnessError => 2,
            Self::CoverageNotReached => 1,
        }
    }

    const fn receipt_outcome(self) -> (crate::EvidenceStatus, crate::FailureClassification) {
        match self {
            Self::InvariantViolation => (
                crate::EvidenceStatus::Fail,
                crate::FailureClassification::InvariantViolation,
            ),
            Self::HarnessError => (
                crate::EvidenceStatus::Error,
                crate::FailureClassification::HarnessError,
            ),
            Self::CoverageNotReached => (
                crate::EvidenceStatus::Incomplete,
                crate::FailureClassification::CoverageNotReached,
            ),
        }
    }
}

struct MachineEventInspection {
    global_issue: Option<RawEventIssue>,
    diagnostics: Vec<String>,
}

fn verify_nonpassing_event_classification(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    identity: &crate::SimulatorIdentity,
    events: &BTreeMap<String, Vec<Value>>,
    global_issue: Option<RawEventIssue>,
) -> Result<(), AggregateError> {
    let mut expected = global_issue;
    for event in identity
        .checks
        .iter()
        .flat_map(|name| events.get(name).into_iter().flatten())
    {
        let (candidate, _) = raw_event_issue(
            event
                .get("check_id")
                .and_then(Value::as_str)
                .unwrap_or("<missing>"),
            event,
        );
        if candidate.is_some_and(|candidate| {
            expected.is_none_or(|expected| candidate.rank() > expected.rank())
        }) {
            expected = candidate;
        }
    }
    let Some(expected) = expected else {
        return Ok(());
    };
    let (expected_status, expected_classification) = expected.receipt_outcome();
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

fn inspect_machine_events(
    profile: &str,
    descriptors: &[crate::EvidenceDescriptor],
    events: &BTreeMap<String, Vec<Value>>,
) -> MachineEventInspection {
    let claimed = descriptors
        .iter()
        .filter_map(|descriptor| descriptor.simulator.as_ref())
        .flat_map(|identity| identity.checks.iter().cloned())
        .collect::<BTreeSet<_>>();
    let mut unknown = BTreeSet::new();
    let mut diagnostics = BTreeSet::new();
    let mut global_issue = None;
    for (indexed_check_id, indexed_events) in events {
        for event in indexed_events.iter().filter(|event| {
            event.get("check_id").and_then(Value::as_str) == Some(indexed_check_id.as_str())
        }) {
            let check_id = indexed_check_id.as_str();
            let (event_issue, diagnostic) = raw_event_issue(check_id, event);
            diagnostics.extend(diagnostic);
            let canonical = crate::producer::canonical_check_id(profile, check_id);
            if claimed.contains(check_id)
                || canonical
                    .as_ref()
                    .is_some_and(|canonical| claimed.contains(canonical))
            {
                continue;
            }
            if allowed_summary_event(profile, check_id, event) {
                merge_raw_issue(&mut global_issue, event_issue);
            } else {
                unknown.insert(check_id.to_owned());
            }
        }
    }
    if !unknown.is_empty() {
        diagnostics.insert(format!(
            "simulator emitted unclaimed machine event check IDs: {}",
            unknown.into_iter().collect::<Vec<_>>().join(", ")
        ));
        merge_raw_issue(&mut global_issue, Some(RawEventIssue::HarnessError));
    }
    MachineEventInspection {
        global_issue,
        diagnostics: diagnostics.into_iter().collect(),
    }
}

fn raw_event_issue(check_id: &str, event: &Value) -> (Option<RawEventIssue>, Option<String>) {
    let issue = match (
        event.get("status").and_then(Value::as_str),
        event.get("classification"),
    ) {
        (Some("pass"), None | Some(Value::Null)) => return (None, None),
        (Some("fail"), Some(Value::String(classification)))
            if classification == "invariant-violation" =>
        {
            RawEventIssue::InvariantViolation
        }
        (Some("incomplete"), Some(Value::String(classification)))
            if classification == "coverage-not-reached" =>
        {
            RawEventIssue::CoverageNotReached
        }
        (Some("error"), Some(Value::String(classification)))
            if classification == "harness-error" =>
        {
            RawEventIssue::HarnessError
        }
        _ => {
            return (
                Some(RawEventIssue::HarnessError),
                Some(invalid_event_pair_message(check_id, event)),
            )
        }
    };
    (Some(issue), None)
}

fn merge_raw_issue(current: &mut Option<RawEventIssue>, candidate: Option<RawEventIssue>) {
    if candidate
        .is_some_and(|candidate| current.is_none_or(|current| candidate.rank() > current.rank()))
    {
        *current = candidate;
    }
}

fn allowed_summary_event(profile: &str, check_id: &str, event: &Value) -> bool {
    matches!(profile, "nightly" | "weekly")
        && event.get("event").and_then(Value::as_str) == Some("profile-total")
        && check_id == format!("raft-profile-total-{profile}")
}

fn invalid_event_pair_message(check_id: &str, event: &Value) -> String {
    let field = |name| {
        event
            .get(name)
            .map_or_else(|| "<missing>".to_owned(), Value::to_string)
    };
    format!(
        "simulator check `{check_id}` has invalid status/classification pair: status={}, classification={}",
        field("status"),
        field("classification")
    )
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

struct ScannedSimulatorEvents {
    events: BTreeMap<String, Vec<Value>>,
    diagnostics: Vec<String>,
}

fn simulator_events(
    bundle: &ResultBundle,
    root: &Path,
) -> Result<ScannedSimulatorEvents, AggregateError> {
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
    let mut diagnostics = Vec::new();
    for log in logs {
        let source = fs::read_to_string(root.join(&log.path)).map_err(|error| {
            AggregateError::new(format!("read simulator log {}: {error}", log.path))
        })?;
        let (parsed, parse_diagnostics) = super::simulator_schedule::scan_machine_events(
            &source,
            &format!("simulator event in {}", log.path),
        );
        diagnostics.extend(parse_diagnostics);
        for event in parsed {
            index_simulator_event(&bundle.profile, event, &mut events)
                .map_err(|error| AggregateError::new(format!("{} in {}", error, log.path)))?;
        }
    }
    Ok(ScannedSimulatorEvents {
        events,
        diagnostics,
    })
}

fn index_simulator_event(
    profile: &str,
    event: Value,
    events: &mut BTreeMap<String, Vec<Value>>,
) -> Result<(), &'static str> {
    let check_id = event
        .get("check_id")
        .and_then(Value::as_str)
        .ok_or("simulator event scanner returned an event without check_id")?;
    events
        .entry(check_id.to_owned())
        .or_default()
        .push(event.clone());
    if let Some(canonical) = crate::producer::canonical_check_id(profile, check_id) {
        events.entry(canonical).or_default().push(event);
    }
    Ok(())
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

#[cfg(test)]
mod event_semantics_tests {
    use std::collections::BTreeMap;

    use serde_json::{json, Value};

    use super::{
        index_simulator_event, inspect_machine_events, verify_nonpassing_event_classification,
        RawEventIssue,
    };

    #[test]
    fn serialized_verifier_rejects_contradictory_and_missing_event_pairs() {
        let (catalog, manifest) = crate::tests::loaded();
        let bundle = simulator_bundle(&catalog, &manifest);
        let check = &bundle.execution.checks[0];
        let descriptor = catalog
            .evidence
            .iter()
            .find(|descriptor| descriptor.evidence_id() == check.evidence_ids[0])
            .expect("registered simulator descriptor");
        let identity = descriptor.simulator.as_ref().expect("simulator identity");
        let check_id = &identity.checks[0];

        for event in [
            json!({
                "check_id": check_id,
                "status": "pass",
                "classification": "invariant-violation",
            }),
            json!({"check_id": check_id, "status": "fail"}),
            json!({
                "check_id": check_id,
                "status": "incomplete",
                "classification": null,
            }),
            json!({
                "check_id": check_id,
                "status": "unknown",
                "classification": "harness-error",
            }),
        ] {
            let events = serialized_events("pr", &event);
            let inspection =
                inspect_machine_events("pr", std::slice::from_ref(descriptor), &events);
            assert_eq!(inspection.global_issue, None);
            assert_eq!(inspection.diagnostics.len(), 1);
            assert!(inspection.diagnostics[0].contains("invalid status/classification pair"));
            assert!(verify_nonpassing_event_classification(
                &bundle,
                check,
                identity,
                &events,
                inspection.global_issue,
            )
            .is_err());
        }
    }

    #[test]
    fn serialized_verifier_rejects_unknown_invariant_violation_as_harness_error() {
        let (catalog, manifest) = crate::tests::loaded();
        let bundle = simulator_bundle(&catalog, &manifest);
        let check = &bundle.execution.checks[0];
        let descriptor = catalog
            .evidence
            .iter()
            .find(|descriptor| descriptor.evidence_id() == check.evidence_ids[0])
            .expect("registered simulator descriptor");
        let identity = descriptor.simulator.as_ref().expect("simulator identity");
        let event = json!({
            "check_id": "unknown-invariant",
            "status": "fail",
            "classification": "invariant-violation",
        });
        let events = serialized_events("pr", &event);
        let profile_descriptors = catalog
            .required_evidence(&bundle.execution.plan.contract)
            .into_values()
            .flatten()
            .filter(|descriptor| descriptor.layer == "simulator")
            .collect::<Vec<_>>();
        let inspection = inspect_machine_events("pr", &profile_descriptors, &events);

        assert_eq!(inspection.global_issue, Some(RawEventIssue::HarnessError));
        assert_eq!(
            inspection.diagnostics,
            ["simulator emitted unclaimed machine event check IDs: unknown-invariant"]
        );
        assert!(verify_nonpassing_event_classification(
            &bundle,
            check,
            identity,
            &events,
            inspection.global_issue,
        )
        .is_err());
    }

    fn simulator_bundle(
        catalog: &crate::Catalog,
        manifest: &crate::ProfileManifest,
    ) -> crate::ResultBundle {
        crate::tests::passing_bundles(catalog, manifest)
            .into_iter()
            .find(|bundle| bundle.runner == "simulator")
            .expect("simulator bundle")
    }

    fn serialized_events(profile: &str, event: &Value) -> BTreeMap<String, Vec<Value>> {
        let source = format!("{}{}", crate::artifact_verify::EVENT_PREFIX, event);
        let (parsed, diagnostics) = super::super::simulator_schedule::scan_machine_events(
            &source,
            "serialized simulator fixture",
        );
        assert!(diagnostics.is_empty());
        let mut events = BTreeMap::new();
        for event in parsed {
            index_simulator_event(profile, event, &mut events).expect("index serialized event");
        }
        events
    }
}
