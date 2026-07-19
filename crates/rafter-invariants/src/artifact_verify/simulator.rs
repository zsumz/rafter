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
        is_passing, require_detector_witness_contract, require_exact_test_pass,
        verify_detector_harness_error_invocations, verify_test_invocations,
    },
};

mod liveness;

pub(super) use liveness::verify_liveness_observations;

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
    let mut detector_sources = super::detector_source::DetectorSourceCache::default();
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
            &descriptor.invariant_id,
            identity,
            &events,
            inspection.global_issue,
        )?;
        verify_simulator_observations(bundle, check, identity, &liveness_contracts, &events)?;
        verify_negative_detector_evidence(
            bundle,
            root,
            check,
            descriptor,
            identity,
            &mut detector_sources,
            &mut test_logs,
        )?;
    }
    diagnostics.sort();
    diagnostics.dedup();
    Ok(diagnostics)
}

fn verify_negative_detector_evidence(
    bundle: &ResultBundle,
    root: &Path,
    check: &crate::CheckReceipt,
    descriptor: &crate::EvidenceDescriptor,
    identity: &crate::SimulatorIdentity,
    detector_sources: &mut super::detector_source::DetectorSourceCache,
    test_logs: &mut BTreeMap<String, String>,
) -> Result<(), AggregateError> {
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
    let invocation_contract = verify_negative_fixture_binding_cached(
        root,
        descriptor,
        fixture,
        &check.check_id,
        detector_sources,
    )?;
    let qualified = check
        .observations
        .get("detector_qualified")
        .copied()
        .ok_or_else(|| {
            AggregateError::new(format!(
                "simulator check {} omits detector qualification status",
                check.check_id
            ))
        })?;
    if qualified > 1 {
        return Err(AggregateError::new(format!(
            "simulator check {} has invalid detector qualification count {qualified}",
            check.check_id
        )));
    }
    if qualified == 0 && is_passing(bundle, &check.execution_id) {
        return Err(AggregateError::new(format!(
            "passing simulator check {} did not qualify its detector",
            check.check_id
        )));
    }
    let artifact = check
        .artifacts
        .iter()
        .find(|artifact| artifact.kind == "test-log")
        .cloned();
    let Some(artifact) = artifact else {
        if qualified == 0
            && check
                .artifacts
                .iter()
                .any(|artifact| artifact.kind == "compile-log")
        {
            return Ok(());
        }
        return Err(AggregateError::new(format!(
            "detector log missing for {}",
            check.check_id
        )));
    };
    let source = if let Some(source) = test_logs.get(&artifact.path) {
        source.clone()
    } else {
        let source = fs::read_to_string(root.join(&artifact.path)).map_err(|error| {
            AggregateError::new(format!("read detector log {}: {error}", artifact.path))
        })?;
        test_logs.insert(artifact.path.clone(), source.clone());
        source
    };
    if qualified == 0 {
        return verify_detector_harness_error_invocations(
            bundle,
            check,
            &source,
            &negative_test.test_name,
            &negative_test.check_id(),
            root,
        );
    }
    verify_test_invocations(
        bundle,
        check,
        &source,
        &negative_test.test_name,
        &negative_test.check_id(),
        root,
    )?;
    require_detector_witness_contract(
        bundle,
        &source,
        &negative_test.check_id(),
        invocation_contract.registered_identity(),
        invocation_contract.witnesses(),
    )?;
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
}

struct MachineEventInspection {
    global_issue: Option<RawEventIssue>,
    diagnostics: Vec<String>,
}

fn verify_nonpassing_event_classification(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    invariant_id: &str,
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
            Some(invariant_id),
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
    let outcomes = bundle
        .results
        .iter()
        .filter(|result| result.execution_id == check.execution_id)
        .map(|result| (result.status, result.classification))
        .collect::<Vec<_>>();
    if outcomes.is_empty()
        || outcomes.iter().any(|outcome| {
            receipt_issue(*outcome).is_none_or(|actual| {
                actual.rank() < expected.rank()
                    || (actual == RawEventIssue::InvariantViolation
                        && expected != RawEventIssue::InvariantViolation)
            })
        })
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
    let mut routes = BTreeMap::<String, BTreeSet<String>>::new();
    for descriptor in descriptors {
        if let Some(identity) = descriptor.simulator.as_ref() {
            for check in &identity.checks {
                routes
                    .entry(check.clone())
                    .or_default()
                    .insert(descriptor.invariant_id.clone());
            }
        }
    }
    let mut unknown = BTreeSet::new();
    let mut diagnostics = BTreeSet::new();
    let mut global_issue = None;
    for (indexed_check_id, indexed_events) in events {
        for event in indexed_events.iter().filter(|event| {
            event.get("check_id").and_then(Value::as_str) == Some(indexed_check_id.as_str())
        }) {
            let check_id = indexed_check_id.as_str();
            let (event_issue, diagnostic) = raw_event_issue(check_id, event, None);
            diagnostics.extend(diagnostic);
            let canonical = crate::producer::canonical_check_id(profile, check_id);
            let route = routes.get(check_id).or_else(|| {
                canonical
                    .as_ref()
                    .and_then(|canonical| routes.get(canonical))
            });
            if let Some(route) = route {
                if event_issue == Some(RawEventIssue::InvariantViolation) {
                    match machine_invariant_id(check_id, event) {
                        Ok(invariant_id) if route.contains(invariant_id) => {}
                        Ok(invariant_id) => {
                            diagnostics.insert(format!(
                                "simulator check `{check_id}` emitted invariant {invariant_id} without a registered failure route"
                            ));
                            merge_raw_issue(&mut global_issue, Some(RawEventIssue::HarnessError));
                        }
                        Err(error) => {
                            diagnostics.insert(error);
                            merge_raw_issue(&mut global_issue, Some(RawEventIssue::HarnessError));
                        }
                    }
                }
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

fn raw_event_issue(
    check_id: &str,
    event: &Value,
    expected_invariant_id: Option<&str>,
) -> (Option<RawEventIssue>, Option<String>) {
    let issue = match (
        event.get("status").and_then(Value::as_str),
        event.get("classification"),
    ) {
        (Some("pass"), None | Some(Value::Null)) => {
            if event.get("event").and_then(Value::as_str) == Some("profile-total") {
                return (None, None);
            }
            return match verified_passing_simulator_event_contract(check_id, event) {
                Ok(()) => (None, None),
                Err(error) => (Some(RawEventIssue::HarnessError), Some(error)),
            };
        }
        (Some("fail"), Some(Value::String(classification)))
            if classification == "invariant-violation" =>
        {
            match machine_invariant_id(check_id, event) {
                Ok(observed)
                    if expected_invariant_id.is_none_or(|expected| expected == observed) =>
                {
                    RawEventIssue::InvariantViolation
                }
                Ok(_) => RawEventIssue::CoverageNotReached,
                Err(error) => return (Some(RawEventIssue::HarnessError), Some(error)),
            }
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

fn verified_passing_simulator_event_contract(check_id: &str, event: &Value) -> Result<(), String> {
    let expected_event_kind = if check_id.split('-').any(|segment| segment == "soak") {
        "soak-check"
    } else {
        "exhaustive-check"
    };
    let observations_are_counts = event
        .get("observations")
        .and_then(Value::as_object)
        .is_some_and(|observations| observations.values().all(Value::is_u64));
    let common = event.get("check_id").and_then(Value::as_str) == Some(check_id)
        && event.get("status").and_then(Value::as_str) == Some("pass")
        && matches!(event.get("classification"), None | Some(Value::Null))
        && observations_are_counts;
    let expected_shape = match expected_event_kind {
        "exhaustive-check" => {
            event.get("event").and_then(Value::as_str) == Some(expected_event_kind)
                && event
                    .get("unique_protocol_states")
                    .and_then(Value::as_u64)
                    .is_some()
                && event
                    .get("unique_verifier_states")
                    .and_then(Value::as_u64)
                    .is_some()
        }
        "soak-check" => {
            event.get("event").and_then(Value::as_str) == Some(expected_event_kind)
                && event.get("seed").and_then(Value::as_u64).is_some()
                && event.get("steps").and_then(Value::as_u64).is_some()
                && event.get("duration_ms").and_then(Value::as_u64).is_some()
                && event
                    .get("execution_contract")
                    .is_some_and(Value::is_object)
                && verified_string_array(event.get("observed_actions"))
                && verified_string_array(event.get("liveness_features"))
                && event.get("liveness_reports").is_some_and(Value::is_array)
        }
        _ => unreachable!("simulator passing event kinds are exhaustive or soak"),
    };
    if common && expected_shape {
        return Ok(());
    }
    Err(format!(
        "simulator check `{check_id}` has a malformed passing machine event: expected {expected_event_kind}, found {}",
        event
            .get("event")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
    ))
}

fn verified_string_array(value: Option<&Value>) -> bool {
    value
        .and_then(Value::as_array)
        .is_some_and(|values| values.iter().all(Value::is_string))
}

fn machine_invariant_id<'a>(check_id: &str, event: &'a Value) -> Result<&'a str, String> {
    if event.get("event").and_then(Value::as_str) != Some("check-failure")
        || event.get("event_version").and_then(Value::as_u64) != Some(2)
    {
        return Err(format!(
            "simulator check `{check_id}` invariant violation used an unsupported machine-event contract"
        ));
    }
    let invariant_id = event
        .get("invariant_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            format!("simulator check `{check_id}` invariant violation omitted invariant_id")
        })?;
    let valid_shape = invariant_id.len() == 5
        && invariant_id.as_bytes()[0..2]
            .iter()
            .all(u8::is_ascii_uppercase)
        && invariant_id.as_bytes()[2] == b'-'
        && invariant_id.as_bytes()[3..5].iter().all(u8::is_ascii_digit);
    let label = event
        .get("invariant")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            format!("simulator check `{check_id}` invariant violation omitted its invariant label")
        })?;
    if !valid_shape
        || !label
            .strip_prefix(invariant_id)
            .is_some_and(|suffix| suffix.starts_with(' '))
    {
        return Err(format!(
            "simulator check `{check_id}` has an invalid invariant identity: id={invariant_id:?}, label={label:?}"
        ));
    }
    Ok(invariant_id)
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

#[cfg(test)]
fn verify_negative_fixture_binding(
    root: &Path,
    descriptor: &crate::EvidenceDescriptor,
    fixture: &str,
    check_id: &str,
) -> Result<super::detector_source::DetectorInvocationContract, AggregateError> {
    verify_negative_fixture_binding_cached(
        root,
        descriptor,
        fixture,
        check_id,
        &mut super::detector_source::DetectorSourceCache::default(),
    )
}

fn verify_negative_fixture_binding_cached(
    root: &Path,
    descriptor: &crate::EvidenceDescriptor,
    fixture: &str,
    check_id: &str,
    cache: &mut super::detector_source::DetectorSourceCache,
) -> Result<super::detector_source::DetectorInvocationContract, AggregateError> {
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
    let test_identity = descriptor
        .simulator
        .as_ref()
        .and_then(|identity| identity.negative_test.as_ref())
        .ok_or_else(|| {
            AggregateError::new(format!(
                "simulator check {check_id} has no registered detector test identity"
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
    let canonical_detector = fs::canonicalize(root.join(&descriptor.path)).map_err(|error| {
        AggregateError::new(format!(
            "read simulator detector source {}: {error}",
            descriptor.path
        ))
    })?;
    if !canonical_detector.starts_with(&canonical_root) {
        return Err(AggregateError::new(format!(
            "simulator detector path escapes the source root: {}",
            descriptor.path
        )));
    }
    let fixture_source = fs::read_to_string(&canonical_fixture).map_err(|error| {
        AggregateError::new(format!(
            "read simulator fixture source {fixture_path}: {error}"
        ))
    })?;
    let detector_source = fs::read_to_string(&canonical_detector).map_err(|error| {
        AggregateError::new(format!(
            "read simulator detector source {}: {error}",
            descriptor.path
        ))
    })?;
    super::detector_source::verify_invocation_bound_detector_cached(
        &crate::DetectorFixtureSourceBinding {
            fixture_source: &fixture_source,
            detector_source: &detector_source,
            source_root: &canonical_root,
            fixture_path: &canonical_fixture,
            detector_path: &canonical_detector,
            test_identity,
            fixture,
            detector,
        },
        cache,
    )
    .map_err(|error| {
        AggregateError::new(format!(
            "simulator check {check_id} does not bind fixture {fixture} to detector {detector}: {error}"
        ))
    })
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
    liveness_contracts: &[crate::contract::profile::SimulatorLivenessContract],
    events: &BTreeMap<String, Vec<Value>>,
) -> Result<(), AggregateError> {
    if identity.checks.is_empty() {
        return Err(AggregateError::new(format!(
            "simulator receipt {} names no executed model check",
            check.check_id
        )));
    }
    let (mut derived, profile_issue) =
        derive_simulator_observation_counts(bundle, identity, events)?;
    verify_profile_issue_outcome(bundle, check, profile_issue)?;
    verify_liveness_observations(
        bundle,
        check,
        identity,
        liveness_contracts,
        events,
        &mut derived,
    )?;
    verify_composite_observation(bundle, check, identity, events)?;
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

fn derive_simulator_observation_counts(
    bundle: &ResultBundle,
    identity: &crate::SimulatorIdentity,
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
                derive_check_contract_issue(name, matching, contract, &mut derived),
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

fn verify_profile_issue_outcome(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
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

fn receipt_issue(
    outcome: (crate::EvidenceStatus, Option<crate::FailureClassification>),
) -> Option<RawEventIssue> {
    match outcome {
        (crate::EvidenceStatus::Fail, Some(crate::FailureClassification::InvariantViolation)) => {
            Some(RawEventIssue::InvariantViolation)
        }
        (crate::EvidenceStatus::Error, Some(crate::FailureClassification::HarnessError)) => {
            Some(RawEventIssue::HarnessError)
        }
        (
            crate::EvidenceStatus::Incomplete,
            Some(crate::FailureClassification::CoverageNotReached),
        ) => Some(RawEventIssue::CoverageNotReached),
        _ => None,
    }
}

fn verify_composite_observation(
    bundle: &ResultBundle,
    check: &crate::CheckReceipt,
    identity: &crate::SimulatorIdentity,
    events: &BTreeMap<String, Vec<Value>>,
) -> Result<(), AggregateError> {
    if identity.liveness_report.is_some() || !is_passing(bundle, &check.execution_id) {
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

fn derive_check_contract_issue(
    check: &str,
    events: &[Value],
    contract: &crate::SimulatorCheckContract,
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
#[path = "simulator_event_semantics_tests.rs"]
mod event_semantics_tests;
