use std::{collections::BTreeMap, error::Error, path::Path};

use serde_json::Value;

use crate::types::{SimulatorLivenessBinding, RESULT_SCHEMA_VERSION};
use crate::{
    catalog::{Catalog, ProfileContract},
    CheckCompletion, CheckReceipt, EvidenceDescriptor, EvidenceResult, EvidenceStatus,
    ExecutionReceipt, FailureClassification, ResultBundle, SimulatorIdentity, SourceReceipt,
};

use super::{
    artifact, process, simulator_model, source,
    test_compile::{compile, prepare_target_dir, CompiledTarget, Target},
    test_exec::{self, TestOutcome},
    ProducerContext,
};

struct EvaluatedEvidence {
    completion: CheckCompletion,
    status: EvidenceStatus,
    classification: Option<FailureClassification>,
    message: Option<String>,
    observations: BTreeMap<String, u64>,
    simulator_liveness: Option<SimulatorLivenessBinding>,
    artifacts: Vec<crate::ArtifactRef>,
    duration_ms: u64,
    peak_rss_kib: u64,
}

#[derive(Clone, Copy)]
struct ResourceMetrics {
    duration_ms: u64,
    peak_rss_kib: u64,
}

struct ModelEvidence {
    observations: BTreeMap<String, u64>,
    simulator_liveness: Option<SimulatorLivenessBinding>,
    issue: Option<SimulatorIssue>,
}

enum SimulatorIssue {
    InvariantViolation(String),
    HarnessError(String),
    CoverageNotReached(String),
}

struct DetectorRun {
    outcomes: BTreeMap<String, TestOutcome>,
    artifacts: Vec<crate::ArtifactRef>,
    peak_rss_kib: u64,
    duration_ms: u64,
}

pub(super) fn run(
    catalog: &Catalog,
    contract: &ProfileContract,
    profile: &str,
    source: SourceReceipt,
    output_dir: &Path,
    context: &ProducerContext<'_>,
) -> Result<ResultBundle, Box<dyn Error>> {
    contract
        .runners
        .get("simulator")
        .ok_or("simulator runner missing")?;
    let descriptors = catalog
        .required_evidence(contract)
        .into_values()
        .flatten()
        .filter(|descriptor| descriptor.layer == "simulator")
        .collect::<Vec<_>>();
    let model = simulator_model::execute(profile, &source.commit, output_dir)?;
    let detectors = run_detectors(&descriptors, profile, &source.commit, output_dir)?;
    let liveness_contracts = liveness_contracts(&descriptors)?;
    let mut checks = Vec::with_capacity(descriptors.len());
    let mut results = Vec::with_capacity(descriptors.len());
    for descriptor in &descriptors {
        let execution_id = artifact::stable_id("simulator", &descriptor.evidence_id());
        let evaluated = evaluate(descriptor, profile, &liveness_contracts, &model, &detectors)?;
        results.push(EvidenceResult {
            invariant_id: descriptor.invariant_id.clone(),
            evidence_id: descriptor.evidence_id(),
            execution_id: execution_id.clone(),
            status: evaluated.status,
            classification: evaluated.classification,
            message: evaluated.message.clone(),
            artifacts: if evaluated.status == EvidenceStatus::Pass {
                Vec::new()
            } else {
                evaluated.artifacts.clone()
            },
        });
        checks.push(CheckReceipt {
            execution_id,
            check_id: format!("simulator/{}", descriptor.evidence_id()),
            evidence_ids: vec![descriptor.evidence_id()],
            completion: evaluated.completion,
            observations: evaluated.observations,
            simulator_liveness: evaluated.simulator_liveness,
            duration_ms: evaluated.duration_ms,
            peak_rss_kib: evaluated.peak_rss_kib,
            artifacts: evaluated.artifacts,
        });
    }
    source::verify(&source)?;
    let mut execution_artifacts = model.artifacts.clone();
    execution_artifacts.extend(detectors.artifacts);
    Ok(ResultBundle {
        schema_version: RESULT_SCHEMA_VERSION,
        runner: "simulator".to_owned(),
        profile: profile.to_owned(),
        source_ref: source.commit.clone(),
        execution: ExecutionReceipt {
            plan: context.plan.clone(),
            invocation: context.invocation.clone(),
            producer: context.producer.clone(),
            source,
            checks,
            duration_ms: model
                .build_duration_ms
                .saturating_add(model.duration_ms)
                .saturating_add(detectors.duration_ms),
            peak_rss_kib: model
                .build_peak_rss_kib
                .max(model.runtime_peak_rss_kib)
                .max(detectors.peak_rss_kib),
            artifacts: execution_artifacts,
        },
        results,
    })
}

fn run_detectors(
    descriptors: &[EvidenceDescriptor],
    profile: &str,
    source_ref: &str,
    output_dir: &Path,
) -> Result<DetectorRun, Box<dyn Error>> {
    let identities = unique_detector_identities(
        descriptors
            .iter()
            .filter_map(|descriptor| descriptor.simulator.as_ref()?.negative_test.clone())
            .collect(),
    )?;
    if identities.is_empty() {
        return Err("simulator detector inventory is empty".into());
    }
    let detector_profile = format!("{profile}-simulator-detectors");
    let target_dir = prepare_target_dir(&detector_profile, source_ref)?;
    let mut environment = process::base_environment();
    environment.insert(
        "CARGO_TARGET_DIR".to_owned(),
        target_dir.to_string_lossy().into_owned(),
    );
    let targets = identities
        .iter()
        .map(Target::from)
        .collect::<std::collections::BTreeSet<_>>();
    let mut compiled = BTreeMap::new();
    for target in targets {
        let outcome = compile(
            &target,
            &detector_profile,
            source_ref,
            &environment,
            output_dir,
        )?;
        compiled.insert(target, outcome);
    }
    evaluate_detectors(
        identities,
        &compiled,
        &detector_profile,
        source_ref,
        output_dir,
    )
}

fn unique_detector_identities(
    identities: Vec<crate::TestIdentity>,
) -> Result<Vec<crate::TestIdentity>, Box<dyn Error>> {
    let mut unique = BTreeMap::new();
    for identity in identities {
        let check_id = identity.check_id();
        if let Some(previous) = unique.insert(check_id.clone(), identity.clone()) {
            if previous != identity {
                return Err(
                    format!("colliding simulator detector check identity {check_id}").into(),
                );
            }
        }
    }
    Ok(unique.into_values().collect())
}

fn evaluate_detectors(
    identities: Vec<crate::TestIdentity>,
    compiled: &BTreeMap<Target, CompiledTarget>,
    profile: &str,
    source_ref: &str,
    output_dir: &Path,
) -> Result<DetectorRun, Box<dyn Error>> {
    let mut outcomes = BTreeMap::new();
    let mut peak_rss_kib = compiled
        .values()
        .map(|target| target.peak_rss_kib)
        .max()
        .unwrap_or_default();
    let mut artifacts = Vec::new();
    let mut duration_ms = compiled
        .values()
        .map(|target| target.duration_ms)
        .sum::<u64>();
    for target in compiled.values() {
        artifacts.push(target.artifact.clone());
        if let Some(binary) = &target.binary_artifact {
            artifacts.push(binary.clone());
        }
    }
    for identity in identities {
        let target = Target::from(&identity);
        let compiled_target = compiled
            .get(&target)
            .ok_or("compiled simulator detector target inventory changed")?;
        let execution_id = artifact::stable_id("detector", &identity.check_id());
        let mut outcome = test_exec::evaluate(
            &identity,
            compiled_target,
            profile,
            source_ref,
            &execution_id,
            output_dir,
        )?;
        if let Some(binary) = &compiled_target.binary_artifact {
            outcome.artifacts.push(binary.clone());
        }
        peak_rss_kib = peak_rss_kib.max(outcome.peak_rss_kib);
        if !outcome
            .artifacts
            .iter()
            .any(|artifact| artifact.kind == "compile-log")
        {
            duration_ms = duration_ms.saturating_add(outcome.duration_ms);
        }
        if outcomes.insert(identity.check_id(), outcome).is_some() {
            return Err("duplicate simulator detector execution outcome".into());
        }
    }
    Ok(DetectorRun {
        outcomes,
        artifacts,
        peak_rss_kib,
        duration_ms,
    })
}

fn evaluate(
    descriptor: &EvidenceDescriptor,
    profile: &str,
    liveness_contracts: &[crate::types::SimulatorLivenessContract],
    model: &simulator_model::SimulatorExecution,
    detectors: &DetectorRun,
) -> Result<EvaluatedEvidence, Box<dyn Error>> {
    let identity = descriptor
        .simulator
        .as_ref()
        .ok_or("simulator descriptor omitted execution identity")?;
    let mut model_evidence =
        model_observations(profile, identity, liveness_contracts, &model.events);
    let mut artifacts = model.artifacts.clone();
    let detector_outcome = identity
        .negative_test
        .as_ref()
        .map(|test| {
            detectors
                .outcomes
                .get(&test.check_id())
                .ok_or("detector result is missing")
        })
        .transpose()?;
    let detector_passed = match detector_outcome {
        Some(test) => {
            artifacts.extend(test.artifacts.clone());
            test.status == EvidenceStatus::Pass
        }
        None => true,
    };
    let resources = ResourceMetrics {
        duration_ms: model
            .duration_ms
            .saturating_add(detector_outcome.map_or(0, |outcome| outcome.duration_ms)),
        peak_rss_kib: model
            .runtime_peak_rss_kib
            .max(detector_outcome.map_or(0, |outcome| outcome.peak_rss_kib)),
    };
    model_evidence
        .observations
        .insert("detector_qualified".to_owned(), u64::from(detector_passed));
    if let Some(issue) = model_evidence.issue {
        return Ok(evaluate_issue(
            issue,
            model_evidence.observations,
            artifacts,
            resources,
        ));
    }
    let coverage = coverage_reached(identity, &model_evidence.observations);
    if model.processes_succeeded && detector_passed && coverage {
        return Ok(EvaluatedEvidence {
            completion: if identity.liveness_report.is_some() {
                CheckCompletion::Completed
            } else {
                CheckCompletion::FrontierExhausted
            },
            status: EvidenceStatus::Pass,
            classification: None,
            message: None,
            observations: model_evidence.observations,
            simulator_liveness: model_evidence.simulator_liveness,
            artifacts,
            duration_ms: resources.duration_ms,
            peak_rss_kib: resources.peak_rss_kib,
        });
    }
    if !detector_passed || !model.processes_succeeded {
        let message = if detector_passed {
            "simulator profile process did not complete successfully"
        } else {
            "detector qualification fixture did not pass"
        };
        return Ok(EvaluatedEvidence {
            completion: CheckCompletion::HarnessError,
            status: EvidenceStatus::Error,
            classification: Some(FailureClassification::HarnessError),
            message: Some(message.to_owned()),
            observations: model_evidence.observations,
            simulator_liveness: None,
            artifacts,
            duration_ms: resources.duration_ms,
            peak_rss_kib: resources.peak_rss_kib,
        });
    }
    Ok(EvaluatedEvidence {
        completion: CheckCompletion::CoverageNotReached,
        status: EvidenceStatus::Incomplete,
        classification: Some(FailureClassification::CoverageNotReached),
        message: Some("required semantic simulator coverage was not reached".to_owned()),
        observations: model_evidence.observations,
        simulator_liveness: None,
        artifacts,
        duration_ms: resources.duration_ms,
        peak_rss_kib: resources.peak_rss_kib,
    })
}

fn evaluate_issue(
    issue: SimulatorIssue,
    observations: BTreeMap<String, u64>,
    artifacts: Vec<crate::ArtifactRef>,
    resources: ResourceMetrics,
) -> EvaluatedEvidence {
    let (completion, status, classification, message) = match issue {
        SimulatorIssue::InvariantViolation(message) => (
            CheckCompletion::Counterexample,
            EvidenceStatus::Fail,
            FailureClassification::InvariantViolation,
            message,
        ),
        SimulatorIssue::HarnessError(message) => (
            CheckCompletion::HarnessError,
            EvidenceStatus::Error,
            FailureClassification::HarnessError,
            message,
        ),
        SimulatorIssue::CoverageNotReached(message) => (
            CheckCompletion::CoverageNotReached,
            EvidenceStatus::Incomplete,
            FailureClassification::CoverageNotReached,
            message,
        ),
    };
    EvaluatedEvidence {
        completion,
        status,
        classification: Some(classification),
        message: Some(message),
        observations,
        simulator_liveness: None,
        artifacts,
        duration_ms: resources.duration_ms,
        peak_rss_kib: resources.peak_rss_kib,
    }
}

#[cfg(test)]
mod detector_identity_tests {
    use super::unique_detector_identities;
    use crate::TestIdentity;

    fn identity(package: &str, kind: &str, target: &str) -> TestIdentity {
        TestIdentity {
            package: package.to_owned(),
            target_kind: kind.to_owned(),
            target: target.to_owned(),
            test_name: "same_test_name".to_owned(),
        }
    }

    #[test]
    fn detector_inventory_deduplicates_only_complete_test_identities() {
        let first = identity("first", "lib", "first");
        let second = identity("second", "test", "second");
        let unique = unique_detector_identities(vec![first.clone(), first, second])
            .expect("complete identities remain distinct");
        assert_eq!(unique.len(), 2);
        assert_ne!(unique[0].check_id(), unique[1].check_id());
    }

    #[test]
    fn detector_inventory_rejects_ambiguous_check_id_encoding() {
        let first = identity("a/b", "c", "d");
        let second = identity("a", "b/c", "d");
        assert_eq!(first.check_id(), second.check_id());
        assert!(unique_detector_identities(vec![first, second]).is_err());
    }
}

fn model_observations(
    profile: &str,
    identity: &SimulatorIdentity,
    liveness_contracts: &[crate::types::SimulatorLivenessContract],
    events: &BTreeMap<String, Vec<Value>>,
) -> ModelEvidence {
    let mut observations = BTreeMap::new();
    let mut issue = None;
    for check in &identity.checks {
        let matching = events.get(check).map(Vec::as_slice).unwrap_or_default();
        observations.insert(format!("runs:{check}"), matching.len() as u64);
        observations.insert(
            format!("passes:{check}"),
            matching
                .iter()
                .filter(|event| event["status"] == "pass")
                .count() as u64,
        );
        let minimum_steps = matching
            .iter()
            .filter_map(|event| event["steps"].as_u64())
            .min()
            .unwrap_or_default();
        observations.insert(format!("steps:{check}"), minimum_steps);
        for event in matching {
            merge_issue(&mut issue, simulator_event_issue(check, event));
            if identity.liveness_report.is_none() {
                merge_event_observations(event, &mut observations);
            }
        }
    }
    if identity.liveness_report.is_none() {
        return ModelEvidence {
            observations,
            simulator_liveness: None,
            issue,
        };
    }
    observations.insert(identity.required_observation.clone(), 0);
    let simulator_liveness = if issue.is_none() {
        match crate::catalog::derive_liveness_binding(profile, identity, liveness_contracts, events)
        {
            Ok(binding) => {
                observations.insert(
                    identity.required_observation.clone(),
                    binding.reports.len() as u64,
                );
                Some(binding)
            }
            Err(error) => {
                issue = Some(match error.kind {
                    crate::catalog::LivenessReportErrorKind::Missing => {
                        SimulatorIssue::CoverageNotReached(error.message)
                    }
                    crate::catalog::LivenessReportErrorKind::Malformed => {
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
        simulator_liveness,
        issue,
    }
}

fn liveness_contracts(
    descriptors: &[EvidenceDescriptor],
) -> Result<Vec<crate::types::SimulatorLivenessContract>, Box<dyn Error>> {
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

fn simulator_event_issue(check: &str, event: &Value) -> Option<SimulatorIssue> {
    if event.get("status").and_then(Value::as_str) == Some("pass") {
        return None;
    }
    let message = event.get("message").and_then(Value::as_str).map_or_else(
        || format!("simulator check `{check}` did not pass"),
        str::to_owned,
    );
    match event.get("classification").and_then(Value::as_str) {
        Some("invariant-violation") => Some(SimulatorIssue::InvariantViolation(message)),
        Some("coverage-not-reached") => Some(SimulatorIssue::CoverageNotReached(message)),
        Some("harness-error") => Some(SimulatorIssue::HarnessError(message)),
        _ => Some(SimulatorIssue::HarnessError(format!(
            "simulator check `{check}` has an invalid failure classification"
        ))),
    }
}

fn merge_issue(current: &mut Option<SimulatorIssue>, candidate: Option<SimulatorIssue>) {
    let Some(candidate) = candidate else {
        return;
    };
    let rank = |issue: &SimulatorIssue| match issue {
        SimulatorIssue::InvariantViolation(_) => 3,
        SimulatorIssue::HarnessError(_) => 2,
        SimulatorIssue::CoverageNotReached(_) => 1,
    };
    if current
        .as_ref()
        .is_none_or(|issue| rank(&candidate) > rank(issue))
    {
        *current = Some(candidate);
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

fn coverage_reached(identity: &SimulatorIdentity, observations: &BTreeMap<String, u64>) -> bool {
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{model_observations, simulator_event_issue, SimulatorIssue};
    use crate::SimulatorIdentity;
    use serde_json::json;

    #[test]
    fn simulator_failure_classifications_remain_distinct() {
        let invariant = simulator_event_issue(
            "raft-soak",
            &json!({"status": "fail", "classification": "invariant-violation"}),
        );
        assert!(matches!(
            invariant,
            Some(SimulatorIssue::InvariantViolation(_))
        ));

        let incomplete = simulator_event_issue(
            "raft-soak",
            &json!({"status": "incomplete", "classification": "coverage-not-reached"}),
        );
        assert!(matches!(
            incomplete,
            Some(SimulatorIssue::CoverageNotReached(_))
        ));

        let malformed = simulator_event_issue(
            "raft-soak",
            &json!({"status": "error", "classification": "harness-error"}),
        );
        assert!(matches!(malformed, Some(SimulatorIssue::HarnessError(_))));
    }

    #[test]
    fn safety_model_events_preserve_their_structured_failure_classification() {
        let identity = SimulatorIdentity {
            checks: vec!["raft-commit".to_owned()],
            required_observation: "commit_floor_advances".to_owned(),
            minimum_observation: 1,
            minimum_protocol_states: Some(1),
            minimum_verifier_states: Some(1),
            minimum_runs_per_check: None,
            minimum_steps: None,
            liveness_report: None,
            negative_test: None,
        };
        let events = BTreeMap::from([(
            "raft-commit".to_owned(),
            vec![json!({
                "status": "fail",
                "classification": "invariant-violation",
                "message": "commit witness violated"
            })],
        )]);

        let evidence = model_observations("pr", &identity, &[], &events);

        assert!(matches!(
            evidence.issue,
            Some(SimulatorIssue::InvariantViolation(message))
                if message == "commit witness violated"
        ));
    }
}
