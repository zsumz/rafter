use std::{collections::BTreeMap, error::Error, path::Path};

use serde_json::Value;

use crate::{
    contract::{catalog::Catalog, profile::ProfileContract},
    evidence::{SimulatorLivenessBinding, RESULT_SCHEMA_VERSION},
    CheckCompletion, CheckReceipt, EvidenceDescriptor, EvidenceResult, EvidenceStatus,
    ExecutionReceipt, FailureClassification, ResultBundle, SimulatorCheckContract,
    SimulatorIdentity, SourceReceipt,
};

use super::{
    artifact, process, simulator_model, source,
    test_compile::{compile, prepare_target_dir, CompiledTarget, Target},
    test_exec::{self, TestOutcome},
    ProducerContext,
};

#[path = "simulator_events.rs"]
mod events;
mod liveness;

pub(crate) use events::passing_simulator_event_contract;
use events::{simulator_event_inventory_issue, simulator_event_issue};

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
    per_check_required_observations: BTreeMap<String, u64>,
    simulator_liveness: Option<SimulatorLivenessBinding>,
    issue: Option<SimulatorIssue>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
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
    harness_error: Option<String>,
}

pub(super) fn run(
    catalog: &Catalog,
    contract: &ProfileContract,
    profile: &str,
    source: SourceReceipt,
    output_dir: &Path,
    context: &ProducerContext<'_>,
) -> Result<ResultBundle, Box<dyn Error>> {
    let runner = contract
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
    let detectors = match run_detectors(&descriptors, profile, &source.commit, output_dir) {
        Ok(detectors) => detectors,
        Err(error) => DetectorRun {
            outcomes: BTreeMap::new(),
            artifacts: Vec::new(),
            peak_rss_kib: 0,
            duration_ms: 0,
            harness_error: Some(format!("simulator detector execution failed: {error}")),
        },
    };
    let liveness_contracts = liveness_contracts(&descriptors)?;
    let (checks, results) = evaluate_descriptors(
        &descriptors,
        profile,
        &runner.simulator_checks,
        &liveness_contracts,
        &model,
        &detectors,
    )?;
    source::verify(&source)?;
    let mut execution_artifacts = model.artifacts.clone();
    execution_artifacts.extend(detectors.artifacts.clone());
    let resources = execution_resource_metrics(&model, &detectors);
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
            duration_ms: resources.duration_ms,
            peak_rss_kib: resources.peak_rss_kib,
            artifacts: execution_artifacts,
        },
        results,
    })
}

fn evaluate_descriptors(
    descriptors: &[EvidenceDescriptor],
    profile: &str,
    check_contracts: &BTreeMap<String, SimulatorCheckContract>,
    liveness_contracts: &[crate::contract::profile::SimulatorLivenessContract],
    model: &simulator_model::SimulatorExecution,
    detectors: &DetectorRun,
) -> Result<(Vec<CheckReceipt>, Vec<EvidenceResult>), Box<dyn Error>> {
    let inventory_issue = simulator_event_inventory_issue(profile, descriptors, &model.events);
    let mut checks = Vec::with_capacity(descriptors.len());
    let mut results = Vec::with_capacity(descriptors.len());
    for descriptor in descriptors {
        let execution_id = artifact::stable_id("simulator", &descriptor.evidence_id());
        let evaluated = evaluate_with_inventory_issue(
            descriptor,
            profile,
            check_contracts,
            liveness_contracts,
            model,
            detectors,
            inventory_issue.as_ref(),
        )?;
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
    Ok((checks, results))
}

#[cfg(test)]
pub(crate) fn evaluate_model_fixture(
    catalog: &Catalog,
    profile: &str,
    model: &simulator_model::SimulatorExecution,
) -> Result<(Vec<CheckReceipt>, Vec<EvidenceResult>), Box<dyn Error>> {
    let descriptors = catalog
        .evidence
        .iter()
        .filter(|descriptor| descriptor.layer == "simulator")
        .cloned()
        .collect::<Vec<_>>();
    let contracts = liveness_contracts(&descriptors)?;
    let (_, manifest) = crate::tests::loaded();
    let check_contracts = &manifest.profiles[profile].runners["simulator"].simulator_checks;
    evaluate_descriptors(
        &descriptors,
        profile,
        check_contracts,
        &contracts,
        model,
        &DetectorRun {
            outcomes: BTreeMap::new(),
            artifacts: Vec::new(),
            peak_rss_kib: 0,
            duration_ms: 0,
            harness_error: None,
        },
    )
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
    let (execution_deadline, _) = process::active_layer_deadlines(profile, "simulator")?;
    let target_dir = prepare_target_dir(&detector_profile, source_ref, execution_deadline)?;
    let mut environment = process::base_environment();
    environment.insert(
        "CARGO_TARGET_DIR".to_owned(),
        target_dir.external_path().to_string_lossy().into_owned(),
    );
    let targets = identities
        .iter()
        .map(Target::from)
        .collect::<std::collections::BTreeSet<_>>();
    let mut compiled = BTreeMap::new();
    for target in targets {
        target_dir.verify()?;
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
        execution_deadline,
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
    scratch_deadline: std::time::Instant,
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
        let mut outcome = test_exec::evaluate_detector(
            &identity,
            compiled_target,
            profile,
            source_ref,
            &execution_id,
            output_dir,
            scratch_deadline,
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
        harness_error: None,
    })
}

#[cfg(test)]
fn evaluate(
    descriptor: &EvidenceDescriptor,
    profile: &str,
    liveness_contracts: &[crate::contract::profile::SimulatorLivenessContract],
    model: &simulator_model::SimulatorExecution,
    detectors: &DetectorRun,
) -> Result<EvaluatedEvidence, Box<dyn Error>> {
    evaluate_with_inventory_issue(
        descriptor,
        profile,
        &BTreeMap::new(),
        liveness_contracts,
        model,
        detectors,
        None,
    )
}

fn evaluate_with_inventory_issue(
    descriptor: &EvidenceDescriptor,
    profile: &str,
    check_contracts: &BTreeMap<String, SimulatorCheckContract>,
    liveness_contracts: &[crate::contract::profile::SimulatorLivenessContract],
    model: &simulator_model::SimulatorExecution,
    detectors: &DetectorRun,
    inventory_issue: Option<&SimulatorIssue>,
) -> Result<EvaluatedEvidence, Box<dyn Error>> {
    let identity = descriptor
        .simulator
        .as_ref()
        .ok_or("simulator descriptor omitted execution identity")?;
    let ModelEvidence {
        mut observations,
        per_check_required_observations,
        simulator_liveness,
        issue,
    } = model_observations(
        profile,
        &descriptor.invariant_id,
        identity,
        check_contracts,
        liveness_contracts,
        &model.events,
    );
    let mut artifacts = model.artifacts.clone();
    let detector_outcome = identity
        .negative_test
        .as_ref()
        .and_then(|test| detectors.outcomes.get(&test.check_id()));
    let detector_passed =
        detector_outcome.is_none_or(|outcome| outcome.status == EvidenceStatus::Pass);
    observations.insert("detector_qualified".to_owned(), u64::from(detector_passed));
    if let Some(outcome) = detector_outcome {
        artifacts.extend(outcome.artifacts.clone());
    }
    let issue = combined_simulator_issue(
        issue,
        inventory_issue,
        identity,
        model,
        detectors,
        detector_outcome,
    );
    if let Some(issue) = issue {
        if identity.liveness_report.is_some() {
            observations.insert(identity.required_observation.clone(), 0);
        }
        return Ok(evaluate_issue(
            issue,
            observations,
            artifacts,
            resource_metrics(model, detector_outcome),
        ));
    }
    let resources = resource_metrics(model, detector_outcome);
    if !detector_passed {
        return Ok(EvaluatedEvidence {
            completion: CheckCompletion::HarnessError,
            status: EvidenceStatus::Error,
            classification: Some(FailureClassification::HarnessError),
            message: Some("detector qualification fixture did not pass".to_owned()),
            observations,
            simulator_liveness: None,
            artifacts,
            duration_ms: resources.duration_ms,
            peak_rss_kib: resources.peak_rss_kib,
        });
    }
    let coverage = coverage_reached(identity, &observations, &per_check_required_observations);
    if coverage {
        return Ok(EvaluatedEvidence {
            completion: if identity.liveness_report.is_some() {
                CheckCompletion::Completed
            } else {
                CheckCompletion::FrontierExhausted
            },
            status: EvidenceStatus::Pass,
            classification: None,
            message: None,
            observations,
            simulator_liveness,
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
        observations,
        simulator_liveness: None,
        artifacts,
        duration_ms: resources.duration_ms,
        peak_rss_kib: resources.peak_rss_kib,
    })
}

fn combined_simulator_issue(
    mut issue: Option<SimulatorIssue>,
    inventory_issue: Option<&SimulatorIssue>,
    identity: &SimulatorIdentity,
    model: &simulator_model::SimulatorExecution,
    detectors: &DetectorRun,
    detector_outcome: Option<&TestOutcome>,
) -> Option<SimulatorIssue> {
    merge_issue(&mut issue, inventory_issue.cloned());
    if !model.processes_succeeded {
        let message = if model.harness_errors.is_empty() {
            "simulator profile process did not complete successfully".to_owned()
        } else {
            model.harness_errors.join("; ")
        };
        merge_issue(&mut issue, Some(SimulatorIssue::HarnessError(message)));
    }
    merge_issue(
        &mut issue,
        detectors
            .harness_error
            .as_ref()
            .map(|error| SimulatorIssue::HarnessError(error.clone())),
    );
    if identity.negative_test.is_some() && detector_outcome.is_none() {
        merge_issue(
            &mut issue,
            Some(SimulatorIssue::HarnessError(
                "detector result is missing".to_owned(),
            )),
        );
    }
    if detector_outcome.is_some_and(|outcome| outcome.status != EvidenceStatus::Pass) {
        merge_issue(
            &mut issue,
            Some(SimulatorIssue::HarnessError(
                "detector qualification fixture did not pass".to_owned(),
            )),
        );
    }
    issue
}

fn resource_metrics(
    model: &simulator_model::SimulatorExecution,
    detector: Option<&TestOutcome>,
) -> ResourceMetrics {
    // Detector compilation is paid by the aggregate run; compile-only outcomes have no check runtime.
    let detector = detector.filter(|outcome| {
        !outcome
            .artifacts
            .iter()
            .any(|artifact| artifact.kind == "compile-log")
    });
    ResourceMetrics {
        duration_ms: model
            .duration_ms
            .saturating_add(detector.map_or(0, |outcome| outcome.duration_ms)),
        peak_rss_kib: model
            .runtime_peak_rss_kib
            .max(detector.map_or(0, |outcome| outcome.peak_rss_kib)),
    }
}

fn execution_resource_metrics(
    model: &simulator_model::SimulatorExecution,
    detectors: &DetectorRun,
) -> ResourceMetrics {
    ResourceMetrics {
        duration_ms: model
            .build_duration_ms
            .saturating_add(model.duration_ms)
            .saturating_add(detectors.duration_ms),
        peak_rss_kib: model
            .build_peak_rss_kib
            .max(model.runtime_peak_rss_kib)
            .max(detectors.peak_rss_kib),
    }
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
#[path = "simulator_detector_identity_tests.rs"]
mod detector_identity_tests;

fn model_observations(
    profile: &str,
    invariant_id: &str,
    identity: &SimulatorIdentity,
    check_contracts: &BTreeMap<String, SimulatorCheckContract>,
    liveness_contracts: &[crate::contract::profile::SimulatorLivenessContract],
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

fn liveness_contracts(
    descriptors: &[EvidenceDescriptor],
) -> Result<Vec<crate::contract::profile::SimulatorLivenessContract>, Box<dyn Error>> {
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

fn simulator_check_contract_issue(
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

fn coverage_reached(
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

#[cfg(test)]
#[path = "simulator_tests.rs"]
mod tests;
