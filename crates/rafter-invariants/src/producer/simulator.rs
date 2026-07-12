use std::{collections::BTreeMap, error::Error, path::Path, time::Instant};

use serde_json::Value;

use crate::types::RESULT_SCHEMA_VERSION;
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
    artifacts: Vec<crate::ArtifactRef>,
}

struct DetectorRun {
    outcomes: BTreeMap<String, TestOutcome>,
    artifacts: Vec<crate::ArtifactRef>,
    peak_rss_kib: u64,
}

pub(super) fn run(
    catalog: &Catalog,
    contract: &ProfileContract,
    profile: &str,
    source: SourceReceipt,
    output_dir: &Path,
    context: &ProducerContext<'_>,
) -> Result<ResultBundle, Box<dyn Error>> {
    let started = Instant::now();
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
    let mut checks = Vec::with_capacity(descriptors.len());
    let mut results = Vec::with_capacity(descriptors.len());
    for descriptor in &descriptors {
        let execution_id = artifact::stable_id("simulator", &descriptor.evidence_id());
        let evaluated = evaluate(descriptor, &model, &detectors)?;
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
            duration_ms: model.duration_ms,
            peak_rss_kib: model.peak_rss_kib.max(detectors.peak_rss_kib),
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
            source,
            checks,
            duration_ms: process::duration_ms(started.elapsed()),
            peak_rss_kib: model.peak_rss_kib.max(detectors.peak_rss_kib),
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
    let identities = descriptors
        .iter()
        .filter_map(|descriptor| descriptor.simulator.as_ref()?.negative_test.clone())
        .collect::<Vec<_>>();
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
        outcomes.insert(identity.test_name, outcome);
    }
    Ok(DetectorRun {
        outcomes,
        artifacts,
        peak_rss_kib,
    })
}

fn evaluate(
    descriptor: &EvidenceDescriptor,
    model: &simulator_model::SimulatorExecution,
    detectors: &DetectorRun,
) -> Result<EvaluatedEvidence, Box<dyn Error>> {
    let identity = descriptor
        .simulator
        .as_ref()
        .ok_or("simulator descriptor omitted execution identity")?;
    let mut observations = model_observations(identity, &model.events);
    let mut artifacts = model.artifacts.clone();
    let detector_passed = match &identity.negative_test {
        Some(test) => {
            let outcome = detectors
                .outcomes
                .get(&test.test_name)
                .ok_or("detector result is missing")?;
            artifacts.extend(outcome.artifacts.clone());
            outcome.status == EvidenceStatus::Pass
        }
        None => true,
    };
    observations.insert("detector_qualified".to_owned(), u64::from(detector_passed));
    let coverage = coverage_reached(identity, &observations);
    if model.processes_succeeded && detector_passed && coverage {
        return Ok(EvaluatedEvidence {
            completion: if identity.required_liveness_feature.is_some() {
                CheckCompletion::Completed
            } else {
                CheckCompletion::FrontierExhausted
            },
            status: EvidenceStatus::Pass,
            classification: None,
            message: None,
            observations,
            artifacts,
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
            observations,
            artifacts,
        });
    }
    Ok(EvaluatedEvidence {
        completion: CheckCompletion::CoverageNotReached,
        status: EvidenceStatus::Incomplete,
        classification: Some(FailureClassification::CoverageNotReached),
        message: Some("required semantic simulator coverage was not reached".to_owned()),
        observations,
        artifacts,
    })
}

fn model_observations(
    identity: &SimulatorIdentity,
    events: &BTreeMap<String, Vec<Value>>,
) -> BTreeMap<String, u64> {
    let mut observations = BTreeMap::new();
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
            merge_event_observations(event, &mut observations);
        }
    }
    observations
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
    if let Some(feature) = &identity.required_liveness_feature {
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
            && !feature.is_empty();
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
