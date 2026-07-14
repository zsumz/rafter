use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    path::Path,
};

use crate::types::RESULT_SCHEMA_VERSION;
use crate::{
    catalog::{Catalog, ProfileContract},
    CheckReceipt, EvidenceDescriptor, EvidenceResult, EvidenceStatus, ExecutionReceipt,
    ResultBundle, SourceReceipt, TestIdentity,
};

use super::{
    artifact, process, source,
    test_compile::{compile, prepare_target_dir, CompiledTarget, Target},
    ProducerContext,
};

type TestEvidence = BTreeMap<TestIdentity, Vec<EvidenceDescriptor>>;

struct CheckResults {
    checks: Vec<CheckReceipt>,
    results: Vec<EvidenceResult>,
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
    let runner = contract
        .runners
        .get("tests")
        .ok_or("tests runner missing")?;
    let target_dir = prepare_target_dir(profile, &source.commit)?;
    let mut build_environment = process::base_environment();
    build_environment.insert(
        "CARGO_TARGET_DIR".to_owned(),
        target_dir.to_string_lossy().into_owned(),
    );
    let identities = test_evidence(catalog, contract);
    let targets = identities.keys().map(Target::from).collect::<BTreeSet<_>>();
    let mut compiled = BTreeMap::new();
    let mut execution_artifacts = Vec::new();
    let mut peak_rss_kib = 0;
    let mut compile_duration_ms = 0_u64;
    for target in targets {
        let outcome = compile(
            &target,
            profile,
            &source.commit,
            &build_environment,
            output_dir,
        )?;
        peak_rss_kib = peak_rss_kib.max(outcome.peak_rss_kib);
        compile_duration_ms = compile_duration_ms.saturating_add(outcome.duration_ms);
        execution_artifacts.push(outcome.artifact.clone());
        compiled.insert(target, outcome);
    }

    let check_results = run_checks(identities, &compiled, profile, &source.commit, output_dir)?;
    peak_rss_kib = peak_rss_kib.max(check_results.peak_rss_kib);
    let checks = check_results.checks;
    let results = check_results.results;
    source::verify(&source)?;
    let summary = format!(
        "profile={profile}\nproducer={}\ntargets={}\nchecks={}\nresults={}\n",
        runner.producer,
        compiled.len(),
        checks.len(),
        results.len()
    );
    execution_artifacts.push(artifact::write(
        output_dir,
        Path::new(&format!(
            "{profile}-tests/{}/summary.log",
            source.commit.get(..12).unwrap_or(&source.commit)
        )),
        "summary",
        summary.as_bytes(),
    )?);
    let execution_duration_ms = compile_duration_ms.saturating_add(
        checks
            .iter()
            .filter(|check| {
                !check
                    .artifacts
                    .iter()
                    .any(|artifact| artifact.kind == "compile-log")
            })
            .map(|check| check.duration_ms)
            .sum(),
    );
    Ok(ResultBundle {
        schema_version: RESULT_SCHEMA_VERSION,
        runner: "tests".to_owned(),
        profile: profile.to_owned(),
        source_ref: source.commit.clone(),
        execution: ExecutionReceipt {
            plan: context.plan.clone(),
            invocation: context.invocation.clone(),
            producer: context.producer.clone(),
            source,
            checks,
            duration_ms: execution_duration_ms,
            peak_rss_kib,
            artifacts: execution_artifacts,
        },
        results,
    })
}

fn test_evidence(catalog: &Catalog, contract: &ProfileContract) -> TestEvidence {
    let required = catalog.required_evidence(contract);
    let mut identities = BTreeMap::<TestIdentity, Vec<_>>::new();
    for descriptor in required.values().flatten() {
        if let Some(identity) = &descriptor.test {
            identities
                .entry(identity.clone())
                .or_default()
                .push(descriptor.clone());
        }
    }
    identities
}

fn run_checks(
    identities: TestEvidence,
    compiled: &BTreeMap<Target, CompiledTarget>,
    profile: &str,
    source_ref: &str,
    output_dir: &Path,
) -> Result<CheckResults, Box<dyn Error>> {
    let mut checks = Vec::with_capacity(identities.len());
    let mut results = Vec::new();
    let mut peak_rss_kib = 0;
    for (identity, evidence) in identities {
        let evidence_ids = evidence
            .iter()
            .map(EvidenceDescriptor::evidence_id)
            .collect::<Vec<_>>();
        let check_id = identity.check_id();
        let execution_id = artifact::stable_id("test", &check_id);
        let target = Target::from(&identity);
        let compiled_target = compiled
            .get(&target)
            .ok_or("compiled target inventory changed during execution")?;
        let mut outcome = super::test_exec::evaluate(
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
        results.extend(evidence.into_iter().map(|descriptor| EvidenceResult {
            invariant_id: descriptor.invariant_id.clone(),
            evidence_id: descriptor.evidence_id(),
            execution_id: execution_id.clone(),
            status: outcome.status,
            classification: outcome.classification,
            message: outcome.message.clone(),
            artifacts: if outcome.status == EvidenceStatus::Pass {
                Vec::new()
            } else {
                outcome.artifacts.clone()
            },
        }));
        checks.push(CheckReceipt {
            execution_id,
            check_id,
            evidence_ids,
            completion: outcome.completion,
            observations: outcome.observations,
            simulator_liveness: None,
            duration_ms: outcome.duration_ms,
            peak_rss_kib: outcome.peak_rss_kib,
            artifacts: outcome.artifacts,
        });
    }
    Ok(CheckResults {
        checks,
        results,
        peak_rss_kib,
    })
}
