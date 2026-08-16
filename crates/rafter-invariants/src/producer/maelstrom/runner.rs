//! Maelstrom trial orchestration, evidence assembly, and scenario reduction.

use std::{collections::BTreeSet, error::Error, path::Path};

use crate::evidence::{ArtifactRef, RESULT_SCHEMA_VERSION};
use crate::{
    contract::{catalog::Catalog, profile::ProfileContract},
    evidence::{CheckReceipt, ExecutionReceipt, ResultBundle, SourceReceipt},
};

use super::{
    binding::bind_counterexamples,
    evaluation::{evaluate, observations},
    result::evidence_result,
    scenario::{required_configuration, scenario_for},
    source,
    trial::{run_trial, Scenario},
    ProducerContext,
};

pub(in crate::producer) fn run(
    catalog: &Catalog,
    contract: &ProfileContract,
    profile: &str,
    source: SourceReceipt,
    output_dir: &Path,
    context: &ProducerContext<'_>,
) -> Result<ResultBundle, Box<dyn Error>> {
    let runner = contract
        .runners
        .get("maelstrom")
        .ok_or("Maelstrom runner missing")?;
    let trials = required_configuration(&runner.configuration, "trials")?.parse::<u64>()?;
    let descriptors = catalog
        .required_evidence(contract)
        .into_values()
        .flatten()
        .filter(|descriptor| descriptor.layer == "maelstrom")
        .collect::<Vec<_>>();
    let mut checks = Vec::new();
    let mut results = Vec::new();
    let mut execution_artifacts = Vec::new();
    let mut peak_rss_kib = 0;
    for scenario in Scenario::ALL {
        let evidence = descriptors
            .iter()
            .filter(|descriptor| scenario_for(descriptor) == Some(scenario))
            .collect::<Vec<_>>();
        if evidence.is_empty() {
            return Err(format!(
                "Maelstrom scenario {} has no registry evidence",
                scenario.name()
            )
            .into());
        }
        let mut outcomes = Vec::new();
        for trial in 0..trials {
            let outcome = run_trial(
                scenario,
                trial,
                profile,
                &source.commit,
                &runner.configuration,
                output_dir,
            )?;
            peak_rss_kib = peak_rss_kib.max(outcome.peak_rss_kib);
            execution_artifacts.extend(outcome.artifacts.iter().cloned());
            outcomes.push(outcome);
        }
        let verdict = evaluate(scenario, &outcomes);
        let execution_id =
            super::artifact::stable_id("maelstrom", &format!("{profile}/{}", scenario.name()));
        let evidence_ids = evidence
            .iter()
            .map(|descriptor| descriptor.evidence_id())
            .collect::<Vec<_>>();
        let artifacts = deduplicated(
            outcomes
                .iter()
                .flat_map(|outcome| outcome.artifacts.iter().cloned()),
        );
        results.extend(
            evidence
                .iter()
                .map(|descriptor| evidence_result(descriptor, &execution_id, &verdict, &artifacts)),
        );
        checks.push(CheckReceipt {
            execution_id,
            check_id: format!("maelstrom/{}", scenario.name()),
            evidence_ids,
            completion: verdict.completion(),
            observations: observations(&outcomes),
            simulator_liveness: None,
            tla_continuation: None,
            duration_ms: outcomes.iter().map(|outcome| outcome.duration_ms).sum(),
            peak_rss_kib: outcomes
                .iter()
                .map(|outcome| outcome.peak_rss_kib)
                .max()
                .unwrap_or_default(),
            artifacts,
        });
    }
    bind_counterexamples(&mut checks, &mut results)?;
    source::verify(&source)?;
    let execution_duration_ms = checks.iter().map(|check| check.duration_ms).sum();
    Ok(ResultBundle {
        schema_version: RESULT_SCHEMA_VERSION,
        runner: "maelstrom".to_owned(),
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

/// Collapses repeated references to one artifact into a single entry.
///
/// Tool inputs -- the runner script, the Maelstrom jar, the node and proxy
/// binaries -- are captured once per source tree into a shared, content
/// addressed namespace, but the capture runs once per trial. A single-trial
/// check therefore lists each of them once and a three-trial check listed each
/// of them three times, describing one file as three artifacts.
///
/// That is a defect in the claim, not in the verifier that rejected it: an
/// artifact reference *is* its (kind, path, digest, size) identity, so
/// repeating one conveys nothing, spends the receipt's reference budget three
/// times over, and makes "exactly one runner per check" unsatisfiable. Order is
/// preserved and single-trial receipts are unchanged, since they contain no
/// duplicates to collapse.
fn deduplicated(artifacts: impl IntoIterator<Item = ArtifactRef>) -> Vec<ArtifactRef> {
    let mut seen = BTreeSet::new();
    artifacts
        .into_iter()
        .filter(|artifact| seen.insert(artifact.clone()))
        .collect()
}

#[cfg(test)]
#[path = "runner_tests.rs"]
mod tests;
