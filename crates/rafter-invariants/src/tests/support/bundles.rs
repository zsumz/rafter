//! Complete passing result bundles, grouped the way the runners emit them.
//!
//! Required evidence is grouped by layer and then by physical check identity,
//! so a fixture bundle carries the check fan-out the registry demands rather
//! than one receipt per evidence row.

use super::observations::{
    synthetic_artifacts, synthetic_check_id, synthetic_liveness_binding, synthetic_observations,
};
use super::receipts::{
    artifact, invocation_receipt_for_profile, plan_receipt, producer_binding, synthetic_source,
};
use super::scenario::synthetic_continuation_binding;
use crate::*;

pub(crate) fn passing_bundles(catalog: &Catalog, manifest: &ProfileManifest) -> Vec<ResultBundle> {
    passing_bundles_for_profile(catalog, manifest, "pr")
}

pub(crate) fn passing_bundles_for_profile(
    catalog: &Catalog,
    manifest: &ProfileManifest,
    profile: &str,
) -> Vec<ResultBundle> {
    let required = catalog.required_evidence(&manifest.profiles[profile]);
    evidence_by_runner(required.values().flatten())
        .into_iter()
        .map(|(runner, evidence)| synthetic_bundle(&runner, evidence, manifest, profile))
        .collect()
}

pub(super) fn evidence_by_runner<'a>(
    evidence: impl Iterator<Item = &'a EvidenceDescriptor>,
) -> std::collections::BTreeMap<String, Vec<EvidenceDescriptor>> {
    let mut by_runner = std::collections::BTreeMap::new();
    for descriptor in evidence {
        by_runner
            .entry(descriptor.layer.clone())
            .or_insert_with(Vec::new)
            .push(descriptor.clone());
    }
    by_runner
}

pub(super) fn synthetic_bundle(
    runner: &str,
    evidence: Vec<EvidenceDescriptor>,
    manifest: &ProfileManifest,
    profile: &str,
) -> ResultBundle {
    let (checks, results) = synthetic_checks(runner, evidence, manifest, profile);
    let producer = producer_binding(&format!("artifacts/{runner}-producer"));
    ResultBundle {
        schema_version: crate::evidence::RESULT_SCHEMA_VERSION,
        runner: runner.to_owned(),
        profile: profile.to_owned(),
        source_ref: "abc".to_owned(),
        execution: ExecutionReceipt {
            plan: plan_receipt(manifest, profile),
            invocation: invocation_receipt_for_profile(runner, profile),
            producer: producer.clone(),
            source: synthetic_source(runner, manifest, profile),
            checks,
            duration_ms: 1,
            peak_rss_kib: 1,
            artifacts: vec![
                artifact(&format!("artifacts/{runner}-summary.log")),
                producer.executable,
            ],
        },
        results,
    }
}

pub(super) fn synthetic_checks(
    runner: &str,
    evidence: Vec<EvidenceDescriptor>,
    manifest: &ProfileManifest,
    profile: &str,
) -> (Vec<CheckReceipt>, Vec<EvidenceResult>) {
    let mut groups = std::collections::BTreeMap::<String, Vec<EvidenceDescriptor>>::new();
    for descriptor in evidence {
        groups
            .entry(synthetic_check_id(&descriptor))
            .or_default()
            .push(descriptor);
    }
    let mut results = Vec::new();
    let checks = groups
        .into_iter()
        .enumerate()
        .map(|(index, (check_id, descriptors))| {
            let execution_id = format!("{runner}-execution-{index}");
            let evidence_ids = descriptors
                .iter()
                .map(EvidenceDescriptor::evidence_id)
                .collect::<Vec<_>>();
            results.extend(descriptors.iter().map(|descriptor| EvidenceResult {
                invariant_id: descriptor.invariant_id.clone(),
                evidence_id: descriptor.evidence_id(),
                execution_id: execution_id.clone(),
                status: EvidenceStatus::Pass,
                classification: None,
                message: None,
                artifacts: Vec::new(),
            }));
            CheckReceipt {
                execution_id,
                check_id,
                evidence_ids,
                completion: if runner == "tla" {
                    CheckCompletion::FrontierExhausted
                } else {
                    CheckCompletion::Completed
                },
                observations: synthetic_observations(&descriptors, manifest, profile),
                simulator_liveness: synthetic_liveness_binding(&descriptors[0], profile),
                tla_continuation: (runner == "tla")
                    .then(|| synthetic_continuation_binding(manifest, profile)),
                duration_ms: 1,
                peak_rss_kib: 1,
                artifacts: synthetic_artifacts(&descriptors[0], manifest, profile),
            }
        })
        .collect();
    (checks, results)
}
