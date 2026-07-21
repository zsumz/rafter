//! Per-descriptor simulator evidence evaluation and receipt assembly contract.

use std::{collections::BTreeMap, error::Error};

#[cfg(test)]
use crate::contract::{catalog::Catalog, profile::ProfileManifest};
use crate::{
    contract::{
        catalog::EvidenceDescriptor,
        profile::{SimulatorCheckContract, SimulatorLivenessContract},
    },
    evidence::{CheckReceipt, EvidenceResult, EvidenceStatus},
};

use crate::producer::artifact;

use super::model::SimulatorExecution;

#[cfg(test)]
use super::{
    check_contract::liveness_contracts, issue::SimulatorIssue, verdict::EvaluatedEvidence,
};
use super::{
    detector::DetectorRun, events::simulator_event_inventory_issue,
    verdict::evaluate_with_inventory_issue,
};

pub(super) fn evaluate_descriptors(
    descriptors: &[EvidenceDescriptor],
    profile: &str,
    check_contracts: &BTreeMap<String, SimulatorCheckContract>,
    liveness_contracts: &[SimulatorLivenessContract],
    model: &SimulatorExecution,
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
    manifest: &ProfileManifest,
    profile: &str,
    model: &SimulatorExecution,
) -> Result<(Vec<CheckReceipt>, Vec<EvidenceResult>), Box<dyn Error>> {
    let descriptors = catalog
        .evidence
        .iter()
        .filter(|descriptor| descriptor.layer == "simulator")
        .cloned()
        .collect::<Vec<_>>();
    let contracts = liveness_contracts(&descriptors)?;
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

#[cfg(test)]
pub(super) fn evaluate(
    descriptor: &EvidenceDescriptor,
    profile: &str,
    liveness_contracts: &[SimulatorLivenessContract],
    model: &SimulatorExecution,
    detectors: &DetectorRun,
) -> Result<EvaluatedEvidence, Box<dyn Error>> {
    evaluate_with_inventory_issue(
        descriptor,
        profile,
        &BTreeMap::new(),
        liveness_contracts,
        model,
        detectors,
        None::<&SimulatorIssue>,
    )
}
