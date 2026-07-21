//! Cross-bundle result collection and ambiguity handling.

use std::collections::{BTreeMap, BTreeSet};

use crate::{
    contract::{
        catalog::EvidenceDescriptor,
        profile::{EvidenceLayer, ProfileContract},
    },
    evidence::{ArtifactRef, EvidenceResult, ResultBundle},
};

use super::{execution, structure};
use crate::verification::intake::IntakeDefect;

#[derive(Clone, Copy)]
pub(in crate::verification::intake) struct ReceiptExpectation<'a> {
    pub(in crate::verification::intake) evidence: &'a BTreeMap<String, &'a EvidenceDescriptor>,
    pub(in crate::verification::intake) contract: &'a ProfileContract,
    pub(in crate::verification::intake) profile: &'a str,
    pub(in crate::verification::intake) source_ref: &'a str,
}

pub(in crate::verification::intake) struct ReceiptCollection {
    pub(in crate::verification::intake) accepted: BTreeMap<String, EvidenceResult>,
    pub(in crate::verification::intake) defects: Vec<IntakeDefect>,
    pub(in crate::verification::intake) artifacts: Vec<ArtifactRef>,
}

pub(in crate::verification::intake) fn collect_results(
    bundles: &[ResultBundle],
    expectation: ReceiptExpectation<'_>,
) -> ReceiptCollection {
    let mut accepted = BTreeMap::<String, EvidenceResult>::new();
    let mut ambiguous = BTreeSet::new();
    let mut defects = Vec::new();
    let mut artifacts = BTreeSet::new();
    for bundle in bundles {
        if bundle.schema_version != crate::evidence::RESULT_SCHEMA_VERSION {
            defects.push(IntakeDefect::malformed(format!(
                "runner {} used unsupported result schema {}",
                bundle.runner, bundle.schema_version
            )));
            continue;
        }
        if bundle.profile != expectation.profile {
            defects.push(IntakeDefect::stale(format!(
                "runner {} reported profile {} instead of {}",
                bundle.runner, bundle.profile, expectation.profile
            )));
            continue;
        }
        if bundle.source_ref != expectation.source_ref {
            defects.push(IntakeDefect::stale(format!(
                "runner {} evidence is stale: source {} != {}",
                bundle.runner, bundle.source_ref, expectation.source_ref
            )));
            continue;
        }
        let Some(layer) = selected_layer(expectation.contract, &bundle.runner) else {
            defects.push(IntakeDefect::unverifiable(format!(
                "runner {} is not selected by profile {}",
                bundle.runner, expectation.profile
            )));
            continue;
        };
        let Some(runner_contract) = expectation.contract.runners.get(&bundle.runner) else {
            defects.push(IntakeDefect::unverifiable(format!(
                "runner {} has no contract in profile {}",
                bundle.runner, expectation.profile
            )));
            continue;
        };
        if let Err(message) = execution::validate(
            bundle,
            layer,
            expectation.contract,
            runner_contract,
            expectation.evidence,
        ) {
            defects.push(IntakeDefect::unverifiable(format!(
                "runner {}: {message}",
                bundle.runner
            )));
            continue;
        }
        artifacts.extend(bundle.execution.artifacts.iter().cloned());
        collect_bundle_results(
            bundle,
            expectation.evidence,
            &mut accepted,
            &mut ambiguous,
            &mut defects,
        );
    }
    ReceiptCollection {
        accepted,
        defects,
        artifacts: artifacts.into_iter().collect(),
    }
}

fn selected_layer(contract: &ProfileContract, runner: &str) -> Option<EvidenceLayer> {
    contract
        .required_layers
        .iter()
        .copied()
        .find(|layer| layer.as_str() == runner)
}

fn collect_bundle_results(
    bundle: &ResultBundle,
    expected: &BTreeMap<String, &EvidenceDescriptor>,
    accepted: &mut BTreeMap<String, EvidenceResult>,
    ambiguous: &mut BTreeSet<String>,
    defects: &mut Vec<IntakeDefect>,
) {
    for result in &bundle.results {
        let Some(descriptor) = expected.get(&result.evidence_id) else {
            defects.push(IntakeDefect::malformed(format!(
                "runner {} reported unknown evidence {}",
                bundle.runner, result.evidence_id
            )));
            continue;
        };
        if result.invariant_id != descriptor.invariant_id || bundle.runner != descriptor.layer {
            defects.push(IntakeDefect::malformed(format!(
                "evidence {} identity does not match registry invariant/layer",
                result.evidence_id
            )));
            continue;
        }
        if let Err(message) = structure::validate_result(result) {
            defects.push(IntakeDefect::malformed(format!(
                "evidence {}: {message}",
                result.evidence_id
            )));
            continue;
        }
        if ambiguous.contains(&result.evidence_id) {
            continue;
        }
        if accepted.contains_key(&result.evidence_id) {
            accepted.remove(&result.evidence_id);
            ambiguous.insert(result.evidence_id.clone());
            defects.push(IntakeDefect::unverifiable(format!(
                "duplicate result for evidence {}",
                result.evidence_id
            )));
        } else {
            accepted.insert(result.evidence_id.clone(), result.clone());
        }
    }
}
