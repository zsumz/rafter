//! Aggregate-only detector replay and evidence-local qualification overlay.

use std::collections::BTreeMap;

use super::{paths::SourceVerifierState, EvidenceIntake, VerificationRequest};
use crate::verification::detector_replay::{self, DetectorReplayAssessment, ReplayEvidence};

pub(super) fn apply(
    request: VerificationRequest<'_>,
    source_state: &mut SourceVerifierState,
    intake: &mut EvidenceIntake,
) {
    let profile = request.active_plan.profile.as_str();
    let Some(profile_contract) = request.manifest.profiles.get(profile) else {
        return;
    };
    let Some(verifier) = request.manifest.verifiers.get(profile) else {
        return;
    };
    let contract = &verifier.detector_replay;
    let inventory = detector_replay::required_evidence(request.catalog, profile_contract);
    let assessment = execute(
        request,
        source_state,
        profile_contract,
        contract,
        &inventory,
    );
    let assessment = match assessment {
        Ok(assessment) => validate_coverage(assessment, &inventory),
        Err(error) => detector_replay::qualification_failure(
            inventory.clone(),
            &format!("detector replay harness error: {error}"),
            Vec::new(),
        ),
    };
    match assessment.and_then(|assessment| {
        intake
            .apply_detector_replay(assessment)
            .map_err(|error| format!("apply detector replay qualification: {error}"))
    }) {
        Ok(()) => {}
        Err(error) => apply_fallback(intake, inventory, &error),
    }
}

fn execute(
    request: VerificationRequest<'_>,
    source_state: &mut SourceVerifierState,
    profile_contract: &crate::contract::profile::ProfileContract,
    contract: &crate::contract::profile::DetectorReplayContract,
    inventory: &[ReplayEvidence],
) -> Result<DetectorReplayAssessment, Box<dyn std::error::Error>> {
    let profile = request.active_plan.profile.as_str();
    let SourceVerifierState::Ready(source_verifier) = source_state else {
        return Err("authenticated source is unavailable for detector replay".into());
    };
    let deadlines = detector_replay::deadlines(contract)?;
    let receipts = source_verifier.replay_receipts()?;
    let replay = match detector_replay::prepare_bounded(
        request.catalog,
        profile_contract,
        contract,
        source_verifier.source_root(),
        deadlines.work(),
    ) {
        Ok(replay) => replay,
        Err(error) => {
            return detector_replay::publish_preparation_failure(
                detector_replay::PreparationFailureRequest {
                    inventory: inventory.to_vec(),
                    replay: None,
                    receipts,
                    contract,
                    profile,
                    source_ref: request.source_ref,
                    registry: None,
                    message: &format!("prepare detector replay inventory: {error}"),
                    deadlines,
                },
            );
        }
    };
    let source = match source_verifier.prepare_compilation_source(
        crate::verification::source::RegistryMaterializationPolicy {
            required_packages: contract.required_registry_packages,
            maximum_archive_bytes: contract.maximum_registry_archive_bytes,
            maximum_expanded_bytes: contract.maximum_registry_expanded_bytes,
            maximum_entries: contract.maximum_registry_entries,
            deadline: deadlines.work(),
        },
    ) {
        Ok(source) => source,
        Err(error) => {
            return detector_replay::publish_preparation_failure(
                detector_replay::PreparationFailureRequest {
                    inventory: inventory.to_vec(),
                    replay: Some(&replay),
                    receipts,
                    contract,
                    profile,
                    source_ref: request.source_ref,
                    registry: None,
                    message: &format!("materialize authenticated registry source: {error}"),
                    deadlines,
                },
            );
        }
    };
    if source.registry_package_count() != contract.required_registry_packages {
        let message = format!(
            "authenticated registry contains {} packages instead of the required {}",
            source.registry_package_count(),
            contract.required_registry_packages
        );
        return detector_replay::publish_preparation_failure(
            detector_replay::PreparationFailureRequest {
                inventory: inventory.to_vec(),
                replay: Some(&replay),
                receipts,
                contract,
                profile,
                source_ref: request.source_ref,
                registry: Some(source.registry_receipt()),
                message: &message,
                deadlines,
            },
        );
    }
    detector_replay::execute(
        &replay,
        &source,
        contract,
        profile,
        request.source_ref,
        deadlines,
    )
}

pub(super) fn validate_coverage(
    assessment: DetectorReplayAssessment,
    inventory: &[ReplayEvidence],
) -> Result<DetectorReplayAssessment, String> {
    let expected = inventory
        .iter()
        .map(|evidence| {
            (
                evidence.evidence_id.as_str(),
                evidence.invariant_id.as_str(),
            )
        })
        .collect::<BTreeMap<_, _>>();
    let observed = assessment
        .qualifications
        .iter()
        .map(|(evidence_id, qualification)| (evidence_id.as_str(), qualification.invariant_id()))
        .collect::<BTreeMap<_, _>>();
    if observed != expected {
        let message = format!(
            "detector replay covered {} evidence records instead of the required {}",
            observed.len(),
            expected.len()
        );
        return assessment.fail_closed(inventory.iter().cloned(), &message);
    }
    Ok(assessment)
}

pub(super) fn apply_fallback(
    intake: &mut EvidenceIntake,
    inventory: Vec<ReplayEvidence>,
    error: &str,
) {
    let fallback = detector_replay::qualification_failure(
        inventory,
        &format!("detector replay qualification failed closed: {error}"),
        Vec::new(),
    )
    .and_then(|fallback| intake.apply_detector_replay(fallback));
    if let Err(fallback_error) = fallback {
        intake.extend_defects([super::IntakeDefect::unverifiable(format!(
            "detector replay fallback could not be applied after {error}: {fallback_error}"
        ))]);
    }
}
