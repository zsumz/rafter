//! Contract-bound cross-bundle receipt acceptance.

use std::collections::{BTreeMap, BTreeSet};

use super::{EvidenceIntake, IntakeDefect, VerificationRequest};
use crate::{
    contract::catalog::EvidenceDescriptor,
    evidence::{EvidenceStatus, ResultBundle},
    verification::AggregateError,
};

pub(super) fn accept(
    request: VerificationRequest<'_>,
    bundles: &[ResultBundle],
    mut defects: Vec<IntakeDefect>,
) -> Result<EvidenceIntake, AggregateError> {
    request
        .manifest
        .validate(request.catalog)
        .map_err(|error| AggregateError::new(error.to_string()))?;
    let profile = request.active_plan.profile.as_str();
    let contract = request
        .manifest
        .profiles
        .get(profile)
        .ok_or_else(|| AggregateError::new(format!("unknown profile {profile}")))?;
    if request.active_plan.contract != *contract {
        return Err(AggregateError::new(
            "active plan contract does not match the selected profile".to_owned(),
        ));
    }
    let required = request.catalog.required_evidence(contract);
    let expected = required
        .values()
        .flatten()
        .map(|evidence| (evidence.evidence_id(), evidence))
        .collect::<BTreeMap<_, _>>();
    let (accepted, receipt_defects, artifacts) =
        crate::receipt::collect_results(bundles, &expected, contract, profile, request.source_ref);
    defects.extend(receipt_defects);
    Ok(EvidenceIntake::new(
        profile,
        request.source_ref,
        accepted,
        artifacts,
        defects,
    ))
}

#[cfg(test)]
pub(crate) fn verify_receipts_for_test(
    request: VerificationRequest<'_>,
    bundles: &[ResultBundle],
    defects: Vec<IntakeDefect>,
) -> Result<EvidenceIntake, AggregateError> {
    accept(request, bundles, defects)
}

pub(crate) fn require_passing_layer(
    request: VerificationRequest<'_>,
    layer: &str,
    intake: &EvidenceIntake,
) -> Result<(), AggregateError> {
    request
        .manifest
        .validate(request.catalog)
        .map_err(|error| AggregateError::new(error.to_string()))?;
    let profile = request.active_plan.profile.as_str();
    let contract = request
        .manifest
        .profiles
        .get(profile)
        .ok_or_else(|| AggregateError::new(format!("unknown profile {profile}")))?;
    if !intake.defects().is_empty() {
        return Err(AggregateError::new(
            intake
                .defects()
                .iter()
                .map(IntakeDefect::message)
                .collect::<Vec<_>>()
                .join("; "),
        ));
    }
    let expected_layer = request
        .catalog
        .required_evidence(contract)
        .values()
        .flatten()
        .filter(|descriptor| descriptor.layer == layer)
        .map(EvidenceDescriptor::evidence_id)
        .collect::<BTreeSet<_>>();
    let accepted_layer = intake
        .accepted()
        .iter()
        .filter(|(evidence_id, _)| expected_layer.contains(*evidence_id))
        .map(|(evidence_id, _)| evidence_id.clone())
        .collect::<BTreeSet<_>>();
    if accepted_layer != expected_layer
        || intake
            .accepted()
            .iter()
            .filter(|(evidence_id, _)| expected_layer.contains(*evidence_id))
            .any(|(_, result)| result.status != EvidenceStatus::Pass)
    {
        return Err(AggregateError::new(format!(
            "{profile}/{layer} evidence is missing, incomplete, or red"
        )));
    }
    Ok(())
}
