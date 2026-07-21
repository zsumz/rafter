//! Primitive shape checks for receipt-owned wire values.

use crate::evidence::{
    ArtifactRef, CheckReceipt, EvidenceResult, EvidenceStatus, FailureClassification, PlanInput,
};

pub(super) fn valid_plan_input(input: &PlanInput) -> bool {
    !input.path.trim().is_empty() && input.size_bytes > 0 && is_sha256(&input.sha256)
}

pub(super) fn valid_check(check: &CheckReceipt, require_peak_rss: bool) -> bool {
    !check.execution_id.trim().is_empty()
        && !check.check_id.trim().is_empty()
        && !check.evidence_ids.is_empty()
        && (!require_peak_rss || check.peak_rss_kib > 0)
        && !check.artifacts.is_empty()
        && check.artifacts.iter().all(valid_artifact)
}

pub(super) fn valid_artifact(artifact: &ArtifactRef) -> bool {
    !artifact.kind.trim().is_empty()
        && !artifact.path.trim().is_empty()
        && artifact.size_bytes > 0
        && is_sha256(&artifact.sha256)
}

pub(super) fn is_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

pub(super) fn validate_result(result: &EvidenceResult) -> Result<(), &'static str> {
    let expected = match result.status {
        EvidenceStatus::Pass => None,
        EvidenceStatus::Fail => Some(FailureClassification::InvariantViolation),
        EvidenceStatus::Incomplete => Some(FailureClassification::CoverageNotReached),
        EvidenceStatus::Error => Some(FailureClassification::HarnessError),
    };
    if result.classification != expected {
        return Err("status and classification disagree");
    }
    if result.status != EvidenceStatus::Pass
        && result
            .message
            .as_deref()
            .is_none_or(|message| message.trim().is_empty())
    {
        return Err("non-pass result must include a message");
    }
    if result.status != EvidenceStatus::Pass && result.artifacts.is_empty() {
        return Err("non-pass result must preserve a replay or log artifact");
    }
    if result
        .artifacts
        .iter()
        .any(|artifact| !valid_artifact(artifact))
    {
        return Err("result contains a malformed artifact reference");
    }
    Ok(())
}
