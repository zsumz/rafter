//! Binding one TLA+ verdict to reviewed invariant evidence results.

use crate::{
    contract::catalog::EvidenceDescriptor,
    evidence::{ArtifactRef, EvidenceResult, EvidenceStatus, FailureClassification},
};

use super::evaluation::TlaVerdict;

pub(in crate::producer) fn evidence_result(
    descriptor: &EvidenceDescriptor,
    execution_id: &str,
    verdict: &TlaVerdict,
    artifacts: &[ArtifactRef],
) -> EvidenceResult {
    let (status, classification, message) = match verdict {
        // Both pass shapes bind identically. The distinction between a drained
        // frontier and an elapsed budget is recorded on the check receipt,
        // where a reader can see it; it is not a difference in what the
        // predicate evidence claims, and a green scheduled tier stays green.
        TlaVerdict::Pass | TlaVerdict::PassBudgetElapsed => (EvidenceStatus::Pass, None, None),
        TlaVerdict::Violation(symbol) if symbol == &descriptor.symbol => (
            EvidenceStatus::Fail,
            Some(FailureClassification::InvariantViolation),
            Some(format!("TLC reported a counterexample for {symbol}")),
        ),
        TlaVerdict::Violation(symbol) => (
            EvidenceStatus::Incomplete,
            Some(FailureClassification::CoverageNotReached),
            Some(format!(
                "TLC stopped at counterexample {symbol} before proving {}",
                descriptor.symbol
            )),
        ),
        TlaVerdict::Incomplete(_, message) => (
            EvidenceStatus::Incomplete,
            Some(FailureClassification::CoverageNotReached),
            Some(message.clone()),
        ),
        TlaVerdict::Error(message) => (
            EvidenceStatus::Error,
            Some(FailureClassification::HarnessError),
            Some(message.clone()),
        ),
    };
    EvidenceResult {
        invariant_id: descriptor.invariant_id.clone(),
        evidence_id: descriptor.evidence_id(),
        execution_id: execution_id.to_owned(),
        status,
        classification,
        message,
        artifacts: if status == EvidenceStatus::Pass {
            Vec::new()
        } else {
            artifacts.to_vec()
        },
    }
}
