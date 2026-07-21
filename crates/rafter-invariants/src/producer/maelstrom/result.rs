//! Binding one scenario verdict to reviewed invariant evidence results.

use crate::{
    contract::catalog::EvidenceDescriptor,
    evidence::{ArtifactRef, EvidenceResult, EvidenceStatus, FailureClassification},
};

use super::evaluation::ScenarioVerdict;

pub(super) fn evidence_result(
    descriptor: &EvidenceDescriptor,
    execution_id: &str,
    verdict: &ScenarioVerdict,
    artifacts: &[ArtifactRef],
) -> EvidenceResult {
    let (status, classification, message) = match verdict {
        ScenarioVerdict::Pass => (EvidenceStatus::Pass, None, None),
        ScenarioVerdict::Counterexample {
            rd05,
            rd06,
            harness_error,
        } if verdict.targets(&descriptor.invariant_id) => (
            EvidenceStatus::Fail,
            Some(FailureClassification::InvariantViolation),
            Some(if descriptor.invariant_id == "RD-05" && *rd05 {
                let mut message =
                    "an isolated leader renewed its expired lease or served the buffered read"
                        .to_owned();
                if *harness_error {
                    message.push_str("; a later harness error was also observed");
                }
                message
            } else if descriptor.invariant_id == "RD-06" && *rd06 {
                let mut message = "Maelstrom reported a non-linearizable client history".to_owned();
                if *harness_error {
                    message.push_str("; a later harness error was also observed");
                }
                message
            } else {
                unreachable!("targeted Maelstrom counterexample has a known invariant")
            }),
        ),
        ScenarioVerdict::Counterexample { .. } => (
            EvidenceStatus::Incomplete,
            Some(FailureClassification::CoverageNotReached),
            Some("Maelstrom found a client counterexample that cannot be attributed to this supporting invariant".to_owned()),
        ),
        ScenarioVerdict::Incomplete(message) => (
            EvidenceStatus::Incomplete,
            Some(FailureClassification::CoverageNotReached),
            Some(message.clone()),
        ),
        ScenarioVerdict::Error(message) => (
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
