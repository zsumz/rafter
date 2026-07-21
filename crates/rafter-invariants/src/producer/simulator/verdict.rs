//! Simulator evidence verdict and failure-receipt contract.

use std::{collections::BTreeMap, error::Error};

use crate::{
    contract::{
        catalog::EvidenceDescriptor,
        profile::{SimulatorCheckContract, SimulatorLivenessContract},
    },
    evidence::{
        ArtifactRef, CheckCompletion, EvidenceStatus, FailureClassification,
        SimulatorLivenessBinding,
    },
};

use crate::producer::simulator_model::SimulatorExecution;

use super::{
    detector::DetectorRun,
    issue::{combined_simulator_issue, SimulatorIssue},
    observation::{coverage_reached, model_observations, ModelEvidence},
    resources::{resource_metrics, ResourceMetrics},
};

pub(super) struct EvaluatedEvidence {
    pub(super) completion: CheckCompletion,
    pub(super) status: EvidenceStatus,
    pub(super) classification: Option<FailureClassification>,
    pub(super) message: Option<String>,
    pub(super) observations: BTreeMap<String, u64>,
    pub(super) simulator_liveness: Option<SimulatorLivenessBinding>,
    pub(super) artifacts: Vec<ArtifactRef>,
    pub(super) duration_ms: u64,
    pub(super) peak_rss_kib: u64,
}

pub(super) fn evaluate_with_inventory_issue(
    descriptor: &EvidenceDescriptor,
    profile: &str,
    check_contracts: &BTreeMap<String, SimulatorCheckContract>,
    liveness_contracts: &[SimulatorLivenessContract],
    model: &SimulatorExecution,
    detectors: &DetectorRun,
    inventory_issue: Option<&SimulatorIssue>,
) -> Result<EvaluatedEvidence, Box<dyn Error>> {
    let identity = descriptor
        .simulator
        .as_ref()
        .ok_or("simulator descriptor omitted execution identity")?;
    let ModelEvidence {
        mut observations,
        per_check_required_observations,
        simulator_liveness,
        issue,
    } = model_observations(
        profile,
        &descriptor.invariant_id,
        identity,
        check_contracts,
        liveness_contracts,
        &model.events,
    );
    let mut artifacts = model.artifacts.clone();
    let detector_outcome = identity
        .negative_test
        .as_ref()
        .and_then(|test| detectors.outcomes.get(&test.check_id()));
    let detector_passed =
        detector_outcome.is_none_or(|outcome| outcome.status == EvidenceStatus::Pass);
    observations.insert("detector_qualified".to_owned(), u64::from(detector_passed));
    if let Some(outcome) = detector_outcome {
        artifacts.extend(outcome.artifacts.clone());
    }
    let issue = combined_simulator_issue(
        issue,
        inventory_issue,
        identity,
        model,
        detectors,
        detector_outcome,
    );
    if let Some(issue) = issue {
        if identity.liveness_report.is_some() {
            observations.insert(identity.required_observation.clone(), 0);
        }
        return Ok(evaluate_issue(
            issue,
            observations,
            artifacts,
            resource_metrics(model, detector_outcome),
        ));
    }
    let resources = resource_metrics(model, detector_outcome);
    if !detector_passed {
        return Ok(EvaluatedEvidence {
            completion: CheckCompletion::HarnessError,
            status: EvidenceStatus::Error,
            classification: Some(FailureClassification::HarnessError),
            message: Some("detector qualification fixture did not pass".to_owned()),
            observations,
            simulator_liveness: None,
            artifacts,
            duration_ms: resources.duration_ms,
            peak_rss_kib: resources.peak_rss_kib,
        });
    }
    let coverage = coverage_reached(identity, &observations, &per_check_required_observations);
    if coverage {
        return Ok(EvaluatedEvidence {
            completion: if identity.liveness_report.is_some() {
                CheckCompletion::Completed
            } else {
                CheckCompletion::FrontierExhausted
            },
            status: EvidenceStatus::Pass,
            classification: None,
            message: None,
            observations,
            simulator_liveness,
            artifacts,
            duration_ms: resources.duration_ms,
            peak_rss_kib: resources.peak_rss_kib,
        });
    }
    Ok(EvaluatedEvidence {
        completion: CheckCompletion::CoverageNotReached,
        status: EvidenceStatus::Incomplete,
        classification: Some(FailureClassification::CoverageNotReached),
        message: Some("required semantic simulator coverage was not reached".to_owned()),
        observations,
        simulator_liveness: None,
        artifacts,
        duration_ms: resources.duration_ms,
        peak_rss_kib: resources.peak_rss_kib,
    })
}

fn evaluate_issue(
    issue: SimulatorIssue,
    observations: BTreeMap<String, u64>,
    artifacts: Vec<ArtifactRef>,
    resources: ResourceMetrics,
) -> EvaluatedEvidence {
    let (completion, status, classification, message) = match issue {
        SimulatorIssue::InvariantViolation(message) => (
            CheckCompletion::Counterexample,
            EvidenceStatus::Fail,
            FailureClassification::InvariantViolation,
            message,
        ),
        SimulatorIssue::HarnessError(message) => (
            CheckCompletion::HarnessError,
            EvidenceStatus::Error,
            FailureClassification::HarnessError,
            message,
        ),
        SimulatorIssue::CoverageNotReached(message) => (
            CheckCompletion::CoverageNotReached,
            EvidenceStatus::Incomplete,
            FailureClassification::CoverageNotReached,
            message,
        ),
    };
    EvaluatedEvidence {
        completion,
        status,
        classification: Some(classification),
        message: Some(message),
        observations,
        simulator_liveness: None,
        artifacts,
        duration_ms: resources.duration_ms,
        peak_rss_kib: resources.peak_rss_kib,
    }
}
