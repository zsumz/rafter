//! Simulator issue precedence and cross-run failure classification contract.

use crate::{contract::SimulatorIdentity, evidence::EvidenceStatus};

use crate::producer::test_exec::TestOutcome;

use super::model::SimulatorExecution;

use super::detector::DetectorRun;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum SimulatorIssue {
    InvariantViolation(String),
    HarnessError(String),
    CoverageNotReached(String),
}

pub(super) fn combined_simulator_issue(
    mut issue: Option<SimulatorIssue>,
    inventory_issue: Option<&SimulatorIssue>,
    identity: &SimulatorIdentity,
    model: &SimulatorExecution,
    detectors: &DetectorRun,
    detector_outcome: Option<&TestOutcome>,
) -> Option<SimulatorIssue> {
    merge_issue(&mut issue, inventory_issue.cloned());
    if !model.processes_succeeded {
        let message = if model.harness_errors.is_empty() {
            "simulator profile process did not complete successfully".to_owned()
        } else {
            model.harness_errors.join("; ")
        };
        merge_issue(&mut issue, Some(SimulatorIssue::HarnessError(message)));
    }
    merge_issue(
        &mut issue,
        detectors
            .harness_error
            .as_ref()
            .map(|error| SimulatorIssue::HarnessError(error.clone())),
    );
    if identity.negative_test.is_some() && detector_outcome.is_none() {
        merge_issue(
            &mut issue,
            Some(SimulatorIssue::HarnessError(
                "detector result is missing".to_owned(),
            )),
        );
    }
    if detector_outcome.is_some_and(|outcome| outcome.status != EvidenceStatus::Pass) {
        merge_issue(
            &mut issue,
            Some(SimulatorIssue::HarnessError(
                "detector qualification fixture did not pass".to_owned(),
            )),
        );
    }
    issue
}

pub(super) fn merge_issue(current: &mut Option<SimulatorIssue>, candidate: Option<SimulatorIssue>) {
    let Some(candidate) = candidate else {
        return;
    };
    let rank = |issue: &SimulatorIssue| match issue {
        SimulatorIssue::InvariantViolation(_) => 3,
        SimulatorIssue::HarnessError(_) => 2,
        SimulatorIssue::CoverageNotReached(_) => 1,
    };
    if current
        .as_ref()
        .is_none_or(|issue| rank(&candidate) > rank(issue))
    {
        *current = Some(candidate);
    }
}
