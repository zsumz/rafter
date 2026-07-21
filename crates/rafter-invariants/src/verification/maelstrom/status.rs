//! Independent completion and invariant-status rederivation.

use std::collections::BTreeSet;

use crate::{
    evidence::{
        format::maelstrom::{MaelstromSummary, Validity},
        CheckCompletion, CheckReceipt, EvidenceStatus, ResultBundle,
    },
    verification::AggregateError,
};

use super::lease::LeaseArtifactStatus;

pub(super) struct TrialStatuses<'a> {
    pub(super) summaries: &'a [Option<MaelstromSummary>],
    pub(super) result_parse_successes: &'a [bool],
    pub(super) process_successes: &'a [bool],
    pub(super) coverage: &'a [bool],
    pub(super) lease_statuses: &'a [LeaseArtifactStatus],
}

pub(super) fn verify(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    trials: &TrialStatuses<'_>,
) -> Result<bool, AggregateError> {
    let statuses = bundle
        .results
        .iter()
        .filter(|result| result.execution_id == check.execution_id)
        .map(|result| (result.invariant_id.as_str(), result.status))
        .collect::<Vec<_>>();
    let non_linearizable = trials
        .summaries
        .iter()
        .flatten()
        .any(|summary| summary.linearizability == Validity::Invalid);
    let all_valid = trials.summaries.iter().all(|summary| {
        summary
            .as_ref()
            .is_some_and(|summary| summary.validity == Validity::Valid)
    });
    let owns_rd06 = statuses.iter().any(|(id, _)| *id == "RD-06");
    let lease_violation = trials.lease_statuses.iter().any(|status| {
        matches!(
            status,
            LeaseArtifactStatus::Violation | LeaseArtifactStatus::ViolationWithHarnessError
        )
    });
    let harness_error = has_harness_error(
        trials.result_parse_successes,
        trials.process_successes,
        trials.lease_statuses,
    );
    let expected_failures =
        expected_counterexample_invariants(lease_violation, non_linearizable, owns_rd06);
    let globally_bound_rd06 = bundle
        .results
        .iter()
        .any(|result| result.invariant_id == "RD-06" && result.status == EvidenceStatus::Fail);
    let agrees = if !expected_failures.is_empty() {
        local_counterexample_agrees(
            check,
            &statuses,
            &expected_failures,
            non_linearizable,
            owns_rd06,
            globally_bound_rd06,
        )
    } else if non_linearizable {
        supporting_counterexample_statuses(bundle, check, &statuses)
    } else if harness_error {
        uniform_statuses(
            check,
            &statuses,
            CheckCompletion::HarnessError,
            EvidenceStatus::Error,
        )
    } else if !all_valid || trials.coverage.contains(&false) {
        uniform_statuses(
            check,
            &statuses,
            CheckCompletion::CoverageNotReached,
            EvidenceStatus::Incomplete,
        )
    } else {
        uniform_statuses(
            check,
            &statuses,
            CheckCompletion::Completed,
            EvidenceStatus::Pass,
        )
    };
    if agrees {
        Ok((lease_violation || non_linearizable) && harness_error)
    } else {
        Err(error(format!(
            "{} evidence statuses disagree with Maelstrom artifacts",
            check.check_id
        )))
    }
}

pub(crate) fn has_harness_error(
    result_parse_successes: &[bool],
    process_successes: &[bool],
    lease_statuses: &[LeaseArtifactStatus],
) -> bool {
    result_parse_successes.contains(&false)
        || process_successes.contains(&false)
        || lease_statuses.iter().any(|status| {
            matches!(
                status,
                LeaseArtifactStatus::HarnessError | LeaseArtifactStatus::ViolationWithHarnessError
            )
        })
}

pub(crate) fn local_counterexample_agrees(
    check: &CheckReceipt,
    statuses: &[(&str, EvidenceStatus)],
    expected_invariants: &BTreeSet<&str>,
    non_linearizable: bool,
    owns_rd06: bool,
    globally_bound_rd06: bool,
) -> bool {
    counterexample_statuses(check, statuses, expected_invariants)
        && (!non_linearizable || owns_rd06 || globally_bound_rd06)
}

pub(crate) fn expected_counterexample_invariants(
    lease_violation: bool,
    non_linearizable: bool,
    owns_rd06: bool,
) -> BTreeSet<&'static str> {
    [
        (lease_violation, "RD-05"),
        (non_linearizable && owns_rd06, "RD-06"),
    ]
    .into_iter()
    .filter_map(|(required, invariant)| required.then_some(invariant))
    .collect()
}

pub(crate) fn counterexample_statuses(
    check: &CheckReceipt,
    statuses: &[(&str, EvidenceStatus)],
    expected_invariants: &BTreeSet<&str>,
) -> bool {
    let expected_failure = |invariant: &str, status| {
        expected_invariants.contains(invariant) && status == EvidenceStatus::Fail
    };
    check.completion == CheckCompletion::Counterexample
        && statuses
            .iter()
            .filter(|(id, status)| expected_failure(id, *status))
            .count()
            == expected_invariants.len()
        && statuses.iter().all(|(id, status)| {
            expected_failure(id, *status)
                || (!expected_invariants.contains(id) && *status == EvidenceStatus::Incomplete)
        })
}

fn supporting_counterexample_statuses(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    statuses: &[(&str, EvidenceStatus)],
) -> bool {
    bundle
        .results
        .iter()
        .any(|result| result.invariant_id == "RD-06" && result.status == EvidenceStatus::Fail)
        && uniform_statuses(
            check,
            statuses,
            CheckCompletion::CoverageNotReached,
            EvidenceStatus::Incomplete,
        )
}

fn uniform_statuses(
    check: &CheckReceipt,
    statuses: &[(&str, EvidenceStatus)],
    completion: CheckCompletion,
    status: EvidenceStatus,
) -> bool {
    check.completion == completion
        && !statuses.is_empty()
        && statuses.iter().all(|(_, observed)| *observed == status)
}

fn error(message: impl Into<String>) -> AggregateError {
    AggregateError::new(message.into())
}
