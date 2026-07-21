//! Failure precedence shared by event and observation verification.

use crate::evidence::{EvidenceStatus, FailureClassification, ResultBundle};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RawEventIssue {
    InvariantViolation,
    HarnessError,
    CoverageNotReached,
}

impl RawEventIssue {
    pub(crate) const fn rank(self) -> u8 {
        match self {
            Self::InvariantViolation => 3,
            Self::HarnessError => 2,
            Self::CoverageNotReached => 1,
        }
    }
}

pub(crate) fn merge_raw_issue(
    current: &mut Option<RawEventIssue>,
    candidate: Option<RawEventIssue>,
) {
    if candidate
        .is_some_and(|candidate| current.is_none_or(|current| candidate.rank() > current.rank()))
    {
        *current = candidate;
    }
}

pub(crate) fn receipt_issue(
    outcome: (EvidenceStatus, Option<FailureClassification>),
) -> Option<RawEventIssue> {
    match outcome {
        (EvidenceStatus::Fail, Some(FailureClassification::InvariantViolation)) => {
            Some(RawEventIssue::InvariantViolation)
        }
        (EvidenceStatus::Error, Some(FailureClassification::HarnessError)) => {
            Some(RawEventIssue::HarnessError)
        }
        (EvidenceStatus::Incomplete, Some(FailureClassification::CoverageNotReached)) => {
            Some(RawEventIssue::CoverageNotReached)
        }
        _ => None,
    }
}

pub(crate) fn execution_is_passing(bundle: &ResultBundle, execution_id: &str) -> bool {
    bundle
        .results
        .iter()
        .any(|result| result.execution_id == execution_id && result.status == EvidenceStatus::Pass)
}
