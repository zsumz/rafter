use std::{error::Error, fmt};

use super::super::{Action, Failure, StateSummary};

/// Error returned when a model-check trace cannot be replayed as expected.
///
/// This enum is exhaustive because replay failures are closed over these trace
/// and expectation mismatch cases.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReplayError {
    MissingReadyMessage {
        action_index: usize,
        action: Action,
    },
    MissingPromotionBarrier {
        action_index: usize,
        action: Action,
    },
    SchedulingFailure {
        action_index: usize,
        action: Action,
        message: String,
    },
    UnexpectedFailure {
        expected: &'static str,
        actual: Failure,
    },
    ExpectedFailureNotReached {
        expected: &'static str,
        final_state: StateSummary,
    },
    FinalStateMismatch {
        expected: StateSummary,
        actual: StateSummary,
    },
}

impl fmt::Display for ReplayError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingReadyMessage {
                action_index,
                action,
            } => write!(
                formatter,
                "trace action {action_index} could not find a ready message for `{action}`"
            ),
            Self::MissingPromotionBarrier {
                action_index,
                action,
            } => write!(
                formatter,
                "trace action {action_index} could not find a promotion barrier for `{action}`"
            ),
            Self::SchedulingFailure {
                action_index,
                action,
                message,
            } => write!(
                formatter,
                "trace action {action_index} could not resolve scheduler identity for `{action}`: {message}"
            ),
            Self::UnexpectedFailure { expected, actual } => write!(
                formatter,
                "expected replay failure `{expected}`, found `{}`",
                actual.invariant()
            ),
            Self::ExpectedFailureNotReached {
                expected,
                final_state: _,
            } => write!(
                formatter,
                "expected replay failure `{expected}`, but trace completed"
            ),
            Self::FinalStateMismatch {
                expected: _,
                actual: _,
            } => formatter.write_str("replayed trace ended in a different final state"),
        }
    }
}

impl Error for ReplayError {}
