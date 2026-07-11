use std::{error::Error, fmt};

use super::{Action, StateSummary};

/// Top-level class for a model-check failure.
///
/// This enum is exhaustive because model-check triage currently uses this
/// closed set of failure classes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureKind {
    /// An explored state or transition contradicted the named invariant.
    InvariantViolation,
    /// The configured exploration did not reach a required witness scenario.
    CoverageNotReached,
    /// The simulator harness could not apply or resume its own modeled action.
    HarnessError,
}

impl FailureKind {
    /// Returns the stable machine-readable label for this failure kind.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvariantViolation => "invariant-violation",
            Self::CoverageNotReached => "coverage-not-reached",
            Self::HarnessError => "harness-error",
        }
    }
}

impl fmt::Display for FailureKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Failure returned when a bounded exploration finds an invariant violation,
/// coverage miss, or harness error.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Failure {
    pub(in crate::model_check) kind: FailureKind,
    pub(in crate::model_check) invariant: &'static str,
    pub(in crate::model_check) message: String,
    pub(in crate::model_check) trace: Vec<Action>,
    pub(in crate::model_check) state: StateSummary,
}

impl Failure {
    /// Returns the class of failure.
    #[must_use]
    pub const fn kind(&self) -> FailureKind {
        self.kind
    }

    /// Returns the invariant that failed.
    #[must_use]
    pub const fn invariant(&self) -> &'static str {
        self.invariant
    }

    /// Returns a human-readable failure message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns the bounded action trace that led to the failure.
    #[must_use]
    pub fn trace(&self) -> &[Action] {
        &self.trace
    }

    /// Returns the final cluster summary at the failed state.
    #[must_use]
    pub const fn state(&self) -> &StateSummary {
        &self.state
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.invariant, self.message)
    }
}

impl Error for Failure {}
