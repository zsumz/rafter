//! The vocabulary a replay is requested and reported in.
//!
//! A replay names which invariant suite to run and what it expects — a final
//! state or one named invariant failure — and reports the state reached along
//! with any failure observed. Suites and expectations are deliberately closed
//! sets, so extending either is a deliberate change.

use super::super::{Failure, StateSummary};

/// Invariant suite to run while replaying a model-check trace.
///
/// This enum is exhaustive because replay currently supports this closed set
/// of invariant suites.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayCheck {
    ElectionSafety,
    CommitSafety,
}

/// Expected replay result.
///
/// This enum is exhaustive because replay expectations are limited to
/// successful final-state matching or one named invariant failure.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplayExpectation<'a> {
    FinalState(&'a StateSummary),
    FailureInvariant(&'static str),
}

/// Result of replaying a model-check trace.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayReport {
    pub(in crate::model_check::replay) state: StateSummary,
    pub(in crate::model_check::replay) failure: Option<Failure>,
}

impl ReplayReport {
    /// Returns the final or failed state summary produced by replay.
    #[must_use]
    pub const fn state(&self) -> &StateSummary {
        &self.state
    }

    /// Returns the invariant failure observed during replay, if any.
    #[must_use]
    pub const fn failure(&self) -> Option<&Failure> {
        self.failure.as_ref()
    }
}
