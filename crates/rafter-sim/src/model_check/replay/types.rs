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
    pub(in crate::model_check::replay) states: Vec<StateSummary>,
}

impl ReplayReport {
    /// Returns the initial summary followed by the summary after each action
    /// actually replayed, including the action exposing an expected failure.
    ///
    /// Adjacent summaries show term, role, commitment, and log-boundary
    /// changes. They are observations, not a full protocol-state capture.
    /// Replay stops at the first failing action; it performs no trace
    /// minimization and makes no claim that the retained prefix is minimal.
    #[must_use]
    pub fn states(&self) -> &[StateSummary] {
        &self.states
    }

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
