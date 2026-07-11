use std::{error::Error, fmt};

use crate::SimSeed;

use super::SoakAction;
use crate::model_check::Failure;

/// Error returned when a randomized soak finds an invariant violation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SoakFailure {
    pub(in crate::model_check) seed: SimSeed,
    pub(in crate::model_check) step: usize,
    pub(in crate::model_check) trace: Vec<SoakAction>,
    pub(in crate::model_check) failure: Box<Failure>,
}

impl SoakFailure {
    /// Returns the deterministic simulator seed.
    #[must_use]
    pub const fn seed(&self) -> SimSeed {
        self.seed
    }

    /// Returns the step that exposed the invariant failure.
    #[must_use]
    pub const fn step(&self) -> usize {
        self.step
    }

    /// Returns the action trace that led to the failure.
    #[must_use]
    pub fn trace(&self) -> &[SoakAction] {
        &self.trace
    }

    /// Returns the underlying invariant failure.
    #[must_use]
    pub fn failure(&self) -> &Failure {
        &self.failure
    }
}

impl fmt::Display for SoakFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "seed {:?} failed at step {}: {}",
            self.seed, self.step, self.failure
        )
    }
}

impl Error for SoakFailure {}
