//! Compatibility facade for verification-owned Maelstrom artifact acceptance.

pub(super) use crate::verification::maelstrom::verify_authenticated;

#[cfg(test)]
pub(super) use crate::verification::maelstrom::test_support::{
    counterexample_statuses, expected_counterexample_invariants, has_harness_error,
    local_counterexample_agrees, LeaseArtifactStatus,
};
#[cfg(test)]
pub(super) use crate::verification::maelstrom::verify;

#[cfg(test)]
#[path = "verification/maelstrom/tests/status.rs"]
mod tests;
