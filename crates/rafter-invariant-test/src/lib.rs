//! Explicit assertion macros for tests consumed by the invariant gate.
//!
//! The gate supplies a source-bound token. A typed assertion emits one
//! observation marker on success or one violation marker on failure. Ordinary
//! panics and assertions remain harness errors instead of being mistaken for
//! protocol counterexamples.

extern crate self as rafter_invariant_test;

mod detector;
mod oracle;

pub use detector::DetectorTestOutcome;
pub use rafter_invariant_test_macros::detector_test;

#[doc(hidden)]
pub use detector::{
    begin_detector_test as __begin_detector_test, detector_test_outcome as __detector_test_outcome,
};
#[doc(hidden)]
pub use oracle::{
    expect_error as __oracle_expect_err, invoke_recorder as __oracle_invoke_recorder,
    observed as __oracle_observed, violation as __oracle_violation,
    violation_message as __oracle_violation_message, OracleCall as __OracleCall,
};

#[cfg(test)]
pub(crate) use detector::fabricate_witness as __oracle_fabricated_detector_witness;

#[cfg(test)]
mod tests;
