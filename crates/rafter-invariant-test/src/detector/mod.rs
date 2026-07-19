//! Detector-test session and invocation-proof facade.

mod outcome;
mod proof;
mod session;
mod wire;
mod witness;

pub use outcome::DetectorTestOutcome;
pub use session::{begin_detector_test, detector_test_outcome};

pub(crate) use session::{
    mark_first_observation, record_expected_rejection, record_recorder_invocation,
};
pub(crate) use wire::{emit_observed, violation_message};

#[cfg(test)]
pub(crate) use wire::fabricate_witness;
