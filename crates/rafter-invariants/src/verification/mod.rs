//! Independent acceptance of untrusted invariant evidence.

mod detector;
pub(crate) mod simulator;

pub use detector::{validate_detector_fixture_sources, DetectorFixtureSourceBinding};
