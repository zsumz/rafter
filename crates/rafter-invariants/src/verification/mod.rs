//! Independent acceptance of untrusted invariant evidence.

mod detector;
mod error;
mod process_receipt;
pub(crate) mod simulator;

pub use detector::{validate_detector_fixture_sources, DetectorFixtureSourceBinding};
pub(crate) use error::AggregateError;
#[cfg(test)]
pub(crate) use process_receipt::process_launchers_match_runtime;
pub(crate) use process_receipt::{
    process_invocation_is_complete, process_invocation_matches_source,
    script_invocation_matches_source,
};
