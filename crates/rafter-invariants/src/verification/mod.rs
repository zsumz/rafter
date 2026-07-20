//! Independent acceptance of untrusted invariant evidence.

mod artifact;
mod bundle;
mod detector;
mod error;
mod filesystem;
mod intake;
mod process_receipt;
pub(crate) mod simulator;
pub(crate) mod source;
pub(crate) mod target;

pub(crate) use artifact::verify_bundle as verify_bundle_artifacts;
pub(crate) use bundle::{
    authenticate as authenticate_bundle, AuthenticatedArtifacts, BundleBudget,
};
#[cfg(test)]
pub(crate) use bundle::{
    snapshot_available_artifacts, verify_integrity as verify_bundle_integrity,
    verify_producer_invocation_paths,
};
pub use detector::{validate_detector_fixture_sources, DetectorFixtureSourceBinding};
pub(crate) use error::AggregateError;
pub(crate) use intake::{
    require_passing_layer, verify_layer_paths, verify_paths, EvidenceIntake, IntakeDefect,
    VerificationRequest,
};
#[cfg(test)]
pub(crate) use intake::{verify_receipts_for_test, IntakeDefectKind};
#[cfg(test)]
pub(crate) use process_receipt::process_launchers_match_runtime;
pub(crate) use process_receipt::{
    process_invocation_is_complete, process_invocation_matches_source,
    script_invocation_matches_source,
};
