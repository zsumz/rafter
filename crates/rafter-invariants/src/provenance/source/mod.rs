//! Repository source identities used by producers and independent verifiers.

mod checkout;
mod tracked;

#[cfg(test)]
pub(crate) use checkout::source_environment_matches_digest;
#[cfg(test)]
pub(crate) use checkout::MaterializationObservation;
pub(crate) use checkout::{
    capture_checkout_at, file_sha256, find_executable, head_commit_at, identity_probe_at,
    observe_checkout_at, observe_checkout_with, source_environment_sha256, CapturedSourceFile,
    CheckoutCommandRunner, CheckoutObservation, CommandOutput, GeneratedOutputPolicy,
};
pub(crate) use tracked::{
    parse_tracked_source_paths, require_tracked_source_path_at, tracked_source_paths_at,
};
