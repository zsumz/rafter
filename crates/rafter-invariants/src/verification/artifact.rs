//! Verification-owned compatibility mount for legacy artifact reconstruction.

use std::path::Path;

use crate::{contract::catalog::Catalog, evidence::ResultBundle};

use super::AggregateError;

pub(crate) fn verify_bundle(
    bundle: &ResultBundle,
    root: &Path,
    source_root: &Path,
    catalog: &Catalog,
    trusted_profile: &str,
    trusted_runner: &str,
) -> Result<(Vec<String>, super::AuthenticatedArtifacts), AggregateError> {
    if bundle.profile != trusted_profile || bundle.runner != trusted_runner {
        return Err(AggregateError::new(format!(
            "artifact verification identity mismatch: receipt profile/runner {}/{} != trusted {trusted_profile}/{trusted_runner}",
            bundle.profile, bundle.runner
        )));
    }
    let budget = super::bundle::BundleBudget::for_trusted(trusted_profile, trusted_runner)?;
    crate::artifact_verify::verify(bundle, root, source_root, catalog, budget, trusted_runner)
}

#[cfg(test)]
#[path = "artifact/tests.rs"]
mod tests;
