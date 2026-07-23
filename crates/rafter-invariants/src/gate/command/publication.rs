//! Verifier-evidence archive publication and readback adaptation.

use std::error::Error;

use crate::{contract::profile::ProfileManifest, verification::VerifierArchiveExpectation};

use super::model::{CommandOutput, SealVerifierArtifactsOptions, VerifyVerifierArchiveOptions};

/// Seal content-addressed verifier evidence into its canonical archive.
///
/// # Errors
///
/// Returns an error when profile expectation capture, artifact validation, or
/// deterministic archive publication fails.
pub fn seal(options: &SealVerifierArtifactsOptions) -> Result<CommandOutput, Box<dyn Error>> {
    let expectation = expectation(&options.profile, &options.profile_manifest)?;
    let digest = crate::verification::publish_verifier_archive(
        &options.root,
        &options.manifest,
        &options.manifest_sha256,
        &options.archive,
        &expectation,
    )?;
    Ok(CommandOutput::passed(format!("archive_sha256={digest}")))
}

/// Verify a downloaded verifier archive against the active profile.
///
/// # Errors
///
/// Returns an error when expectation capture, digest validation, metadata
/// validation, or semantic archive readback fails.
pub fn verify(options: &VerifyVerifierArchiveOptions) -> Result<CommandOutput, Box<dyn Error>> {
    let expectation = expectation(&options.profile, &options.profile_manifest)?;
    crate::verification::verify_verifier_archive(
        &options.archive,
        &options.archive_sha256,
        &options.manifest_sha256,
        &expectation,
    )?;
    Ok(CommandOutput::passed(format!(
        "verified verifier archive {}",
        options.archive.display()
    )))
}

fn expectation(
    profile: &str,
    profile_manifest: &std::path::Path,
) -> Result<VerifierArchiveExpectation, Box<dyn Error>> {
    let manifest = ProfileManifest::load(profile_manifest)?;
    VerifierArchiveExpectation::capture(&std::env::current_dir()?, profile, &manifest)
}
