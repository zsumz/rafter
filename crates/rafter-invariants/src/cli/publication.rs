//! CLI adaptation for verifier-evidence archive publication and readback.

use std::path::Path;

pub(super) fn seal(
    profile: &str,
    profile_manifest: &Path,
    root: &Path,
    manifest: &Path,
    manifest_sha256: &str,
    archive: &Path,
) -> Result<bool, Box<dyn std::error::Error>> {
    let expectation = expectation(profile, profile_manifest)?;
    let digest = rafter_invariants::publish_verifier_archive(
        root,
        manifest,
        manifest_sha256,
        archive,
        &expectation,
    )?;
    println!("archive_sha256={digest}");
    Ok(true)
}

pub(super) fn verify(
    profile: &str,
    profile_manifest: &Path,
    archive: &Path,
    archive_sha256: &str,
    manifest_sha256: &str,
) -> Result<bool, Box<dyn std::error::Error>> {
    let expectation = expectation(profile, profile_manifest)?;
    rafter_invariants::verify_verifier_archive(
        archive,
        archive_sha256,
        manifest_sha256,
        &expectation,
    )?;
    println!("verified verifier archive {}", archive.display());
    Ok(true)
}

fn expectation(
    profile: &str,
    profile_manifest: &Path,
) -> Result<rafter_invariants::VerifierArchiveExpectation, Box<dyn std::error::Error>> {
    let manifest = rafter_invariants::ProfileManifest::load(profile_manifest)?;
    rafter_invariants::VerifierArchiveExpectation::capture(
        &std::env::current_dir()?,
        profile,
        &manifest,
    )
}
