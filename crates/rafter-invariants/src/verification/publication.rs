//! Exact archive publication and readback for verifier-owned evidence.

mod archive;
mod directory;
mod manifest;
mod model;

use std::{error::Error, path::Path, time::Duration};

use crate::{
    contract::{catalog::Catalog, profile::ProfileManifest},
    verification::source::{RegistryMaterializationPolicy, SourceVerifier},
};

const EXPECTATION_TIMEOUT: Duration = Duration::from_secs(120);

/// Trusted checkout, toolchain, profile, and replay policy expected in an archive.
///
/// The expectation is captured independently from report bytes and remains opaque so callers
/// cannot accidentally substitute report-controlled provenance during publication or readback.
pub struct VerifierArchiveExpectation {
    replay: crate::verification::detector_replay::ReplayReportExpectation,
    source: Option<(std::path::PathBuf, SourceVerifier)>,
}

impl std::fmt::Debug for VerifierArchiveExpectation {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VerifierArchiveExpectation")
            .field("replay", &self.replay)
            .finish_non_exhaustive()
    }
}

impl VerifierArchiveExpectation {
    /// Capture the active checkout and toolchain for one reviewed verification profile.
    ///
    /// # Errors
    ///
    /// Returns an error when the profile is absent or the checkout and active Rust toolchain
    /// cannot be authenticated and snapshotted.
    pub fn capture(
        root: &Path,
        profile: &str,
        manifest: &ProfileManifest,
    ) -> Result<Self, Box<dyn Error>> {
        let root = std::fs::canonicalize(root)?;
        let reviewed_manifest = reviewed_profile_manifest(&root, manifest)?;
        let contract = reviewed_manifest
            .verifiers
            .get(profile)
            .ok_or_else(|| format!("profile {profile} has no verifier contract"))?
            .detector_replay
            .clone();
        let mut source = SourceVerifier::capture(&root)?;
        let deadline = std::time::Instant::now()
            .checked_add(EXPECTATION_TIMEOUT)
            .ok_or("verifier archive expectation deadline overflow")?;
        let registry = source
            .prepare_compilation_source(RegistryMaterializationPolicy {
                required_packages: contract.required_registry_packages,
                maximum_archive_bytes: contract.maximum_registry_archive_bytes,
                maximum_expanded_bytes: contract.maximum_registry_expanded_bytes,
                maximum_entries: contract.maximum_registry_entries,
                deadline,
            })?
            .registry_receipt();
        let receipts = source.replay_receipts()?;
        source
            .revalidate(&root)
            .map_err(|error| error.message().to_owned())?;
        Ok(Self {
            replay: crate::verification::detector_replay::ReplayReportExpectation::new(
                profile.to_owned(),
                receipts,
                contract,
                Some(registry),
            ),
            source: Some((root, source)),
        })
    }

    fn replay(&self) -> &crate::verification::detector_replay::ReplayReportExpectation {
        &self.replay
    }

    fn revalidate(&self) -> Result<(), String> {
        if let Some((root, source)) = &self.source {
            source
                .revalidate(root)
                .map_err(|error| error.message().to_owned())?;
        }
        Ok(())
    }

    #[cfg(test)]
    fn from_replay(replay: crate::verification::detector_replay::ReplayReportExpectation) -> Self {
        Self {
            replay,
            source: None,
        }
    }
}

fn reviewed_profile_manifest(
    root: &Path,
    supplied: &ProfileManifest,
) -> Result<ProfileManifest, Box<dyn Error>> {
    let catalog = Catalog::load(&root.join("verification/raft-invariants.yaml"))?;
    let reviewed = ProfileManifest::load(&root.join("verification/raft-invariant-profiles.json"))?;
    reviewed.validate(&catalog)?;
    if supplied != &reviewed {
        return Err("supplied profile manifest differs from the reviewed checkout manifest".into());
    }
    Ok(reviewed)
}

/// Verify one sealed verifier-evidence directory and publish a deterministic archive.
///
/// The directory must contain exactly one supplied manifest and the immutable regular
/// files named by that manifest. The returned value is the archive SHA-256.
///
/// # Errors
///
/// Returns an error when the source set is not exact, immutable, bounded, or digest-valid,
/// or when the deterministic archive cannot be published atomically.
pub fn publish_verifier_archive(
    root: &Path,
    manifest: &Path,
    expected_manifest_sha256: &str,
    archive: &Path,
    expectation: &VerifierArchiveExpectation,
) -> Result<String, Box<dyn Error>> {
    let set = directory::read(root, manifest, expected_manifest_sha256, expectation)?;
    expectation.revalidate()?;
    let digest = archive::publish(&set, archive)?;
    expectation.revalidate()?;
    Ok(digest)
}

/// Verify a downloaded verifier-evidence archive without extracting it.
///
/// Both the archive digest and the embedded manifest digest must match values captured
/// before upload. Every archive entry is checked against the embedded exact inventory.
///
/// # Errors
///
/// Returns an error when either captured digest differs, the archive is malformed or
/// noncanonical, or its embedded inventory is incomplete, excessive, or inconsistent.
pub fn verify_verifier_archive(
    archive: &Path,
    expected_archive_sha256: &str,
    expected_manifest_sha256: &str,
    expectation: &VerifierArchiveExpectation,
) -> Result<(), Box<dyn Error>> {
    archive::verify(
        archive,
        expected_archive_sha256,
        expected_manifest_sha256,
        expectation,
    )?;
    expectation.revalidate()?;
    Ok(())
}

#[cfg(test)]
#[path = "publication/tests/mod.rs"]
mod tests;
