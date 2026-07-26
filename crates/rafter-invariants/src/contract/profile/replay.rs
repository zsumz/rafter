//! Aggregate-owned detector replay policy and resource bounds.

use serde::{Deserialize, Serialize};

pub(crate) const REVIEWED_DETECTOR_REPLAY_INVENTORY_SHA256: &str =
    "d69894e5e0cc43412655ecc6cf1ec2cad2fc42ae82c55d0db77a1dcb8d9b046e";
pub(crate) const REVIEWED_DETECTOR_REPLAY_TOTAL_TIMEOUT_SECONDS: u64 = 30 * 60;

/// Independent source and execution policy for detector replay.
///
/// This enum is deliberately exhaustive so unknown policies fail during decoding.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum DetectorReplayPolicy {
    /// Authenticate source, then perform a fresh verifier-owned execution.
    #[serde(rename = "authenticated-source-fresh-execution-v1")]
    AuthenticatedSourceFreshExecutionV1,
}

/// Source capability accepted by detector replay.
///
/// This enum is deliberately exhaustive so unknown source capabilities fail closed.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum DetectorReplaySource {
    /// A private source snapshot authenticated by the aggregate verifier.
    #[serde(rename = "private-authenticated-snapshot")]
    PrivateAuthenticatedSnapshot,
}

/// Build protocol used for detector replay.
///
/// This enum is deliberately exhaustive so unknown build protocols fail closed.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum DetectorReplayBuild {
    /// Locked offline compilation from authenticated directory sources.
    #[serde(rename = "locked-offline-authenticated-directory-source-no-default-features-v1")]
    LockedOfflineAuthenticatedDirectorySourceNoDefaultFeaturesV1,
}

/// Target-directory isolation policy for detector replay.
///
/// This enum is deliberately exhaustive so unknown isolation policies fail closed.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum DetectorReplayTargetDirectory {
    /// Compile into a fresh private directory.
    #[serde(rename = "fresh-private-directory")]
    FreshPrivateDirectory,
}

/// Registry selection policy for replayed detector fixtures.
///
/// This enum is deliberately exhaustive so unknown fixture inventories fail closed.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum DetectorReplayFixtureInventory {
    /// Replay every direct simulator fixture selected by the active profile.
    #[serde(rename = "all-profile-selected-direct-simulator-fixtures")]
    AllProfileSelectedDirectSimulatorFixtures,
}

/// Runtime challenge protocol required from each replayed fixture.
///
/// This enum is deliberately exhaustive so unknown challenge protocols fail closed.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum DetectorReplayChallenge {
    /// Challenge over an inherited descriptor before the fixture body runs.
    #[serde(rename = "inherited-descriptor-pre-body-secret-v3")]
    InheritedDescriptorPreBodySecretV3,
}

/// Artifact publication policy for detector replay.
///
/// This enum is deliberately exhaustive so unknown artifact policies fail closed.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum DetectorReplayArtifactPolicy {
    /// Publish the typed replay report and bounded process logs.
    #[serde(rename = "json-and-process-logs")]
    JsonAndProcessLogs,
}

/// Source, execution, inventory, artifact, and time bounds for detector replay.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DetectorReplayContract {
    pub policy: DetectorReplayPolicy,
    pub source: DetectorReplaySource,
    pub build: DetectorReplayBuild,
    pub target_directory: DetectorReplayTargetDirectory,
    pub fixture_inventory: DetectorReplayFixtureInventory,
    pub challenge: DetectorReplayChallenge,
    pub artifact_policy: DetectorReplayArtifactPolicy,
    pub required_inventory_sha256: String,
    pub required_registry_packages: usize,
    pub maximum_registry_archive_bytes: u64,
    pub maximum_registry_expanded_bytes: u64,
    pub maximum_registry_entries: u64,
    pub required_unique_fixtures: usize,
    pub required_evidence_bindings: usize,
    pub required_targets: usize,
    pub compile_timeout_seconds: u64,
    pub fixture_timeout_seconds: u64,
    pub total_timeout_seconds: u64,
}

#[cfg(test)]
#[path = "replay_tests.rs"]
mod tests;
