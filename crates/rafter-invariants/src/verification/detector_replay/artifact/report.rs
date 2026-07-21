//! Canonical replay report inventory and artifact publication.

use std::error::Error;

use crate::evidence::ArtifactRef;

use super::{
    model::{ReplayInventory, ReplayReport},
    publisher::ReplayArtifactPublisher,
    validation::validate_report_bytes,
};
use crate::verification::detector_replay::DetectorReplayPlan;

pub(super) fn inventory(replay: &DetectorReplayPlan) -> Result<ReplayInventory, String> {
    Ok(ReplayInventory {
        fixtures: replay.fixture_count(),
        targets: replay.target_count(),
        evidence_bindings: replay.evidence_binding_count(),
        sha256: Some(replay.inventory_sha256()?),
    })
}

pub(super) fn publish(
    publisher: &ReplayArtifactPublisher,
    report: &ReplayReport,
) -> Result<ArtifactRef, Box<dyn Error>> {
    let mut bytes = serde_json::to_vec_pretty(report)?;
    bytes.push(b'\n');
    validate_report_bytes(&bytes)?;
    publisher.capture("verifier-replay-report", &bytes)
}
