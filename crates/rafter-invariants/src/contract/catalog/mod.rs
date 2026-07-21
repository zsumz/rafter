//! Normalized executable view of the reviewed registry.

mod model;
mod policy;
mod resolve;

pub use crate::contract::error::CatalogError;
pub use crate::contract::profile::{
    ClausePolicy, DetectorReplayArtifactPolicy, DetectorReplayBuild, DetectorReplayChallenge,
    DetectorReplayContract, DetectorReplayFixtureInventory, DetectorReplayPolicy,
    DetectorReplaySource, DetectorReplayTargetDirectory, EvidenceLayer, EvidencePolicy,
    EvidenceStrength, ProfileContract, ProfileManifest, RequiredClauseStrength, RunnerContract,
    SimulatorCheckContract, VerifierContract,
};
pub use crate::contract::{SimulatorIdentity, TestIdentity};
pub use model::{Catalog, ClauseDescriptor, EvidenceDescriptor, InvariantDescriptor};

#[cfg(test)]
mod tests;
