//! Deterministic aggregation for Rafter's invariant evidence contract.

#[cfg(test)]
mod artifact_verify;
#[cfg(test)]
mod artifact_verify_maelstrom;
#[cfg(test)]
mod artifact_verify_maelstrom_support;
#[cfg(test)]
#[path = "verification/maelstrom/tests/full_bundle.rs"]
mod artifact_verify_maelstrom_tests;
#[cfg(test)]
mod artifact_verify_tla;
mod contract;
mod evidence;
mod execution;
/// Source-bound producer orchestration, verification, and command adaptation.
pub mod gate;
mod plan;
mod producer;
mod provenance;
#[cfg(test)]
mod receipt;
#[cfg(test)]
mod receipt_maelstrom;
#[cfg(test)]
mod receipt_tests;
mod verdict;
mod verification;

pub use contract::catalog::{
    Catalog, CatalogError, ClauseDescriptor, ClausePolicy, DetectorReplayArtifactPolicy,
    DetectorReplayBuild, DetectorReplayChallenge, DetectorReplayContract,
    DetectorReplayFixtureInventory, DetectorReplayPolicy, DetectorReplaySource,
    DetectorReplayTargetDirectory, EvidenceDescriptor, EvidenceLayer, EvidencePolicy,
    EvidenceStrength, InvariantDescriptor, ProfileContract, ProfileManifest,
    RequiredClauseStrength, RunnerContract, SimulatorCheckContract, SimulatorIdentity,
    TestIdentity, VerifierContract,
};
pub use contract::registry::render_registry_markdown;
pub use contract::registry::{
    PersistenceEvidenceKind, RegistryClause, RegistryCounts, RegistryDocument, RegistryEvidence,
    RegistryInvariant, RegistryParseError, REGISTRY_SCHEMA_VERSION,
};
#[cfg(test)]
pub(crate) use evidence::ResultBundle;
pub use evidence::{
    ArtifactRef, CheckCompletion, CheckReceipt, EvidenceResult, EvidenceStatus, ExecutableReceipt,
    ExecutionPlanReceipt, ExecutionReceipt, FailureClassification, InvocationReceipt,
    LauncherReceipt, PlanInput, ProducerBindingReceipt, SourceMaterializationReceipt,
    SourceReceipt, ToolReceipt, PLAN_SCHEMA_VERSION,
};
pub use gate::{
    current_source_ref, run_all, verify_and_write_report, verify_layer_evidence, verify_report_set,
    ReportWriteOutcome, RunAllOptions, RunAllOutcome,
};
pub use plan::{ExecutionPlan, PlanOptions};
pub use producer::{produce, ProducerOptions, ProducerOutcome};
pub use provenance::image::ensure_immutable_producer;
pub use verdict::{ClauseVerdict, InvariantVerdict, VerdictReport, VerdictStatus};
pub use verification::{
    publish_verifier_archive, validate_detector_fixture_sources, verify_verifier_archive,
    DetectorFixtureAnalysis, DetectorFixtureSourceBatch, DetectorFixtureSourceBinding,
    VerifierArchiveExpectation,
};

#[cfg(test)]
mod tests;
