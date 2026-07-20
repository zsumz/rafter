//! Deterministic aggregation for Rafter's invariant evidence contract.

mod artifact_verify;
mod artifact_verify_maelstrom;
mod artifact_verify_maelstrom_support;
#[cfg(test)]
mod artifact_verify_maelstrom_tests;
mod artifact_verify_tla;
mod contract;
mod evidence;
mod execution;
mod gate;
mod plan;
mod producer;
mod provenance;
mod receipt;
mod receipt_maelstrom;
mod receipt_simulator;
mod receipt_tests;
mod receipt_tla;
mod verdict;
mod verification;

#[doc(hidden)]
pub use artifact_verify::DetectorFixtureSourceBatch;
pub use contract::catalog::{
    Catalog, CatalogError, ClauseDescriptor, EvidenceDescriptor, InvariantDescriptor,
    ProfileContract, ProfileManifest, RunnerContract, SimulatorCheckContract, SimulatorIdentity,
    TestIdentity,
};
pub use contract::registry::render_registry_markdown;
pub use contract::registry::{
    PersistenceEvidenceKind, RegistryClause, RegistryCounts, RegistryDocument, RegistryEvidence,
    RegistryInvariant, RegistryParseError, REGISTRY_SCHEMA_VERSION,
};
pub(crate) use evidence::ResultBundle;
pub use evidence::{
    ArtifactRef, CheckCompletion, CheckReceipt, EvidenceResult, EvidenceStatus, ExecutableReceipt,
    ExecutionPlanReceipt, ExecutionReceipt, FailureClassification, InvocationReceipt,
    LauncherReceipt, PlanInput, ProducerBindingReceipt, SourceMaterializationReceipt,
    SourceReceipt, ToolReceipt, PLAN_SCHEMA_VERSION,
};
pub use gate::{
    current_source_ref, run_all, verify_and_write_report, verify_layer_evidence,
    ReportWriteOutcome, RunAllOptions, RunAllOutcome,
};
pub use plan::{ExecutionPlan, PlanOptions};
pub use producer::{produce, ProducerOptions, ProducerOutcome};
pub use provenance::image::ensure_immutable_producer;
pub use verdict::{ClauseVerdict, InvariantVerdict, VerdictReport, VerdictStatus};
pub use verification::{validate_detector_fixture_sources, DetectorFixtureSourceBinding};

#[cfg(test)]
mod tests;
