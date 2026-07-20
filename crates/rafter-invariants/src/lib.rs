//! Deterministic aggregation for Rafter's invariant evidence contract.

mod aggregate;
mod artifact_verify;
mod artifact_verify_maelstrom;
mod artifact_verify_maelstrom_support;
#[cfg(test)]
mod artifact_verify_maelstrom_tests;
mod artifact_verify_tla;
mod contract;
mod evidence;
mod execution;
mod plan;
mod producer;
mod producer_image;
mod provenance;
mod receipt;
mod receipt_maelstrom;
mod receipt_simulator;
mod receipt_tests;
mod receipt_tla;
mod render;
mod run_all;
mod rust_target;
mod verdict;
mod verification;

#[cfg(test)]
pub(crate) use aggregate::aggregate;
pub(crate) use aggregate::{aggregate_with_harness_errors, load_evidence, verify_layer_bundle};
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
pub(crate) use plan::{capture_invocation, verify_bundle_plan};
pub use plan::{ExecutionPlan, PlanOptions};
pub(crate) use producer::produce_with_plan;
pub use producer::{produce, ProducerOptions, ProducerOutcome};
pub use producer_image::ensure_immutable_producer;
pub(crate) use render::{render_junit, render_markdown};
pub use run_all::{
    current_source_ref, run_all, verify_and_write_report, verify_layer_evidence,
    ReportWriteOutcome, RunAllOptions, RunAllOutcome,
};
pub use verdict::{ClauseVerdict, InvariantVerdict, VerdictReport, VerdictStatus};
pub use verification::{validate_detector_fixture_sources, DetectorFixtureSourceBinding};

#[cfg(test)]
mod tests;
