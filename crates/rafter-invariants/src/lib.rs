//! Deterministic aggregation for Rafter's invariant evidence contract.

mod aggregate;
mod artifact_verify;
mod artifact_verify_maelstrom;
mod artifact_verify_maelstrom_support;
#[cfg(test)]
mod artifact_verify_maelstrom_tests;
mod artifact_verify_tla;
mod catalog;
mod plan;
mod producer;
mod receipt;
mod receipt_maelstrom;
mod receipt_simulator;
mod receipt_tests;
mod receipt_tla;
mod registry;
mod registry_document;
mod registry_parse;
mod render;
mod run_all;
mod schema;
mod types;

pub use aggregate::{
    aggregate, aggregate_with_harness_errors, load_bundles, load_evidence, verify_layer_bundle,
    AggregateError, LoadedEvidence,
};
pub use catalog::{
    Catalog, CatalogError, ClauseDescriptor, EvidenceDescriptor, InvariantDescriptor,
    ProfileContract, ProfileManifest, RunnerContract, SimulatorIdentity, TestIdentity,
};
pub use plan::{capture_invocation, verify_bundle_plan, ExecutionPlan, PlanOptions};
pub use producer::{produce, produce_with_plan, ProducerOptions, ProducerOutcome};
pub use registry::{
    RegistryClause, RegistryCounts, RegistryDocument, RegistryEvidence, RegistryInvariant,
    REGISTRY_SCHEMA_VERSION,
};
pub use registry_document::render_registry_markdown;
pub use render::{render_junit, render_markdown};
pub use run_all::{run_all, write_report, RunAllOptions, RunAllOutcome};
pub use types::{
    ArtifactRef, CheckCompletion, CheckReceipt, ClauseVerdict, EvidenceResult, EvidenceStatus,
    ExecutionPlanReceipt, ExecutionReceipt, FailureClassification, InvariantVerdict,
    InvocationReceipt, PlanInput, ResultBundle, SourceReceipt, ToolReceipt, VerdictReport,
    VerdictStatus, PLAN_SCHEMA_VERSION,
};

#[cfg(test)]
mod tests;
