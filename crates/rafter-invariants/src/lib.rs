//! Deterministic aggregation for Rafter's invariant evidence contract.

mod aggregate;
mod artifact_verify;
mod artifact_verify_maelstrom;
mod artifact_verify_maelstrom_support;
#[cfg(test)]
mod artifact_verify_maelstrom_tests;
mod artifact_verify_tla;
mod catalog;
mod producer;
mod receipt;
mod receipt_maelstrom;
mod receipt_simulator;
mod receipt_tests;
mod receipt_tla;
mod registry_parse;
mod render;
mod types;

pub use aggregate::{
    aggregate, aggregate_with_harness_errors, load_bundles, load_evidence, verify_layer_bundle,
    AggregateError, LoadedEvidence,
};
pub use catalog::{
    Catalog, CatalogError, ClauseDescriptor, EvidenceDescriptor, InvariantDescriptor,
    ProfileManifest, SimulatorIdentity, TestIdentity,
};
pub use producer::{produce, ProducerOptions, ProducerOutcome};
pub use render::{render_junit, render_markdown};
pub use types::{
    ArtifactRef, CheckCompletion, CheckReceipt, ClauseVerdict, EvidenceResult, EvidenceStatus,
    ExecutionReceipt, FailureClassification, InvariantVerdict, ResultBundle, SourceReceipt,
    ToolReceipt, VerdictReport, VerdictStatus,
};

#[cfg(test)]
mod tests;
