//! Deterministic aggregation for Rafter's invariant evidence contract.

mod aggregate;
mod artifact_verify;
mod artifact_verify_tla;
mod catalog;
mod producer;
mod receipt;
mod receipt_simulator;
mod receipt_tests;
mod receipt_tla;
mod registry_parse;
mod render;
mod types;

pub use aggregate::{aggregate, load_bundles, AggregateError};
pub use catalog::{
    Catalog, CatalogError, EvidenceDescriptor, ProfileManifest, SimulatorIdentity, TestIdentity,
};
pub use producer::{produce, ProducerOptions, ProducerOutcome};
pub use render::{render_junit, render_markdown};
pub use types::{
    ArtifactRef, CheckCompletion, CheckReceipt, EvidenceResult, EvidenceStatus, ExecutionReceipt,
    FailureClassification, InvariantVerdict, ResultBundle, SourceReceipt, ToolReceipt,
    VerdictReport, VerdictStatus,
};

#[cfg(test)]
mod tests;
