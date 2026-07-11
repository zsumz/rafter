//! Deterministic aggregation for Rafter's invariant evidence contract.

mod aggregate;
mod catalog;
mod render;
mod types;

pub use aggregate::{aggregate, load_bundles, AggregateError};
pub use catalog::{Catalog, CatalogError, EvidenceDescriptor, ProfileManifest};
pub use render::{render_junit, render_markdown};
pub use types::{
    ArtifactRef, EvidenceResult, EvidenceStatus, FailureClassification, InvariantVerdict,
    ResultBundle, VerdictReport, VerdictStatus,
};

#[cfg(test)]
mod tests;
