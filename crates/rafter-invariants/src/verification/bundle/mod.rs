//! Whole-bundle integrity checks and immutable authenticated artifact access.

mod budget;
mod integrity;

pub(crate) use budget::{BundleBudget, ProfileBudget, MAX_RECEIPT_BYTES};
pub(crate) use integrity::declared_artifacts;
pub(crate) use integrity::{authenticate, AuthenticatedArtifacts};
#[cfg(test)]
pub(crate) use integrity::{
    snapshot_available_artifacts, verify as verify_integrity, verify_producer_invocation_paths,
};
