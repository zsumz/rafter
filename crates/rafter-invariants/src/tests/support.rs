//! Shared fixture vocabulary for the registry and aggregate scenarios.
//!
//! The suite's synthetic receipts are assembled from the reviewed registry and
//! profile manifest rather than frozen here, so the scenarios over them judge
//! the contract the workspace actually pins.

mod bundles;
mod maelstrom;
mod observations;
mod receipts;
mod scenario;

pub(crate) use bundles::{passing_bundles, passing_bundles_for_profile};
pub(crate) use receipts::plan_receipt;
pub(in crate::tests) use receipts::{
    artifact, invocation_receipt, producer_binding, source_receipt,
};
pub(in crate::tests) use scenario::{aggregate, assert_only_parent_red, verify_layer_bundle};
pub(crate) use scenario::{aggregate_unverified, loaded, synthetic_obligation_summary};
