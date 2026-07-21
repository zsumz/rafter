//! Typed trust boundary between unverified runner output and verdict reduction.

mod identity;
mod json;
mod model;
mod paths;
mod preflight;
mod receipt;
mod receipt_file;
mod replay;
mod verify;

#[cfg(test)]
pub(crate) use model::IntakeDefectKind;
pub(crate) use model::{EvidenceIntake, IntakeDefect, VerificationRequest};
#[cfg(test)]
pub(crate) use paths::verify_paths;
pub(crate) use paths::{verify_aggregate_paths, verify_layer_paths};
pub(crate) use verify::require_passing_layer;

#[cfg(test)]
pub(crate) use verify::verify_receipts_for_test;

#[cfg(test)]
mod tests;
