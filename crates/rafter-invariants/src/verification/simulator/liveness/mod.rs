//! Semantic acceptance of bounded-liveness reports and their evidence binding.

mod binding;
mod error;
mod inventory;
mod raw;
mod validate;

pub(crate) use binding::{derive_verified_liveness_binding, verify_present_liveness_reports};

#[cfg(test)]
pub(crate) use error::{LivenessReportError, LivenessReportErrorKind};

#[cfg(test)]
pub(crate) use validate::validate_liveness_report;

#[cfg(test)]
pub(crate) mod tests;
