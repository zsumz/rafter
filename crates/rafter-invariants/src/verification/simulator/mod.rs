//! Independent verification of simulator evidence.

mod liveness;

pub(crate) use liveness::{derive_verified_liveness_binding, verify_present_liveness_reports};

#[cfg(test)]
pub(crate) use liveness::{tests as liveness_report_tests, validate_liveness_report};
