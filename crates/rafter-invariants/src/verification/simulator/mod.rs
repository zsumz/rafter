//! Independent verification of simulator evidence.

mod detector;
mod event;
mod liveness;
mod observation;
mod receipt;
pub(crate) mod schedule;
mod verify;

#[cfg(test)]
pub(crate) mod event_semantics_test_support;

pub(crate) use detector::DetectorLogVerifier;
pub(crate) use liveness::{derive_verified_liveness_binding, verify_present_liveness_reports};
pub(crate) use receipt::validate as validate_receipt;
pub(crate) use verify::verify_simulator_logs;

#[cfg(test)]
pub(crate) use liveness::{tests as liveness_report_tests, validate_liveness_report};
#[cfg(test)]
pub(crate) use observation::verify_liveness_observations;
