//! Verifier-owned replay reports and exact bounded process-log artifacts.

mod coordinator;
mod fixtures;
mod model;
mod process;
mod publisher;
mod report;
mod validation;

pub(super) use coordinator::{publish_attempt, publish_preparation_failure};
pub(in crate::verification) use publisher::ReplayArtifactGuard;
#[cfg(test)]
pub(in crate::verification) use validation::canonical_report_value;
pub(in crate::verification) use validation::{validate_report_bundle, ReplayReportExpectation};

#[cfg(test)]
#[path = "tests.rs"]
mod tests;
