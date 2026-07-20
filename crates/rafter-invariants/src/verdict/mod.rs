//! Fail-closed aggregate verdict vocabulary and validation.

mod aggregate;
mod model;
pub(crate) mod report;
mod validate;

pub(crate) use aggregate::reduce;
pub use model::{
    ClauseVerdict, InvariantVerdict, VerdictIssue, VerdictReport, VerdictStatus, VerdictSummary,
    VERDICT_SCHEMA_VERSION,
};
pub(crate) use validate::validate_verdict_report;

#[cfg(test)]
pub(crate) use validate::validate_verdict_value;

#[cfg(test)]
mod tests;
