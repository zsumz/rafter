//! Invariant-gate orchestration over verified evidence intake.

mod check;
/// Binary command adapters that preserve gate-owned lifecycle preconditions.
pub mod command;
mod report;
mod report_set;
mod run;
mod run_all;

pub use check::{verify_and_write_report, verify_layer_evidence, ReportWriteOutcome};
pub use report_set::verify_report_set;
pub use run_all::{current_source_ref, run_all, RunAllOptions, RunAllOutcome};
