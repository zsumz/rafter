//! Invariant-gate orchestration over verified evidence intake.

mod check;
mod report;
mod run;
mod run_all;

pub use check::{verify_and_write_report, verify_layer_evidence, ReportWriteOutcome};
pub use run_all::{current_source_ref, run_all, RunAllOptions, RunAllOutcome};
