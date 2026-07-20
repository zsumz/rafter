//! Complete-profile orchestration from immutable plan through verified report.

use std::{error::Error, path::PathBuf};

use crate::{
    plan::{capture_invocation, ExecutionPlan, PlanOptions},
    verdict::VerdictReport,
};

use super::{check::verify_and_write_report_with_errors, run::produce_layer};

#[derive(Clone, Debug)]
/// Inputs and output locations for one complete profile execution.
pub struct RunAllOptions {
    pub plan: PlanOptions,
    pub results_dir: PathBuf,
    pub output_dir: PathBuf,
}

#[derive(Debug)]
/// Aggregate report plus producer-level structural failures.
pub struct RunAllOutcome {
    pub report: VerdictReport,
    pub structural_errors: Vec<String>,
    pub all_layers_passed: bool,
}

/// Returns the bounded, managed Git identity for the active checkout.
///
/// # Errors
///
/// Returns an error when Git does not complete successfully within the
/// identity-command timeout or omits the commit identity.
pub fn current_source_ref() -> Result<String, Box<dyn Error>> {
    crate::plan::current_source_ref()
}

/// Executes each required layer from one immutable plan and aggregates only
/// the bundles written by this invocation.
///
/// # Errors
///
/// Returns an error when the plan cannot be loaded or the final report cannot
/// be constructed or written. Producer failures are retained in the returned
/// outcome and rendered as red invariant verdicts.
pub fn run_all(options: &RunAllOptions) -> Result<RunAllOutcome, Box<dyn Error>> {
    let plan = ExecutionPlan::load(&options.plan)?;
    let invocation = capture_invocation()?;
    let source_ref = crate::plan::current_source_ref()?;
    let mut paths = Vec::new();
    let mut structural_errors = Vec::new();
    let mut all_layers_passed = true;

    for layer in &plan.contract().required_layers {
        match produce_layer(&plan, layer, &options.results_dir, &invocation) {
            Ok(outcome) => {
                all_layers_passed &= outcome.all_passed;
                paths.push(outcome.path);
            }
            Err(error) => {
                all_layers_passed = false;
                structural_errors.push(format!("produce {layer} evidence: {error}"));
            }
        }
    }

    let outcome = verify_and_write_report_with_errors(
        &plan,
        &source_ref,
        &paths,
        &options.output_dir,
        structural_errors,
    )?;
    Ok(RunAllOutcome {
        report: outcome.report,
        structural_errors: outcome.structural_errors,
        all_layers_passed,
    })
}

#[cfg(test)]
mod tests;
