//! Aggregation command adaptation and required-result discovery.

use std::{env, error::Error, path::Path, path::PathBuf};

use crate::{contract::catalog::EvidenceLayer, plan::ExecutionPlan};

use super::{
    model::{CheckOptions, CommandOutput},
    output::report_lines,
};

/// Aggregate existing evidence paths and publish the canonical report set.
///
/// # Errors
///
/// Returns an error when plan loading, source identity, evidence verification,
/// or report publication fails.
pub fn execute(options: CheckOptions) -> Result<CommandOutput, Box<dyn Error>> {
    let plan = ExecutionPlan::load(&options.plan.plan_options())?;
    let results = if options.results.is_empty() {
        profile_result_files(
            &options.results_dir,
            &options.plan.profile,
            &plan.contract().required_layers,
        )
    } else {
        options.results
    };
    let source_ref = match options
        .source_ref
        .or_else(|| env::var("RAFTER_SOURCE_REF").ok())
    {
        Some(source_ref) => source_ref,
        None => crate::gate::current_source_ref()?,
    };
    let outcome =
        crate::gate::verify_and_write_report(&plan, &source_ref, &results, &options.output_dir)?;
    let success = outcome.report.summary.green == 44 && outcome.report.summary.total == 44;
    Ok(CommandOutput::new(success, report_lines(&outcome.report)))
}

fn profile_result_files(
    directory: &Path,
    profile: &str,
    required_layers: &[EvidenceLayer],
) -> Vec<PathBuf> {
    let mut paths = required_layers
        .iter()
        .map(|layer| directory.join(format!("{profile}-{}.json", layer.as_str())))
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

#[cfg(test)]
#[path = "check_tests.rs"]
mod tests;
