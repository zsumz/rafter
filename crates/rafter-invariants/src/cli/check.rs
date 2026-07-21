//! Aggregate-check adaptation and required-result discovery.

use std::{env, path::PathBuf};

use rafter_invariants::{current_source_ref, verify_and_write_report, ExecutionPlan, PlanOptions};

use super::report::print_report;

pub(super) struct Options {
    pub(super) profile: String,
    pub(super) registry: PathBuf,
    pub(super) manifest: PathBuf,
    pub(super) results: Vec<PathBuf>,
    pub(super) results_dir: PathBuf,
    pub(super) output_dir: PathBuf,
    pub(super) source_ref: Option<String>,
}

pub(super) fn execute(options: Options) -> Result<bool, Box<dyn std::error::Error>> {
    let Options {
        profile,
        registry,
        manifest,
        mut results,
        results_dir,
        output_dir,
        source_ref,
    } = options;
    let plan = ExecutionPlan::load(&PlanOptions {
        profile: profile.clone(),
        registry,
        manifest,
    })?;
    if results.is_empty() {
        results = profile_result_files(&results_dir, &profile, &plan.contract().required_layers);
    }
    let source_ref = match source_ref.or_else(|| env::var("RAFTER_SOURCE_REF").ok()) {
        Some(source_ref) => source_ref,
        None => current_source_ref()?,
    };
    let outcome = verify_and_write_report(&plan, &source_ref, &results, &output_dir)?;
    print_report(&outcome.report);
    Ok(outcome.report.summary.green == 44 && outcome.report.summary.total == 44)
}

fn profile_result_files(
    directory: &std::path::Path,
    profile: &str,
    required_layers: &[rafter_invariants::EvidenceLayer],
) -> Vec<PathBuf> {
    let mut paths = required_layers
        .iter()
        .map(|layer| directory.join(format!("{profile}-{}.json", layer.as_str())))
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

#[cfg(test)]
#[path = "check/tests.rs"]
mod tests;
