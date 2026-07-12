use std::{error::Error, fs, path::PathBuf};

use crate::{
    aggregate_with_harness_errors, load_evidence, produce_with_plan, render_junit, render_markdown,
    verify_bundle_plan, ExecutionPlan, InvocationReceipt, PlanOptions, VerdictReport,
};

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

/// Executes each required layer from one immutable plan and aggregates only
/// the bundles written by this invocation.
///
/// # Errors
///
/// Returns an error when the plan cannot be loaded or the final report cannot
/// be constructed or written. Producer failures are retained in the returned
/// outcome and rendered as red invariant verdicts.
pub fn run_all(
    options: &RunAllOptions,
    invocation: &InvocationReceipt,
) -> Result<RunAllOutcome, Box<dyn Error>> {
    let plan = ExecutionPlan::load(&options.plan)?;
    let source_ref = crate::producer::source::head_commit()?;
    let mut paths = Vec::new();
    let mut structural_errors = Vec::new();
    let mut all_layers_passed = true;

    for layer in &plan.contract().required_layers {
        match produce_with_plan(&plan, layer, &options.results_dir, invocation) {
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

    let mut loaded = load_evidence(&paths);
    structural_errors.append(&mut loaded.harness_errors);
    for bundle in &loaded.bundles {
        if let Err(error) = verify_bundle_plan(bundle, &plan.receipt) {
            structural_errors.push(error.to_string());
        }
    }
    loaded
        .harness_errors
        .extend(structural_errors.iter().cloned());
    let report = aggregate_with_harness_errors(
        &plan.catalog,
        &plan.manifest,
        &plan.receipt.profile,
        &source_ref,
        &loaded.bundles,
        &loaded.harness_errors,
    )?;
    write_report(&report, &options.output_dir)?;
    Ok(RunAllOutcome {
        report,
        structural_errors,
        all_layers_passed,
    })
}

/// Writes deterministic JSON, `JUnit`, and Markdown aggregate reports.
///
/// # Errors
///
/// Returns an error when the output directory or a report file cannot be written.
pub fn write_report(
    report: &VerdictReport,
    output_dir: &std::path::Path,
) -> Result<(), Box<dyn Error>> {
    fs::create_dir_all(output_dir)?;
    atomic_write(
        output_dir.join(format!("{}.json", report.profile)),
        format!("{}\n", serde_json::to_string_pretty(report)?).as_bytes(),
    )?;
    atomic_write(
        output_dir.join(format!("{}.xml", report.profile)),
        render_junit(report).as_bytes(),
    )?;
    atomic_write(
        output_dir.join(format!("{}.md", report.profile)),
        render_markdown(report).as_bytes(),
    )?;
    Ok(())
}

fn atomic_write(path: PathBuf, contents: &[u8]) -> Result<(), Box<dyn Error>> {
    let temporary = path.with_extension(format!(
        "{}.tmp-{}",
        path.extension()
            .and_then(|value| value.to_str())
            .unwrap_or("report"),
        std::process::id()
    ));
    fs::write(&temporary, contents)?;
    fs::rename(temporary, path)?;
    Ok(())
}
