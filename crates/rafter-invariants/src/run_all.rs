use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use crate::{
    aggregate_with_harness_errors, capture_invocation, load_evidence, produce_with_plan,
    render_junit, render_markdown, verify_bundle_plan, ExecutionPlan, PlanOptions, VerdictReport,
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

#[derive(Debug)]
/// One official report written only after path-based evidence verification.
pub struct ReportWriteOutcome {
    pub report: VerdictReport,
    pub structural_errors: Vec<String>,
}

/// Returns the bounded, managed Git identity for the active checkout.
///
/// # Errors
///
/// Returns an error when Git does not complete successfully within the
/// identity-command timeout or omits the commit identity.
pub fn current_source_ref() -> Result<String, Box<dyn Error>> {
    crate::producer::source::head_commit()
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
    let source_ref = crate::producer::source::head_commit()?;
    let mut paths = Vec::new();
    let mut structural_errors = Vec::new();
    let mut all_layers_passed = true;

    for layer in &plan.contract().required_layers {
        match produce_with_plan(&plan, layer, &options.results_dir, &invocation) {
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

/// Loads and verifies evidence artifacts before writing official reports.
///
/// # Errors
///
/// Returns an error when aggregation or report writing fails. Unreadable,
/// malformed, stale, or otherwise unverified evidence remains a structural
/// error and makes the written report red.
pub fn verify_and_write_report(
    plan: &ExecutionPlan,
    source_ref: &str,
    evidence_paths: &[PathBuf],
    output_dir: &std::path::Path,
) -> Result<ReportWriteOutcome, Box<dyn Error>> {
    let verified_plan = ExecutionPlan::load(&PlanOptions {
        profile: plan.receipt.profile.clone(),
        registry: PathBuf::from(&plan.receipt.registry.path),
        manifest: PathBuf::from(&plan.receipt.manifest.path),
    })?;
    if verified_plan.receipt != plan.receipt {
        return Err("caller-supplied execution plan does not match active plan inputs".into());
    }
    verify_and_write_report_with_errors(
        &verified_plan,
        source_ref,
        evidence_paths,
        output_dir,
        Vec::new(),
    )
}

/// Verifies one layer result through the same path-based trust boundary used
/// by the aggregate reporter.
///
/// # Errors
///
/// Returns an error unless the path contains exactly one structurally valid
/// bundle for the active execution plan, profile, and layer.
pub fn verify_layer_evidence(
    plan: &ExecutionPlan,
    profile: &str,
    layer: &str,
    evidence_path: &Path,
) -> Result<(), Box<dyn Error>> {
    let verified_plan = ExecutionPlan::load(&PlanOptions {
        profile: plan.receipt.profile.clone(),
        registry: PathBuf::from(&plan.receipt.registry.path),
        manifest: PathBuf::from(&plan.receipt.manifest.path),
    })?;
    if verified_plan.receipt != plan.receipt {
        return Err("caller-supplied execution plan does not match active plan inputs".into());
    }
    if profile != verified_plan.receipt.profile {
        return Err(format!(
            "requested profile {profile} does not match active profile {}",
            verified_plan.receipt.profile
        )
        .into());
    }

    let loaded = load_evidence(&[evidence_path.to_path_buf()]);
    if !loaded.harness_errors.is_empty() {
        return Err(loaded.harness_errors.join("; ").into());
    }
    let [bundle] = loaded.bundles.as_slice() else {
        return Err("layer verification requires exactly one result bundle".into());
    };
    crate::producer::source::verify_layer_contract(layer, &bundle.execution.source)?;
    verify_bundle_plan(bundle, &verified_plan.receipt)?;
    crate::verify_layer_bundle(
        &verified_plan.catalog,
        &verified_plan.manifest,
        profile,
        layer,
        bundle,
    )?;
    Ok(())
}

fn verify_and_write_report_with_errors(
    plan: &ExecutionPlan,
    source_ref: &str,
    evidence_paths: &[PathBuf],
    output_dir: &std::path::Path,
    mut structural_errors: Vec<String>,
) -> Result<ReportWriteOutcome, Box<dyn Error>> {
    let loaded = load_evidence(evidence_paths);
    structural_errors.extend(loaded.harness_errors);
    for bundle in &loaded.bundles {
        if let Err(error) = verify_bundle_plan(bundle, &plan.receipt) {
            structural_errors.push(error.to_string());
        }
        if let Err(error) =
            crate::producer::source::verify_layer_contract(&bundle.runner, &bundle.execution.source)
        {
            structural_errors.push(error.to_string());
        }
    }
    let report = aggregate_with_harness_errors(
        &plan.catalog,
        &plan.manifest,
        &plan.receipt.profile,
        source_ref,
        &loaded.bundles,
        &structural_errors,
    )?;
    write_report(&report, &plan.catalog, &plan.manifest, output_dir)?;
    Ok(ReportWriteOutcome {
        report,
        structural_errors,
    })
}

fn write_report(
    report: &VerdictReport,
    catalog: &crate::Catalog,
    manifest: &crate::ProfileManifest,
    output_dir: &std::path::Path,
) -> Result<(), Box<dyn Error>> {
    crate::verdict::validate_verdict_report(report, catalog, manifest)?;
    fs::create_dir_all(output_dir)?;
    let outputs = [
        (
            output_dir.join(format!("{}.json", report.profile)),
            format!("{}\n", serde_json::to_string_pretty(report)?).into_bytes(),
        ),
        (
            output_dir.join(format!("{}.xml", report.profile)),
            render_junit(report).into_bytes(),
        ),
        (
            output_dir.join(format!("{}.md", report.profile)),
            render_markdown(report).into_bytes(),
        ),
    ];
    for (path, contents) in &outputs {
        atomic_write(path.clone(), contents)?;
    }
    for (path, contents) in &outputs {
        verify_written_report(path, contents)?;
    }
    Ok(())
}

fn verify_written_report(path: &Path, expected: &[u8]) -> Result<(), Box<dyn Error>> {
    let actual = fs::read(path)?;
    if actual == expected {
        Ok(())
    } else {
        Err(format!(
            "written report {} does not match the rendered output",
            path.display()
        )
        .into())
    }
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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{
        run_all, verify_and_write_report, verify_written_report, RunAllOptions, RunAllOutcome,
    };

    static TEST_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn run_all_does_not_accept_a_caller_authored_invocation_receipt() {
        let _: fn(&RunAllOptions) -> Result<RunAllOutcome, Box<dyn std::error::Error>> = run_all;
    }

    #[test]
    fn official_writer_reloads_and_rejects_unverified_passing_bundles() {
        let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "rafter-invariants-official-writer-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).expect("create report scratch directory");
        let (catalog, manifest) = crate::tests::loaded();
        let fabricated = crate::tests::passing_bundles(&catalog, &manifest)
            .into_iter()
            .find(|bundle| bundle.runner == "tests")
            .expect("fabricated passing tests bundle");
        let plan = crate::ExecutionPlan {
            catalog,
            manifest,
            receipt: fabricated.execution.plan.clone(),
        };
        let evidence = root.join("fabricated.json");
        std::fs::write(
            &evidence,
            serde_json::to_vec_pretty(&fabricated).expect("serialize fabricated bundle"),
        )
        .expect("write fabricated evidence");

        let error = verify_and_write_report(
            &plan,
            &fabricated.source_ref,
            &[evidence],
            &root.join("report"),
        )
        .expect_err("official writer must independently reload its active plan");
        assert!(error
            .to_string()
            .contains("verification/raft-invariants.yaml"));
        assert!(!root.join("report/pr.json").exists());
        std::fs::remove_dir_all(root).expect("remove report scratch directory");
    }

    #[test]
    fn report_readback_rejects_any_post_write_difference() {
        let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "rafter-invariants-report-readback-{}-{id}.json",
            std::process::id()
        ));
        std::fs::write(&path, b"corrupt").expect("write corrupt report fixture");

        let error = verify_written_report(&path, b"expected")
            .expect_err("a changed official report must fail readback");
        assert!(error
            .to_string()
            .contains("does not match the rendered output"));
        std::fs::remove_file(path).expect("remove report fixture");
    }
}
