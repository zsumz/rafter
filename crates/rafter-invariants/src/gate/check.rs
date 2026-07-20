//! Verification and publication of reports from existing evidence paths.

use std::{
    error::Error,
    path::{Path, PathBuf},
};

use crate::{
    plan::{ExecutionPlan, PlanOptions},
    verdict::VerdictReport,
};

use super::report::write;

#[derive(Debug)]
/// One official report written only after path-based evidence verification.
pub struct ReportWriteOutcome {
    pub report: VerdictReport,
    pub structural_errors: Vec<String>,
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
    output_dir: &Path,
) -> Result<ReportWriteOutcome, Box<dyn Error>> {
    let verified_plan = reload_plan(plan)?;
    verify_and_write_report_with_errors(
        &verified_plan,
        source_ref,
        evidence_paths,
        output_dir,
        Vec::new(),
    )
}

/// Verifies one layer result through the aggregate reporter's path boundary.
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
    let verified_plan = reload_plan(plan)?;
    if profile != verified_plan.receipt.profile {
        return Err(format!(
            "requested profile {profile} does not match active profile {}",
            verified_plan.receipt.profile
        )
        .into());
    }

    let source_ref = crate::plan::current_source_ref()?;
    let request = crate::verification::VerificationRequest::new(
        &verified_plan.catalog,
        &verified_plan.manifest,
        &verified_plan.receipt,
        &source_ref,
        Path::new("."),
    );
    let intake =
        crate::verification::verify_layer_paths(request, layer, evidence_path.to_path_buf())?;
    crate::verification::require_passing_layer(request, layer, &intake)?;
    Ok(())
}

pub(super) fn verify_and_write_report_with_errors(
    plan: &ExecutionPlan,
    source_ref: &str,
    evidence_paths: &[PathBuf],
    output_dir: &Path,
    structural_errors: Vec<String>,
) -> Result<ReportWriteOutcome, Box<dyn Error>> {
    let request = crate::verification::VerificationRequest::new(
        &plan.catalog,
        &plan.manifest,
        &plan.receipt,
        source_ref,
        Path::new("."),
    );
    let defects = structural_errors
        .into_iter()
        .map(crate::verification::IntakeDefect::unverifiable)
        .collect();
    let intake = crate::verification::verify_paths(request, evidence_paths, defects)?;
    let report = crate::verdict::reduce(&plan.catalog, &plan.manifest, &intake)?;
    let structural_errors = intake.defect_messages();
    write(&report, &plan.catalog, &plan.manifest, output_dir)?;
    Ok(ReportWriteOutcome {
        report,
        structural_errors,
    })
}

fn reload_plan(plan: &ExecutionPlan) -> Result<ExecutionPlan, Box<dyn Error>> {
    let verified = ExecutionPlan::load(&PlanOptions {
        profile: plan.receipt.profile.clone(),
        registry: PathBuf::from(&plan.receipt.registry.path),
        manifest: PathBuf::from(&plan.receipt.manifest.path),
    })?;
    if verified.receipt != plan.receipt {
        return Err("caller-supplied execution plan does not match active plan inputs".into());
    }
    Ok(verified)
}
