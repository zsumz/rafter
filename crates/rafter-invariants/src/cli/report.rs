//! Human-readable terminal projection of deterministic verdicts.

use std::path::Path;

use rafter_invariants::{Catalog, ProfileManifest, VerdictReport, VerdictStatus};

pub(super) fn verify_set(
    profile: &str,
    report_dir: &Path,
    registry: &Path,
    manifest: &Path,
) -> Result<bool, Box<dyn std::error::Error>> {
    let catalog = Catalog::load(registry)?;
    let manifest = ProfileManifest::load(manifest)?;
    rafter_invariants::verify_report_set(report_dir, profile, &catalog, &manifest)?;
    println!("verified {profile} report set");
    Ok(true)
}

pub(super) fn print_report(report: &VerdictReport) {
    for result in &report.invariants {
        let label = match result.status {
            VerdictStatus::Green => "GREEN",
            VerdictStatus::Red => "RED",
        };
        println!(
            "{label} {} {}/{} clauses, {}/{} evidence checks",
            result.invariant_id,
            result.passed_clauses,
            result.required_clauses,
            result.passed_evidence,
            result.required_evidence
        );
    }
    println!(
        "invariant verdict: {}/{} green ({})",
        report.summary.green, report.summary.total, report.profile
    );
}
