//! Published report-set semantic readback adaptation.

use std::error::Error;

use crate::contract::{catalog::Catalog, profile::ProfileManifest};

use super::model::{CommandOutput, VerifyReportSetOptions};

/// Verify one JSON, `JUnit`, and Markdown report set as exact verdict projections.
///
/// # Errors
///
/// Returns an error when contract loading, report parsing, semantic validation,
/// or canonical-rendering readback fails.
pub fn execute(options: &VerifyReportSetOptions) -> Result<CommandOutput, Box<dyn Error>> {
    let catalog = Catalog::load(&options.plan.registry)?;
    let manifest = ProfileManifest::load(&options.plan.manifest)?;
    crate::gate::verify_report_set(
        &options.report_dir,
        &options.plan.profile,
        &catalog,
        &manifest,
    )?;
    Ok(CommandOutput::passed(format!(
        "verified {} report set",
        options.plan.profile
    )))
}
