//! Producer bootstrap, layer execution, verification, and complete-profile adaptation.

use std::{env, error::Error};

use crate::{plan::ExecutionPlan, producer::ProducerOptions};

use super::{
    model::{CommandOutput, ProduceOptions, RunAllOptions, VerifyLayerOptions},
    output::report_lines,
};

/// Bootstrap the immutable producer image and execute one evidence layer.
///
/// # Errors
///
/// Returns an error when image bootstrap, plan loading, layer execution, or
/// receipt publication fails.
pub fn produce(options: ProduceOptions) -> Result<CommandOutput, Box<dyn Error>> {
    crate::producer::ensure_immutable()?;
    let outcome = crate::producer::produce(&ProducerOptions {
        profile: options.plan.profile,
        layer: options.layer,
        registry: options.plan.registry,
        manifest: options.plan.manifest,
        output_dir: options.output_dir,
    })?;
    Ok(CommandOutput::new(
        outcome.all_passed,
        vec![format!("wrote {}", outcome.path.display())],
    ))
}

/// Bootstrap the immutable producer image and execute a complete profile.
///
/// # Errors
///
/// Returns an error when bootstrap, production, verification, or publication
/// cannot complete structurally.
pub fn run_all(options: RunAllOptions) -> Result<CommandOutput, Box<dyn Error>> {
    crate::producer::ensure_immutable()?;
    let outcome = crate::gate::run_all(&crate::gate::RunAllOptions {
        plan: options.plan.plan_options(),
        results_dir: options.results_dir,
        output_dir: options.output_dir,
    })?;
    let lines = report_lines(&outcome.report);
    if !outcome.structural_errors.is_empty() {
        return Ok(CommandOutput::structurally_failed(
            lines,
            outcome.structural_errors.join("; "),
        ));
    }
    let success = outcome.all_layers_passed
        && outcome.report.summary.green == 44
        && outcome.report.summary.total == 44;
    Ok(CommandOutput::new(success, lines))
}

/// Independently verify one produced evidence layer.
///
/// # Errors
///
/// Returns an error when the plan, source identity, receipt, or artifacts do
/// not satisfy the selected layer contract.
pub fn verify_layer(options: &VerifyLayerOptions) -> Result<CommandOutput, Box<dyn Error>> {
    let plan = ExecutionPlan::load(&options.plan.plan_options())?;
    crate::gate::verify_layer_evidence(
        &plan,
        &options.plan.profile,
        &options.layer,
        &options.result,
    )?;
    Ok(CommandOutput::passed(format!(
        "verified {}/{} evidence",
        options.plan.profile, options.layer
    )))
}

/// Bootstrap the immutable producer image and report its executable path.
///
/// # Errors
///
/// Returns an error when immutable-image bootstrap or executable-path capture
/// fails.
pub fn producer_probe() -> Result<CommandOutput, Box<dyn Error>> {
    crate::producer::ensure_immutable()?;
    Ok(CommandOutput::passed(
        env::current_exe()?.display().to_string(),
    ))
}
