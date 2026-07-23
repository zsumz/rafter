//! Command dispatch into library-owned gate operations.

use rafter_invariants::gate::command;

use super::command::{Cli, Commands};

pub(crate) fn run(cli: Cli) -> Result<bool, Box<dyn std::error::Error>> {
    let output = dispatch(cli.command)?;
    for line in output.lines {
        println!("{line}");
    }
    if let Some(error) = output.structural_error {
        return Err(error.into());
    }
    Ok(output.success)
}

fn dispatch(request: Commands) -> Result<command::CommandOutput, Box<dyn std::error::Error>> {
    Ok(match request {
        Commands::Check {
            profile,
            registry,
            manifest,
            results,
            results_dir,
            output_dir,
            source_ref,
        } => command::check(command::CheckOptions {
            plan: plan(profile, registry, manifest),
            results,
            results_dir,
            output_dir,
            source_ref,
        })?,
        Commands::Run {
            profile,
            layer,
            registry,
            manifest,
            output_dir,
        } => command::produce(command::ProduceOptions {
            plan: plan(profile, registry, manifest),
            layer,
            output_dir,
        })?,
        Commands::RunAll {
            profile,
            registry,
            manifest,
            results_dir,
            output_dir,
        } => command::run_all(command::RunAllOptions {
            plan: plan(profile, registry, manifest),
            results_dir,
            output_dir,
        })?,
        Commands::VerifyLayer {
            profile,
            layer,
            result,
            registry,
            manifest,
        } => command::verify_layer(&command::VerifyLayerOptions {
            plan: plan(profile, registry, manifest),
            layer,
            result,
        })?,
        Commands::RenderDoc {
            registry,
            output,
            check,
        } => command::render_document(&command::RenderDocumentOptions {
            registry,
            output,
            check,
        })?,
        Commands::SealVerifierArtifacts {
            profile,
            profile_manifest,
            root,
            manifest,
            manifest_sha256,
            archive,
        } => command::seal_verifier_artifacts(&command::SealVerifierArtifactsOptions {
            profile,
            profile_manifest,
            root,
            manifest,
            manifest_sha256,
            archive,
        })?,
        Commands::VerifyVerifierArchive {
            profile,
            profile_manifest,
            archive,
            archive_sha256,
            manifest_sha256,
        } => command::verify_verifier_archive(&command::VerifyVerifierArchiveOptions {
            profile,
            profile_manifest,
            archive,
            archive_sha256,
            manifest_sha256,
        })?,
        Commands::VerifyReportSet {
            profile,
            report_dir,
            registry,
            manifest,
        } => command::verify_report_set(&command::VerifyReportSetOptions {
            plan: plan(profile, registry, manifest),
            report_dir,
        })?,
        Commands::ProducerProbe => command::producer_probe()?,
    })
}

fn plan(
    profile: String,
    registry: std::path::PathBuf,
    manifest: std::path::PathBuf,
) -> command::PlanSelection {
    command::PlanSelection {
        profile,
        registry,
        manifest,
    }
}
