//! Command dispatch into library-owned gate operations.

use std::env;

use rafter_invariants::{
    ensure_immutable_producer, produce, run_all, verify_layer_evidence, ExecutionPlan, PlanOptions,
    ProducerOptions, RunAllOptions,
};

use super::{
    check::{self, Options},
    command::{Cli, Commands},
    document, publication,
    report::print_report,
};

pub(crate) fn run(cli: Cli) -> Result<bool, Box<dyn std::error::Error>> {
    match cli.command {
        Commands::Check {
            profile,
            registry,
            manifest,
            results,
            results_dir,
            output_dir,
            source_ref,
        } => check::execute(Options {
            profile,
            registry,
            manifest,
            results,
            results_dir,
            output_dir,
            source_ref,
        }),
        Commands::Run {
            profile,
            layer,
            registry,
            manifest,
            output_dir,
        } => produce_layer(&ProducerOptions {
            profile,
            layer,
            registry,
            manifest,
            output_dir,
        }),
        Commands::RunAll {
            profile,
            registry,
            manifest,
            results_dir,
            output_dir,
        } => execute_all(&RunAllOptions {
            plan: PlanOptions {
                profile,
                registry,
                manifest,
            },
            results_dir,
            output_dir,
        }),
        Commands::VerifyLayer {
            profile,
            layer,
            result,
            registry,
            manifest,
        } => verify_layer(&profile, &layer, &result, registry, manifest),
        Commands::RenderDoc {
            registry,
            output,
            check,
        } => document::execute(&registry, &output, check),
        Commands::SealVerifierArtifacts {
            profile,
            profile_manifest,
            root,
            manifest,
            manifest_sha256,
            archive,
        } => publication::seal(
            &profile,
            &profile_manifest,
            &root,
            &manifest,
            &manifest_sha256,
            &archive,
        ),
        Commands::VerifyVerifierArchive {
            profile,
            profile_manifest,
            archive,
            archive_sha256,
            manifest_sha256,
        } => publication::verify(
            &profile,
            &profile_manifest,
            &archive,
            &archive_sha256,
            &manifest_sha256,
        ),
        Commands::VerifyReportSet {
            profile,
            report_dir,
            registry,
            manifest,
        } => super::report::verify_set(&profile, &report_dir, &registry, &manifest),
        Commands::ProducerProbe => {
            ensure_immutable_producer()?;
            println!("{}", env::current_exe()?.display());
            Ok(true)
        }
    }
}

fn produce_layer(options: &ProducerOptions) -> Result<bool, Box<dyn std::error::Error>> {
    ensure_immutable_producer()?;
    let outcome = produce(options)?;
    println!("wrote {}", outcome.path.display());
    Ok(outcome.all_passed)
}

fn execute_all(options: &RunAllOptions) -> Result<bool, Box<dyn std::error::Error>> {
    ensure_immutable_producer()?;
    let outcome = run_all(options)?;
    print_report(&outcome.report);
    if !outcome.structural_errors.is_empty() {
        return Err(outcome.structural_errors.join("; ").into());
    }
    Ok(outcome.all_layers_passed
        && outcome.report.summary.green == 44
        && outcome.report.summary.total == 44)
}

fn verify_layer(
    profile: &str,
    layer: &str,
    result: &std::path::Path,
    registry: std::path::PathBuf,
    manifest: std::path::PathBuf,
) -> Result<bool, Box<dyn std::error::Error>> {
    let plan = ExecutionPlan::load(&PlanOptions {
        profile: profile.to_owned(),
        registry,
        manifest,
    })?;
    verify_layer_evidence(&plan, profile, layer, result)?;
    println!("verified {profile}/{layer} evidence");
    Ok(true)
}
