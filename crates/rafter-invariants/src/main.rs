use std::{
    env,
    path::{Path, PathBuf},
    process::{Command, ExitCode},
};

use clap::{Parser, Subcommand};
use rafter_invariants::{
    aggregate_with_harness_errors, capture_invocation, load_bundles, load_evidence, produce,
    run_all, verify_bundle_plan, verify_layer_bundle, write_report, ExecutionPlan, PlanOptions,
    ProducerOptions, RunAllOptions, VerdictReport, VerdictStatus,
};

#[derive(Debug, Parser)]
#[command(name = "rafter-invariants")]
#[command(about = "Deterministically aggregate Rafter invariant evidence")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

struct CheckOptions {
    profile: String,
    registry: PathBuf,
    manifest: PathBuf,
    results: Vec<PathBuf>,
    results_dir: PathBuf,
    output_dir: PathBuf,
    source_ref: Option<String>,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Check {
        #[arg(long)]
        profile: String,
        #[arg(long, default_value = "verification/raft-invariants.yaml")]
        registry: PathBuf,
        #[arg(long, default_value = "verification/raft-invariant-profiles.json")]
        manifest: PathBuf,
        #[arg(long = "result")]
        results: Vec<PathBuf>,
        #[arg(long, default_value = "artifacts/invariants")]
        results_dir: PathBuf,
        #[arg(long, default_value = "target/rafter-invariants")]
        output_dir: PathBuf,
        #[arg(long)]
        source_ref: Option<String>,
    },
    Run {
        #[arg(long)]
        profile: String,
        #[arg(long)]
        layer: String,
        #[arg(long, default_value = "verification/raft-invariants.yaml")]
        registry: PathBuf,
        #[arg(long, default_value = "verification/raft-invariant-profiles.json")]
        manifest: PathBuf,
        #[arg(long, default_value = "artifacts/invariants")]
        output_dir: PathBuf,
    },
    RunAll {
        #[arg(long)]
        profile: String,
        #[arg(long, default_value = "verification/raft-invariants.yaml")]
        registry: PathBuf,
        #[arg(long, default_value = "verification/raft-invariant-profiles.json")]
        manifest: PathBuf,
        #[arg(long, default_value = "artifacts/invariants")]
        results_dir: PathBuf,
        #[arg(long, default_value = "target/rafter-invariants")]
        output_dir: PathBuf,
    },
    VerifyLayer {
        #[arg(long)]
        profile: String,
        #[arg(long)]
        layer: String,
        #[arg(long)]
        result: PathBuf,
        #[arg(long, default_value = "verification/raft-invariants.yaml")]
        registry: PathBuf,
        #[arg(long, default_value = "verification/raft-invariant-profiles.json")]
        manifest: PathBuf,
    },
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(green) if green => ExitCode::SUCCESS,
        Ok(_) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("rafter-invariants: {error}");
            ExitCode::from(2)
        }
    }
}

fn run(cli: Cli) -> Result<bool, Box<dyn std::error::Error>> {
    match cli.command {
        Commands::Check {
            profile,
            registry,
            manifest,
            results,
            results_dir,
            output_dir,
            source_ref,
        } => check(CheckOptions {
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
        } => {
            let outcome = produce(&ProducerOptions {
                profile,
                layer,
                registry,
                manifest,
                output_dir,
            })?;
            println!("wrote {}", outcome.path.display());
            Ok(outcome.all_passed)
        }
        Commands::RunAll {
            profile,
            registry,
            manifest,
            results_dir,
            output_dir,
        } => {
            let invocation = capture_invocation()?;
            let outcome = run_all(
                &RunAllOptions {
                    plan: PlanOptions {
                        profile,
                        registry,
                        manifest,
                    },
                    results_dir,
                    output_dir,
                },
                &invocation,
            )?;
            print_report(&outcome.report);
            if !outcome.structural_errors.is_empty() {
                return Err(outcome.structural_errors.join("; ").into());
            }
            Ok(outcome.all_layers_passed
                && outcome.report.summary.green == 44
                && outcome.report.summary.total == 44)
        }
        Commands::VerifyLayer {
            profile,
            layer,
            result,
            registry,
            manifest,
        } => {
            let plan = ExecutionPlan::load(&PlanOptions {
                profile: profile.clone(),
                registry,
                manifest,
            })?;
            let bundles = load_bundles(&[result])?;
            let [bundle] = bundles.as_slice() else {
                return Err("layer verification requires exactly one result bundle".into());
            };
            verify_bundle_plan(bundle, &plan.receipt)?;
            verify_layer_bundle(&plan.catalog, &plan.manifest, &profile, &layer, bundle)?;
            println!("verified {profile}/{layer} evidence");
            Ok(true)
        }
    }
}

fn check(options: CheckOptions) -> Result<bool, Box<dyn std::error::Error>> {
    let CheckOptions {
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
    let source_ref = source_ref
        .or_else(|| env::var("RAFTER_SOURCE_REF").ok())
        .unwrap_or_else(git_head);
    let mut loaded = load_evidence(&results);
    for bundle in &loaded.bundles {
        if let Err(error) = verify_bundle_plan(bundle, &plan.receipt) {
            loaded.harness_errors.push(error.to_string());
        }
    }
    let report = aggregate_with_harness_errors(
        &plan.catalog,
        &plan.manifest,
        &profile,
        &source_ref,
        &loaded.bundles,
        &loaded.harness_errors,
    )?;

    write_report(&report, &output_dir)?;
    print_report(&report);
    Ok(report.summary.green == 44 && report.summary.total == 44)
}

fn print_report(report: &VerdictReport) {
    for verdict in &report.invariants {
        let label = match verdict.status {
            VerdictStatus::Green => "GREEN",
            VerdictStatus::Red => "RED",
        };
        println!(
            "{label} {} {}/{} clauses, {}/{} evidence checks",
            verdict.invariant_id,
            verdict.passed_clauses,
            verdict.required_clauses,
            verdict.passed_evidence,
            verdict.required_evidence
        );
    }
    println!(
        "invariant verdict: {}/{} green ({})",
        report.summary.green, report.summary.total, report.profile
    );
}

fn profile_result_files(
    directory: &Path,
    profile: &str,
    required_layers: &[String],
) -> Vec<PathBuf> {
    let mut paths = required_layers
        .iter()
        .map(|layer| directory.join(format!("{profile}-{layer}.json")))
        .filter(|path| path.exists())
        .collect::<Vec<_>>();
    paths.sort();
    paths
}

fn git_head() -> String {
    Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_owned())
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::profile_result_files;

    static DIRECTORY_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn implicit_discovery_ignores_other_profiles_and_unexpected_json() {
        let id = DIRECTORY_ID.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "rafter-invariants-discovery-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).expect("test directory exists");
        for name in [
            "pr-tests.json",
            "pr-simulator.json",
            "nightly-tests.json",
            "pr-unexpected.json",
        ] {
            std::fs::write(root.join(name), b"not parsed during discovery")
                .expect("fixture writes");
        }

        let paths = profile_result_files(
            &root,
            "pr",
            &["tests".to_owned(), "simulator".to_owned(), "tla".to_owned()],
        );
        assert_eq!(
            paths,
            vec![root.join("pr-simulator.json"), root.join("pr-tests.json")]
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
