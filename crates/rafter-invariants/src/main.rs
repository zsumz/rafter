use std::{
    env, fs,
    path::{Path, PathBuf},
    process::ExitCode,
};

use clap::{Parser, Subcommand};
use rafter_invariants::{
    current_source_ref, produce, render_registry_markdown, run_all, verify_and_write_report,
    verify_layer_evidence, ExecutionPlan, PlanOptions, ProducerOptions, RegistryDocument,
    RunAllOptions, VerdictReport, VerdictStatus,
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
    RenderDoc {
        #[arg(long, default_value = "verification/raft-invariants.yaml")]
        registry: PathBuf,
        #[arg(long, default_value = "docs/raft-invariants.md")]
        output: PathBuf,
        #[arg(long)]
        check: bool,
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
            let outcome = run_all(&RunAllOptions {
                plan: PlanOptions {
                    profile,
                    registry,
                    manifest,
                },
                results_dir,
                output_dir,
            })?;
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
            verify_layer_evidence(&plan, &profile, &layer, &result)?;
            println!("verified {profile}/{layer} evidence");
            Ok(true)
        }
        Commands::RenderDoc {
            registry,
            output,
            check,
        } => render_doc(&registry, &output, check),
    }
}

fn render_doc(
    registry_path: &Path,
    output_path: &Path,
    check: bool,
) -> Result<bool, Box<dyn std::error::Error>> {
    let registry = RegistryDocument::load(registry_path)?;
    let rendered = render_registry_markdown(&registry);
    if check {
        let current = fs::read_to_string(output_path).map_err(|error| {
            format!(
                "{} is missing or unreadable: {error}; run scripts/render-raft-invariants-doc",
                output_path.display()
            )
        })?;
        if current != rendered {
            return Err(format!(
                "{} is out of date; run scripts/render-raft-invariants-doc",
                output_path.display()
            )
            .into());
        }
        return Ok(true);
    }
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(output_path, rendered)?;
    println!("wrote {}", output_path.display());
    Ok(true)
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
    let source_ref = match source_ref.or_else(|| env::var("RAFTER_SOURCE_REF").ok()) {
        Some(source_ref) => source_ref,
        None => current_source_ref()?,
    };
    let outcome = verify_and_write_report(&plan, &source_ref, &results, &output_dir)?;
    let report = outcome.report;
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

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::{profile_result_files, render_doc};

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

    #[test]
    fn document_check_fails_stale_and_accepts_canonical_output() {
        let id = DIRECTORY_ID.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "rafter-invariants-document-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).expect("test directory exists");
        let workspace = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
        let registry = workspace.join("verification/raft-invariants.yaml");
        let output = root.join("raft-invariants.md");
        std::fs::write(&output, "stale\n").expect("stale fixture writes");

        assert!(render_doc(&registry, &output, true).is_err());
        assert!(render_doc(&registry, &output, false).expect("render document"));
        assert!(render_doc(&registry, &output, true).expect("check current document"));
        let _ = std::fs::remove_dir_all(root);
    }
}
