use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, ExitCode},
};

use clap::{Parser, Subcommand};
use rafter_invariants::{
    aggregate_with_harness_errors, load_bundles, load_evidence, produce, render_junit,
    render_markdown, verify_layer_bundle, Catalog, ProducerOptions, ProfileManifest, VerdictStatus,
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
        Commands::VerifyLayer {
            profile,
            layer,
            result,
            registry,
            manifest,
        } => {
            let catalog = Catalog::load(&registry)?;
            let manifest = ProfileManifest::load(&manifest)?;
            let bundles = load_bundles(&[result])?;
            let [bundle] = bundles.as_slice() else {
                return Err("layer verification requires exactly one result bundle".into());
            };
            verify_layer_bundle(&catalog, &manifest, &profile, &layer, bundle)?;
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
    let mut discovery_errors = Vec::new();
    if results.is_empty() {
        match json_files(&results_dir) {
            Ok(paths) => results = paths,
            Err(error) => discovery_errors.push(format!(
                "discover evidence in {}: {error}",
                results_dir.display()
            )),
        }
    }
    let source_ref = source_ref
        .or_else(|| env::var("RAFTER_SOURCE_REF").ok())
        .unwrap_or_else(git_head);
    let catalog = Catalog::load(&registry)?;
    let manifest = ProfileManifest::load(&manifest)?;
    let mut loaded = load_evidence(&results);
    loaded.harness_errors.extend(discovery_errors);
    let report = aggregate_with_harness_errors(
        &catalog,
        &manifest,
        &profile,
        &source_ref,
        &loaded.bundles,
        &loaded.harness_errors,
    )?;

    fs::create_dir_all(&output_dir)?;
    fs::write(
        output_dir.join(format!("{profile}.json")),
        format!("{}\n", serde_json::to_string_pretty(&report)?),
    )?;
    fs::write(
        output_dir.join(format!("{profile}.xml")),
        render_junit(&report),
    )?;
    fs::write(
        output_dir.join(format!("{profile}.md")),
        render_markdown(&report),
    )?;

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
        report.summary.green, report.summary.total, profile
    );
    Ok(report.summary.green == 44 && report.summary.total == 44)
}

fn json_files(directory: &Path) -> Result<Vec<PathBuf>, std::io::Error> {
    if !directory.exists() {
        return Ok(Vec::new());
    }
    let mut paths = fs::read_dir(directory)?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .collect::<Vec<_>>();
    paths.sort();
    Ok(paths)
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
