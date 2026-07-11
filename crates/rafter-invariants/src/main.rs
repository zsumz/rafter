use std::{
    env, fs,
    path::{Path, PathBuf},
    process::{Command, ExitCode},
};

use clap::{Parser, Subcommand};
use rafter_invariants::{
    aggregate, load_bundles, render_junit, render_markdown, Catalog, ProfileManifest, VerdictStatus,
};

#[derive(Debug, Parser)]
#[command(name = "rafter-invariants")]
#[command(about = "Deterministically aggregate Rafter invariant evidence")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
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
    let Commands::Check {
        profile,
        registry,
        manifest,
        mut results,
        results_dir,
        output_dir,
        source_ref,
    } = cli.command;
    if results.is_empty() {
        results = json_files(&results_dir)?;
    }
    let source_ref = source_ref
        .or_else(|| env::var("RAFTER_SOURCE_REF").ok())
        .unwrap_or_else(git_head);
    let catalog = Catalog::load(&registry)?;
    let manifest = ProfileManifest::load(&manifest)?;
    let bundles = load_bundles(&results)?;
    let report = aggregate(&catalog, &manifest, &profile, &source_ref, &bundles)?;

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
            "{label} {} {}/{} evidence checks",
            verdict.invariant_id, verdict.passed_evidence, verdict.required_evidence
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
