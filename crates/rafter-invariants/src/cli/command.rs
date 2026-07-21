//! Clap-owned command and option vocabulary.

use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "rafter-invariants")]
#[command(about = "Deterministically aggregate Rafter invariant evidence")]
pub(crate) struct Cli {
    #[command(subcommand)]
    pub(super) command: Commands,
}

#[derive(Debug, Subcommand)]
pub(super) enum Commands {
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
    #[command(name = "seal-verifier-artifacts", hide = true)]
    SealVerifierArtifacts {
        #[arg(long)]
        profile: String,
        #[arg(long, default_value = "verification/raft-invariant-profiles.json")]
        profile_manifest: PathBuf,
        #[arg(long)]
        root: PathBuf,
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long)]
        manifest_sha256: String,
        #[arg(long)]
        archive: PathBuf,
    },
    #[command(name = "verify-verifier-archive", hide = true)]
    VerifyVerifierArchive {
        #[arg(long)]
        profile: String,
        #[arg(long, default_value = "verification/raft-invariant-profiles.json")]
        profile_manifest: PathBuf,
        #[arg(long)]
        archive: PathBuf,
        #[arg(long)]
        archive_sha256: String,
        #[arg(long)]
        manifest_sha256: String,
    },
    #[command(name = "verify-report-set", hide = true)]
    VerifyReportSet {
        #[arg(long)]
        profile: String,
        #[arg(long)]
        report_dir: PathBuf,
        #[arg(long, default_value = "verification/raft-invariants.yaml")]
        registry: PathBuf,
        #[arg(long, default_value = "verification/raft-invariant-profiles.json")]
        manifest: PathBuf,
    },
    #[command(name = "producer-probe", hide = true)]
    ProducerProbe,
}
