//! Source, toolchain, and clean-checkout observation without acceptance policy.

use std::{
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

use crate::provenance::source::parse_tracked_source_paths;

mod cargo_config;
mod cargo_graph;
mod cargo_inputs;
mod environment;
mod materialization;
mod package_metadata;
mod path_validation;
mod rust_inputs;

#[cfg(test)]
#[path = "checkout/cargo_graph_tests.rs"]
mod cargo_graph_tests;
#[cfg(test)]
#[path = "checkout/observation_tests.rs"]
mod observation_tests;

use cargo_config::cargo_config_sha256;
#[cfg(test)]
use cargo_config::{cargo_config_sha256_with_home, CargoRelease};
use cargo_graph::validate_registry_build_script_source_identity;
use cargo_inputs::validate_trusted_cargo_package_metadata;
#[cfg(test)]
pub(crate) use environment::source_environment_matches_digest;
pub(crate) use environment::source_environment_sha256;
use materialization::capture_materialization;
pub(crate) use materialization::CapturedSourceFile;
pub(crate) use materialization::MaterializationObservation;
use package_metadata::{validate_manifest_path_overrides, validate_resolved_path_package_metadata};
use rust_inputs::validate_resolved_tracked_rust_inputs;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CheckoutObservation {
    pub(crate) commit: String,
    pub(crate) tree: String,
    pub(crate) materialization: MaterializationObservation,
    pub(crate) cargo_lock_sha256: String,
    pub(crate) cargo: String,
    pub(crate) cargo_sha256: String,
    pub(crate) cargo_config_sha256: String,
    pub(crate) rustc: String,
    pub(crate) rustc_sha256: String,
    pub(crate) target: String,
}

#[derive(Debug)]
pub(crate) struct CapturedCheckout {
    pub(crate) observation: CheckoutObservation,
    pub(crate) files: Vec<CapturedSourceFile>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CommandOutput {
    pub(crate) stdout: String,
    pub(crate) stderr: String,
}

pub(crate) trait CheckoutCommandRunner {
    fn run(
        &self,
        program: &str,
        arguments: &[&str],
        current_dir: &Path,
    ) -> Result<CommandOutput, Box<dyn Error>>;
}

pub(crate) trait GeneratedOutputPolicy {
    fn permits(&self, path: &Path) -> bool;
}

struct StandaloneCommandRunner;

impl CheckoutCommandRunner for StandaloneCommandRunner {
    fn run(
        &self,
        program: &str,
        arguments: &[&str],
        current_dir: &Path,
    ) -> Result<CommandOutput, Box<dyn Error>> {
        let output =
            crate::execution::process::run_identity_command_in(program, arguments, current_dir)?;
        Ok(CommandOutput {
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

pub(crate) fn observe_checkout_at(
    root: &Path,
    generated_outputs: &impl GeneratedOutputPolicy,
) -> Result<CheckoutObservation, Box<dyn Error>> {
    observe_checkout_with(root, &StandaloneCommandRunner, generated_outputs)
}

pub(crate) fn capture_checkout_at(
    root: &Path,
    generated_outputs: &impl GeneratedOutputPolicy,
) -> Result<CapturedCheckout, Box<dyn Error>> {
    capture_checkout_with(root, &StandaloneCommandRunner, generated_outputs)
}

pub(crate) fn head_commit_at(root: &Path) -> Result<String, Box<dyn Error>> {
    command_output_at(
        &StandaloneCommandRunner,
        "git",
        &["rev-parse", "HEAD"],
        false,
        root,
    )
}

pub(crate) fn identity_probe_at(
    program: &str,
    arguments: &[&str],
    root: &Path,
) -> Result<CommandOutput, Box<dyn Error>> {
    StandaloneCommandRunner.run(program, arguments, root)
}

pub(crate) fn observe_checkout_with(
    root: &Path,
    runner: &impl CheckoutCommandRunner,
    generated_outputs: &impl GeneratedOutputPolicy,
) -> Result<CheckoutObservation, Box<dyn Error>> {
    capture_checkout_with(root, runner, generated_outputs).map(|capture| capture.observation)
}

fn capture_checkout_with(
    root: &Path,
    runner: &impl CheckoutCommandRunner,
    generated_outputs: &impl GeneratedOutputPolicy,
) -> Result<CapturedCheckout, Box<dyn Error>> {
    require_clean_worktree(root, runner)?;
    let materialized = capture_materialization(root, runner, generated_outputs)?;
    let cargo = command_output_at(runner, "cargo", &["-vV"], false, root)?;
    let rustc = command_output_at(runner, "rustc", &["-vV"], false, root)?;
    let target = rustc
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .ok_or_else(|| format!("rustc -vV omitted host target from output: {rustc:?}"))?
        .to_owned();
    let cargo_lock = fs::read(root.join("Cargo.lock"))?;
    let cargo_config_sha256 = cargo_config_sha256(root, &cargo)?;
    validate_resolved_path_packages(root, runner)?;
    require_clean_worktree(root, runner)?;
    Ok(CapturedCheckout {
        observation: CheckoutObservation {
            commit: materialized.commit,
            tree: materialized.tree,
            materialization: materialized.receipt,
            cargo_lock_sha256: format!("{:x}", Sha256::digest(cargo_lock)),
            cargo,
            cargo_sha256: executable_sha256("cargo")?,
            cargo_config_sha256,
            rustc,
            rustc_sha256: executable_sha256("rustc")?,
            target,
        },
        files: materialized.files,
    })
}

fn require_clean_worktree(
    root: &Path,
    runner: &impl CheckoutCommandRunner,
) -> Result<(), Box<dyn Error>> {
    let status = command_output_at(
        runner,
        "git",
        &["status", "--porcelain=v1", "--untracked-files=all"],
        true,
        root,
    )?;
    if status.trim().is_empty() {
        Ok(())
    } else {
        Err("source observation requires a clean tracked and untracked worktree".into())
    }
}

fn command_output_at(
    runner: &impl CheckoutCommandRunner,
    program: &str,
    arguments: &[&str],
    allow_empty: bool,
    root: &Path,
) -> Result<String, Box<dyn Error>> {
    let stdout = command_stdout_at(runner, program, arguments, root)?;
    let value = stdout.trim().to_owned();
    if value.is_empty() && !allow_empty {
        return Err(format!("{program} produced empty identity output").into());
    }
    Ok(value)
}

fn command_output_raw_at(
    runner: &impl CheckoutCommandRunner,
    program: &str,
    arguments: &[&str],
    allow_empty: bool,
    root: &Path,
) -> Result<String, Box<dyn Error>> {
    let value = command_stdout_at(runner, program, arguments, root)?;
    if value.is_empty() && !allow_empty {
        return Err(format!("{program} produced empty identity output").into());
    }
    Ok(value)
}

fn command_stdout_at(
    runner: &impl CheckoutCommandRunner,
    program: &str,
    arguments: &[&str],
    root: &Path,
) -> Result<String, Box<dyn Error>> {
    let output = runner.run(program, arguments, root)?;
    Ok(output.stdout)
}

fn validate_resolved_path_packages(
    root: &Path,
    runner: &impl CheckoutCommandRunner,
) -> Result<(), Box<dyn Error>> {
    let root = fs::canonicalize(root)?;
    let tracked = command_output_raw_at(runner, "git", &["ls-files", "-z"], true, &root)?;
    let tracked = parse_tracked_source_paths(&tracked)?;
    validate_manifest_path_overrides(&root, &tracked)?;
    let metadata = command_output_at(
        runner,
        "cargo",
        &["metadata", "--format-version", "1", "--locked", "--offline"],
        false,
        &root,
    )?;
    validate_resolved_path_package_metadata(&root, &metadata, &tracked)?;
    validate_registry_build_script_source_identity(
        &metadata,
        &fs::read_to_string(root.join("Cargo.lock"))?,
    )?;
    validate_resolved_tracked_rust_inputs(&root, &tracked, &metadata)?;
    validate_trusted_cargo_package_metadata(&root, &metadata)
}

fn executable_sha256(name: &str) -> Result<String, Box<dyn Error>> {
    let path = find_executable(name).ok_or_else(|| format!("{name} is not present on PATH"))?;
    file_sha256(&path)
}

pub(crate) fn file_sha256(path: &Path) -> Result<String, Box<dyn Error>> {
    Ok(format!("{:x}", Sha256::digest(fs::read(path)?)))
}

pub(crate) fn find_executable(name: &str) -> Option<PathBuf> {
    env::split_paths(&env::var_os("PATH")?)
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
}

#[cfg(test)]
#[path = "checkout/config_tests.rs"]
mod config_tests;
