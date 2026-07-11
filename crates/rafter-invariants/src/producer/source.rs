use std::{error::Error, fs, process::Command};

use sha2::{Digest, Sha256};

use crate::SourceReceipt;

use super::process;

pub(super) fn capture() -> Result<SourceReceipt, Box<dyn Error>> {
    let status = command_output(
        "git",
        &["status", "--porcelain=v1", "--untracked-files=all"],
        true,
    )?;
    if !status.trim().is_empty() {
        return Err("evidence producers require a clean tracked and untracked worktree".into());
    }
    let commit = git(&["rev-parse", "HEAD"])?;
    let tree = git(&["rev-parse", "HEAD^{tree}"])?;
    let cargo = command_output("cargo", &["-vV"], false)?;
    let rustc = command_output("rustc", &["-vV"], false)?;
    let target = rustc
        .lines()
        .find_map(|line| line.strip_prefix("host: "))
        .ok_or("rustc -vV omitted host target")?
        .to_owned();
    let cargo_lock = fs::read("Cargo.lock")?;
    let environment = process::base_environment();
    let encoded_environment = environment
        .iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("\0");
    Ok(SourceReceipt {
        commit,
        tree,
        cargo_lock_sha256: format!("{:x}", Sha256::digest(cargo_lock)),
        cargo,
        rustc,
        target,
        build_profile: "test".to_owned(),
        features: vec!["no-default-features".to_owned()],
        environment_sha256: format!("{:x}", Sha256::digest(encoded_environment)),
        clean: true,
    })
}

pub(super) fn verify(expected: &SourceReceipt) -> Result<(), Box<dyn Error>> {
    let observed = capture()?;
    if &observed != expected {
        return Err("source or toolchain identity changed during evidence execution".into());
    }
    Ok(())
}

fn git(arguments: &[&str]) -> Result<String, Box<dyn Error>> {
    command_output("git", arguments, false)
}

fn command_output(
    program: &str,
    arguments: &[&str],
    allow_empty: bool,
) -> Result<String, Box<dyn Error>> {
    let output = Command::new(program).args(arguments).output()?;
    if !output.status.success() {
        return Err(format!(
            "{program} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
        .into());
    }
    let value = String::from_utf8(output.stdout)?.trim().to_owned();
    if value.is_empty() && !allow_empty {
        return Err(format!("{program} produced empty identity output").into());
    }
    Ok(value)
}
