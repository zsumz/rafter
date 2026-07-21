//! Private Cargo command construction and bounded capability execution.

use std::{
    collections::BTreeMap,
    error::Error,
    ffi::OsString,
    time::{Duration, Instant},
};

use crate::{
    execution::filesystem::ChildDirectory, verification::source::AuthenticatedCompilationSource,
};

use super::super::{
    process::{self, ReplayCommand, ReplayProcessOutput},
    toolchain::ReplayToolchain,
    workspace::{ReplayBindings, ReplayWorkspace},
};

pub(super) struct CargoExecution<'run, 'source> {
    pub(super) environment: &'run BTreeMap<String, String>,
    pub(super) source: &'run AuthenticatedCompilationSource<'source>,
    pub(super) workspace: &'run ReplayWorkspace,
    pub(super) bindings: &'run ReplayBindings,
    pub(super) vendor: &'run ChildDirectory,
    pub(super) toolchain: &'run ReplayToolchain,
}

impl CargoExecution<'_, '_> {
    pub(super) fn run(
        &self,
        arguments: &[OsString],
        budget: process::ReplayProcessBudget,
    ) -> Result<ReplayProcessOutput, Box<dyn Error>> {
        self.source.revalidate()?;
        self.toolchain.revalidate(self.source)?;
        self.workspace.verify()?;
        let cargo = self
            .toolchain
            .cargo_path()
            .to_str()
            .ok_or("detector replay Cargo path is not UTF-8")?;
        let command =
            ReplayCommand::bind(cargo, arguments, self.environment, self.source.workspace())?;
        if command.program_sha256() != self.toolchain.cargo_sha256() {
            return Err("descriptor-bound replay Cargo digest changed".into());
        }
        #[cfg(unix)]
        let inherited = [
            self.bindings.target.descriptor(),
            self.bindings.cargo_home.descriptor(),
            self.bindings.temporary.descriptor(),
            self.vendor.descriptor(),
            self.toolchain.rustc_descriptor(),
        ];
        #[cfg(not(unix))]
        let inherited = [];
        let output = process::run(&command, self.environment, budget, &inherited)?;
        self.source.revalidate()?;
        self.toolchain.revalidate(self.source)?;
        self.workspace.verify()?;
        Ok(output)
    }
}

pub(super) fn environment(
    bindings: &ReplayBindings,
    toolchain: &ReplayToolchain,
) -> Result<BTreeMap<String, String>, Box<dyn Error>> {
    let mut environment = process::environment();
    environment.remove("CARGO_HOME");
    environment.remove("HOME");
    environment.extend([
        (
            "CARGO_HOME".to_owned(),
            child_path(&bindings.cargo_home)?.to_owned(),
        ),
        (
            "CARGO_TARGET_DIR".to_owned(),
            child_path(&bindings.target)?.to_owned(),
        ),
        ("CARGO_INCREMENTAL".to_owned(), "0".to_owned()),
        ("CARGO_NET_OFFLINE".to_owned(), "true".to_owned()),
        ("RUST_BACKTRACE".to_owned(), "1".to_owned()),
        (
            "RUSTC".to_owned(),
            toolchain
                .rustc_child_path()
                .to_str()
                .ok_or("detector replay rustc descriptor path is not UTF-8")?
                .to_owned(),
        ),
        (
            "TMPDIR".to_owned(),
            child_path(&bindings.temporary)?.to_owned(),
        ),
    ]);
    Ok(environment)
}

pub(super) fn source_replacement(vendor: &ChildDirectory) -> Result<[String; 2], Box<dyn Error>> {
    let path = child_path(vendor)?;
    let encoded = toml::Value::String(path.to_owned()).to_string();
    Ok([
        "source.crates-io.replace-with=\"rafter-authenticated\"".to_owned(),
        format!("source.rafter-authenticated.directory={encoded}"),
    ])
}

pub(super) fn child_path(directory: &ChildDirectory) -> Result<&str, Box<dyn Error>> {
    directory
        .path()
        .to_str()
        .ok_or_else(|| "detector replay child directory path is not UTF-8".into())
}

pub(super) fn strings<const N: usize>(values: [&str; N]) -> Vec<OsString> {
    values.into_iter().map(OsString::from).collect()
}

pub(super) fn remaining(deadline: Instant, label: &str) -> Result<Duration, Box<dyn Error>> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|duration| !duration.is_zero())
        .ok_or_else(|| format!("detector replay {label} exhausted its compile budget").into())
}

pub(super) fn require_success(
    label: &str,
    output: &ReplayProcessOutput,
) -> Result<(), Box<dyn Error>> {
    if output.timed_out {
        return Err(format!("detector replay {label} timed out").into());
    }
    if !output.status.success() {
        return Err(format!(
            "detector replay {label} failed with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    Ok(())
}
