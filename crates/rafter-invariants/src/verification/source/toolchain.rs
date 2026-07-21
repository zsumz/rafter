//! Canonical identities of the actual Cargo and rustc binaries behind rustup.

use std::{
    fs,
    path::{Path, PathBuf},
};

use crate::provenance::source::{
    file_sha256, find_executable, identity_probe_at, CheckoutObservation,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ToolchainIdentity {
    cargo: ToolchainProgram,
    rustc: ToolchainProgram,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct ToolchainProgram {
    path: PathBuf,
    sha256: String,
}

impl ToolchainIdentity {
    pub(super) fn capture(
        root: &Path,
        checkout: &CheckoutObservation,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let cargo = ToolchainProgram::capture("cargo", &checkout.cargo, root)?;
        let rustc = ToolchainProgram::capture("rustc", &checkout.rustc, root)?;
        Ok(Self { cargo, rustc })
    }

    pub(super) fn cargo(&self) -> &ToolchainProgram {
        &self.cargo
    }

    pub(super) fn rustc(&self) -> &ToolchainProgram {
        &self.rustc
    }

    pub(super) fn revalidate(&self, root: &Path) -> Result<(), String> {
        self.cargo.revalidate("cargo", root)?;
        self.rustc.revalidate("rustc", root)
    }
}

impl ToolchainProgram {
    fn capture(
        name: &str,
        expected_identity: &str,
        root: &Path,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let path = actual_program(name, root)?;
        let observed = identity_probe_at(
            path.to_str()
                .ok_or_else(|| format!("{name} toolchain path is not UTF-8"))?,
            &["-vV"],
            root,
        )?
        .stdout
        .trim()
        .to_owned();
        if observed != expected_identity {
            return Err(format!(
                "actual {name} toolchain identity does not match the source observation"
            )
            .into());
        }
        Ok(Self {
            sha256: file_sha256(&path)?,
            path,
        })
    }

    pub(super) fn path(&self) -> &Path {
        &self.path
    }

    pub(super) fn sha256(&self) -> &str {
        &self.sha256
    }

    fn revalidate(&self, name: &str, root: &Path) -> Result<(), String> {
        let path = actual_program(name, root).map_err(|error| error.to_string())?;
        let sha256 = file_sha256(&path).map_err(|error| error.to_string())?;
        if path != self.path || sha256 != self.sha256 {
            return Err(format!("{name} toolchain path or digest changed"));
        }
        Ok(())
    }
}

fn actual_program(name: &str, root: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = find_executable(name).ok_or_else(|| format!("{name} is not present on PATH"))?;
    let canonical = fs::canonicalize(&path)?;
    let rustup = find_executable("rustup")
        .map(fs::canonicalize)
        .transpose()?;
    if rustup.as_ref() != Some(&canonical) {
        return Ok(canonical);
    }
    let output = identity_probe_at("rustup", &["which", name], root)?;
    let selected = output.stdout.trim();
    if selected.is_empty() {
        return Err(format!("rustup which {name} produced an empty path").into());
    }
    let selected = fs::canonicalize(selected)?;
    if !selected.is_file() {
        return Err(format!("rustup-selected {name} is not a regular file").into());
    }
    Ok(selected)
}
