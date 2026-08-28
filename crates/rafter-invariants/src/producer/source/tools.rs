//! Producer-side capture of external tool identity for a source receipt.
//!
//! Each reviewed tool is probed with its registered arguments, and the binding
//! records both the executable digest and any adjacent input the launcher
//! reaches, so a swapped payload cannot hide behind a stable version string.

use std::{
    env,
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use sha2::{Digest, Sha256};

use crate::{evidence::ToolReceipt, provenance::source::CheckoutCommandRunner};

use super::{maelstrom_jar_path, ProducerCommandRunner};

const TOOL_IDENTITY_PROBES: &[(&str, &[&str])] = &[
    ("java", &["-version"]),
    ("maelstrom", &["serve", "--help"]),
    ("dot", &["-V"]),
    ("gnuplot", &["--version"]),
];

pub(super) fn capture_tool(
    name: &str,
    root: &Path,
    runner: ProducerCommandRunner,
) -> Result<ToolReceipt, Box<dyn Error>> {
    let executable = find_tool(name).ok_or_else(|| format!("{name} is not present on PATH"))?;
    let arguments = tool_identity_arguments(name)?;
    let output = runner.run(name, arguments, root)?;
    let version = bind_adjacent_tool_inputs(
        name,
        tool_version_output(name, &output.stdout, &output.stderr)?,
        &executable,
    )?;
    Ok(ToolReceipt {
        version,
        sha256: file_sha256(&executable)?,
    })
}

pub(super) fn bind_adjacent_tool_inputs(
    name: &str,
    version: String,
    executable: &Path,
) -> Result<String, Box<dyn Error>> {
    if name != "maelstrom" {
        return Ok(version);
    }
    let executable = fs::canonicalize(executable)?;
    let jar = maelstrom_jar_path(&executable)?;
    let jar_sha256 = file_sha256(&jar).map_err(|error| {
        format!(
            "bind Maelstrom launcher {} to adjacent {}: {error}",
            executable.display(),
            jar.display()
        )
    })?;
    Ok(format!(
        "{version}\nrafter-adjacent-lib/maelstrom.jar-sha256: {jar_sha256}"
    ))
}

pub(super) fn tool_identity_arguments(
    name: &str,
) -> Result<&'static [&'static str], Box<dyn Error>> {
    TOOL_IDENTITY_PROBES
        .iter()
        .find_map(|(tool, arguments)| (*tool == name).then_some(*arguments))
        .ok_or_else(|| format!("no reviewed identity probe is registered for {name}").into())
}

pub(super) fn tool_version_output(
    name: &str,
    stdout: &str,
    stderr: &str,
) -> Result<String, Box<dyn Error>> {
    let value = format!("{stdout}{stderr}").trim().to_owned();
    if value.is_empty() {
        return Err(format!("{name} produced empty identity output").into());
    }
    Ok(value)
}

pub(super) fn find_tool(name: &str) -> Option<PathBuf> {
    env::split_paths(&env::var_os("PATH")?)
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
}

fn file_sha256(path: &Path) -> Result<String, Box<dyn Error>> {
    Ok(format!("{:x}", Sha256::digest(fs::read(path)?)))
}
