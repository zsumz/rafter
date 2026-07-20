//! Strict tracked-path inventory from the fixed system Git implementation.

use std::{
    collections::{BTreeMap, HashSet},
    error::Error,
    fs,
    path::{Path, PathBuf},
};

#[cfg(test)]
mod tests;

pub(crate) fn parse_tracked_source_paths(output: &str) -> Result<HashSet<PathBuf>, Box<dyn Error>> {
    output
        .split('\0')
        .filter(|value| !value.is_empty())
        .map(|value| {
            let path = PathBuf::from(value);
            if path.is_absolute()
                || path.components().any(|component| {
                    matches!(
                        component,
                        std::path::Component::Prefix(_)
                            | std::path::Component::RootDir
                            | std::path::Component::CurDir
                            | std::path::Component::ParentDir
                    )
                })
            {
                return Err(format!("git reported a non-relative tracked path: {value:?}").into());
            }
            Ok(path)
        })
        .collect()
}

pub(crate) fn tracked_source_paths_at(root: &Path) -> Result<HashSet<PathBuf>, String> {
    let root = fs::canonicalize(root)
        .map_err(|error| format!("canonicalize source root {}: {error}", root.display()))?;
    // This inventory constrains source analysis; raw HEAD-tree bytes independently prove source
    // acceptance. The absolute Git path and minimal environment keep lookup deterministic.
    let output = std::process::Command::new("/usr/bin/git")
        .args(["--no-replace-objects", "ls-files", "-z"])
        .env_clear()
        .envs(source_control_environment())
        .current_dir(&root)
        .output()
        .map_err(|error| format!("enumerate tracked source paths: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "enumerate tracked source paths: git exited with {:?}: {}",
            output.status.code(),
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let output = String::from_utf8(output.stdout)
        .map_err(|error| format!("enumerate tracked source paths: {error}"))?;
    parse_tracked_source_paths(&output)
        .map_err(|error| format!("parse tracked source paths: {error}"))
}

fn source_control_environment() -> BTreeMap<String, String> {
    const ALLOWED: &[&str] = &[
        "CARGO_HOME",
        "DEVELOPER_DIR",
        "HOME",
        "PATH",
        "RUSTUP_HOME",
        "SDKROOT",
        "SYSTEMROOT",
    ];
    ALLOWED
        .iter()
        .filter_map(|name| {
            std::env::var(name)
                .ok()
                .map(|value| ((*name).to_owned(), value))
        })
        .collect()
}
