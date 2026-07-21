//! Canonical workspace and executable identity checks for Cargo output.

use std::{fs, path::Component, path::Path, path::PathBuf};

use super::target::Target;

pub(super) fn producer_workspace_root() -> Result<PathBuf, String> {
    let current =
        fs::canonicalize(".").map_err(|error| format!("canonicalize workspace: {error}"))?;
    current
        .ancestors()
        .find(|ancestor| {
            ancestor
                .join("crates/rafter-invariant-test/Cargo.toml")
                .is_file()
                && ancestor
                    .join("crates/rafter-invariant-test-macros/Cargo.toml")
                    .is_file()
        })
        .map(Path::to_path_buf)
        .ok_or_else(|| "canonical Rafter workspace is not present".to_owned())
}

pub(super) fn verify_package_identity(
    package_id: &str,
    src_path: &Path,
    target: &Target,
) -> Result<(), String> {
    let current =
        fs::canonicalize(".").map_err(|error| format!("canonicalize workspace: {error}"))?;
    let expected_package_dir = current
        .ancestors()
        .map(|ancestor| ancestor.join("crates").join(&target.package))
        .find(|candidate| candidate.join("Cargo.toml").is_file())
        .ok_or_else(|| format!("workspace package {} is not present", target.package))?;
    let encoded = package_id.strip_prefix("path+file://").ok_or_else(|| {
        format!(
            "Cargo package_id for {} is not a workspace path package",
            target.package
        )
    })?;
    let (package_path, version) = encoded
        .rsplit_once('#')
        .ok_or_else(|| format!("Cargo package_id for {} has no version", target.package))?;
    if version.is_empty() {
        return Err(format!(
            "Cargo package_id for {} has an empty version",
            target.package
        ));
    }
    let observed_package_dir = fs::canonicalize(package_path).map_err(|error| {
        format!(
            "canonicalize Cargo package_id for {}: {error}",
            target.package
        )
    })?;
    let observed_source = fs::canonicalize(src_path).map_err(|error| {
        format!(
            "canonicalize Cargo target source for {}: {error}",
            target.key()
        )
    })?;
    if observed_package_dir != expected_package_dir
        || !observed_source.starts_with(&expected_package_dir)
    {
        return Err(format!(
            "Cargo package_id or source path does not match workspace package {}",
            target.package
        ));
    }
    Ok(())
}

pub(super) fn canonical_test_executable(
    executable: &Path,
    target: &Target,
) -> Result<PathBuf, String> {
    if !executable.is_absolute()
        || executable
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(format!(
            "Cargo emitted a non-canonical executable for {}",
            target.key()
        ));
    }
    let canonical = fs::canonicalize(executable).map_err(|error| {
        format!(
            "canonicalize Cargo executable for {}: {error}",
            target.key()
        )
    })?;
    let expected_prefix = target.name.replace('-', "_");
    let file_name = canonical
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| format!("Cargo executable for {} is not UTF-8", target.key()))?;
    if file_name != expected_prefix
        && !file_name
            .strip_prefix(&expected_prefix)
            .is_some_and(|suffix| suffix.starts_with('-'))
    {
        return Err(format!(
            "Cargo executable {file_name} does not match target {}",
            target.name
        ));
    }
    Ok(canonical)
}
