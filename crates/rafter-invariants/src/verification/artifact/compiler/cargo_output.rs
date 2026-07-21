//! Cargo compiler-message reconstruction and path validation.

use std::path::{Component, Path, PathBuf};

use serde::Deserialize;

use crate::verification::{AggregateError, RecordedWorkspace};

use super::model::{CargoTargetKey, ParsedCompilerArtifact};

#[derive(Debug, Deserialize)]
struct CargoCompilerMessage {
    reason: String,
    package_id: Option<String>,
    target: Option<CargoMessageTarget>,
    fresh: Option<bool>,
    executable: Option<PathBuf>,
}

#[derive(Debug, Deserialize)]
struct CargoMessageTarget {
    kind: Vec<String>,
    name: String,
    src_path: PathBuf,
}

pub(super) fn compiler_artifact_for_test(
    bytes: &[u8],
    expected: &CargoTargetKey,
    workspace: &RecordedWorkspace,
    expected_target_dir: &Path,
    target_label: &str,
) -> Result<ParsedCompilerArtifact, AggregateError> {
    crate::verification::target::verify_protected_compiler_artifacts(
        bytes,
        workspace.producer(),
        workspace.active(),
    )
    .map_err(AggregateError::new)?;
    let mut artifacts = Vec::new();
    for line in String::from_utf8_lossy(bytes).lines() {
        let Ok(message) = serde_json::from_str::<CargoCompilerMessage>(line) else {
            continue;
        };
        if message.reason != "compiler-artifact" {
            continue;
        }
        let Some(target) = message.target else {
            continue;
        };
        if target.name != expected.target || target.kind != [expected.kind.as_str()] {
            continue;
        }
        if message.fresh == Some(true) {
            return Err(AggregateError::new(format!(
                "fresh cached executable is forbidden for {target_label}"
            )));
        }
        let package_id = message.package_id.ok_or_else(|| {
            AggregateError::new(format!(
                "compiler-artifact for {target_label} omitted Cargo package_id"
            ))
        })?;
        verify_cargo_package_identity(&package_id, &target.src_path, &expected.package, workspace)?;
        let executable = message.executable.ok_or_else(|| {
            AggregateError::new(format!(
                "compiler-artifact for {target_label} omitted its executable"
            ))
        })?;
        verify_emitted_test_path(
            &executable,
            expected_target_dir,
            &expected.target,
            target_label,
        )?;
        artifacts.push(ParsedCompilerArtifact {
            package_id,
            executable,
        });
    }
    let [artifact] = artifacts.as_slice() else {
        return Err(AggregateError::new(format!(
            "compile log does not preserve exactly one package-bound executable for {target_label}; found {}",
            artifacts.len()
        )));
    };
    Ok(ParsedCompilerArtifact {
        package_id: artifact.package_id.clone(),
        executable: artifact.executable.clone(),
    })
}

fn verify_cargo_package_identity(
    package_id: &str,
    src_path: &Path,
    expected_package: &str,
    workspace: &RecordedWorkspace,
) -> Result<(), AggregateError> {
    let expected_package_dir =
        workspace.producer_path(Path::new("crates").join(expected_package).as_path())?;
    let encoded = package_id.strip_prefix("path+file://").ok_or_else(|| {
        AggregateError::new(format!(
            "compiler-artifact package_id for {expected_package} is not a workspace path package"
        ))
    })?;
    let (package_path, version) = encoded.rsplit_once('#').ok_or_else(|| {
        AggregateError::new(format!(
            "compiler-artifact package_id for {expected_package} has no version"
        ))
    })?;
    if version.is_empty() {
        return Err(AggregateError::new(format!(
            "compiler-artifact package_id for {expected_package} has an empty version"
        )));
    }
    let observed_package_dir = Path::new(package_path);
    if observed_package_dir != expected_package_dir || !src_path.starts_with(&expected_package_dir)
    {
        return Err(AggregateError::new(format!(
            "compiler-artifact package_id or source path does not match workspace package {expected_package}"
        )));
    }
    workspace.verify_active_directory(
        observed_package_dir,
        &format!("compiler-artifact package {expected_package}"),
    )?;
    workspace.verify_active_file(
        src_path,
        &format!("compiler-artifact source for {expected_package}"),
    )?;
    Ok(())
}

fn verify_emitted_test_path(
    executable: &Path,
    expected_target_dir: &Path,
    expected_target: &str,
    target_label: &str,
) -> Result<(), AggregateError> {
    if !executable.is_absolute()
        || executable
            .components()
            .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
        || !executable.starts_with(expected_target_dir)
    {
        return Err(AggregateError::new(format!(
            "Cargo emitted a non-canonical or cross-build executable for {target_label}"
        )));
    }
    let expected_prefix = expected_target.replace('-', "_");
    let file_name = executable
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            AggregateError::new(format!(
                "Cargo emitted a non-UTF-8 executable name for {target_label}"
            ))
        })?;
    if file_name != expected_prefix
        && !file_name
            .strip_prefix(&expected_prefix)
            .is_some_and(|suffix| suffix.starts_with('-'))
    {
        return Err(AggregateError::new(format!(
            "Cargo executable name {file_name} does not match target {expected_target}"
        )));
    }
    Ok(())
}
