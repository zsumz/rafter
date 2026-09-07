//! Tracked-source validation of the resolved Cargo package graph.
//!
//! Every path package, path dependency, and target source Cargo resolves must
//! be a tracked file, and the workspace manifest must not redirect resolution
//! through `[patch]` or `[replace]`.

use std::{
    collections::HashSet,
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use super::path_validation::validate_tracked_source_path;

pub(super) fn validate_resolved_path_package_metadata(
    root: &Path,
    metadata: &str,
    tracked: &HashSet<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    let root = fs::canonicalize(root)?;
    let metadata: serde_json::Value = serde_json::from_str(metadata)?;
    let workspace_root = metadata
        .get("workspace_root")
        .and_then(serde_json::Value::as_str)
        .ok_or("cargo metadata omitted its workspace_root")?;
    if fs::canonicalize(workspace_root)? != root {
        return Err("cargo metadata resolved a different workspace root".into());
    }
    let packages = metadata
        .get("packages")
        .and_then(serde_json::Value::as_array)
        .ok_or("cargo metadata omitted its package inventory")?;
    for package in packages {
        match package.get("source") {
            Some(source) if source.is_null() => {}
            Some(_) => continue,
            None => return Err("cargo metadata package omitted its source field".into()),
        }
        let manifest = package
            .get("manifest_path")
            .and_then(serde_json::Value::as_str)
            .ok_or("path package omitted its manifest_path")?;
        validate_tracked_source_path(&root, Path::new(manifest), tracked, "package manifest")?;

        let dependencies = package
            .get("dependencies")
            .and_then(serde_json::Value::as_array)
            .ok_or("cargo metadata package omitted its dependency inventory")?;
        for dependency in dependencies {
            let Some(path) = dependency.get("path").and_then(serde_json::Value::as_str) else {
                continue;
            };
            validate_tracked_source_path(
                &root,
                &Path::new(path).join("Cargo.toml"),
                tracked,
                "dependency manifest",
            )?;
        }

        let targets = package
            .get("targets")
            .and_then(serde_json::Value::as_array)
            .ok_or("cargo metadata package omitted its target inventory")?;
        for target in targets {
            if target
                .get("kind")
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(serde_json::Value::as_str)
                .any(|kind| kind == "custom-build")
            {
                return Err(
                    "Cargo custom build targets are outside the source binding contract".into(),
                );
            }
            let source = target
                .get("src_path")
                .and_then(serde_json::Value::as_str)
                .ok_or("cargo metadata target omitted its src_path")?;
            validate_tracked_source_path(&root, Path::new(source), tracked, "target source")?;
        }
    }
    Ok(())
}

pub(super) fn validate_manifest_path_overrides(
    root: &Path,
    tracked: &HashSet<PathBuf>,
) -> Result<(), Box<dyn Error>> {
    let root = fs::canonicalize(root)?;
    let manifest_path = root.join("Cargo.toml");
    validate_tracked_source_path(&root, &manifest_path, tracked, "workspace manifest")?;
    let manifest: toml::Value = fs::read_to_string(&manifest_path)?.parse()?;
    for section in ["patch", "replace"] {
        if manifest.get(section).is_some() {
            return Err(format!(
                "Cargo manifest [{section}] overrides are outside the source binding contract"
            )
            .into());
        }
    }
    Ok(())
}
