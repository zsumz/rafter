//! Cargo workspace package and registered target resolution.

use std::{
    fs,
    path::{Path, PathBuf},
};

pub(super) struct CargoTarget {
    pub(super) crate_name: String,
    pub(super) path: PathBuf,
}

pub(super) fn resolve_registered_target(
    workspace: &Path,
    package_name: &str,
    target_kind: &str,
    target_name: &str,
) -> Result<CargoTarget, String> {
    let package = package_manifest(workspace, package_name)?;
    let manifest_source = fs::read_to_string(&package.manifest)
        .map_err(|error| format!("read {}: {error}", package.manifest.display()))?;
    let manifest = manifest_source
        .parse::<toml::Value>()
        .map_err(|error| format!("parse {}: {error}", package.manifest.display()))?;
    target_root(&package.root, &manifest, target_kind, target_name)
}

struct PackageManifest {
    root: PathBuf,
    manifest: PathBuf,
}

fn package_manifest(workspace: &Path, package: &str) -> Result<PackageManifest, String> {
    let workspace_manifest = workspace.join("Cargo.toml");
    let source = fs::read_to_string(&workspace_manifest)
        .map_err(|error| format!("read {}: {error}", workspace_manifest.display()))?;
    let manifest = source
        .parse::<toml::Value>()
        .map_err(|error| format!("parse {}: {error}", workspace_manifest.display()))?;
    let mut candidates = vec![workspace.to_owned()];
    if let Some(members) = manifest
        .get("workspace")
        .and_then(|workspace| workspace.get("members"))
        .and_then(toml::Value::as_array)
    {
        for member in members {
            let member = member.as_str().ok_or("workspace member must be a string")?;
            if member.contains(['*', '?', '[', ']']) {
                return Err(format!(
                    "workspace member glob is unsupported for detector identity resolution: {member}"
                ));
            }
            candidates.push(workspace.join(member));
        }
    }
    let mut matches = Vec::new();
    for root in candidates {
        let candidate = root.join("Cargo.toml");
        let Ok(source) = fs::read_to_string(&candidate) else {
            continue;
        };
        let Ok(value) = source.parse::<toml::Value>() else {
            continue;
        };
        if value
            .get("package")
            .and_then(|package| package.get("name"))
            .and_then(toml::Value::as_str)
            == Some(package)
        {
            matches.push(PackageManifest {
                root,
                manifest: candidate,
            });
        }
    }
    let [found] = matches.as_slice() else {
        return Err(format!(
            "registered package {package} resolves to {} workspace manifests",
            matches.len()
        ));
    };
    Ok(PackageManifest {
        root: found.root.clone(),
        manifest: found.manifest.clone(),
    })
}

fn target_root(
    package: &Path,
    manifest: &toml::Value,
    target_kind: &str,
    target_name: &str,
) -> Result<CargoTarget, String> {
    let package_name = manifest
        .get("package")
        .and_then(|package| package.get("name"))
        .and_then(toml::Value::as_str)
        .ok_or("package manifest omits package.name")?;
    let normalized_package = package_name.replace('-', "_");
    let (crate_name, relative) = match target_kind {
        "lib" => {
            let table = manifest.get("lib").and_then(toml::Value::as_table);
            let name = table
                .and_then(|table| table.get("name"))
                .and_then(toml::Value::as_str)
                .unwrap_or(&normalized_package)
                .to_owned();
            let path = table
                .and_then(|table| table.get("path"))
                .and_then(toml::Value::as_str)
                .unwrap_or("src/lib.rs");
            (name, PathBuf::from(path))
        }
        "bin" | "test" => {
            let table_name = if target_kind == "bin" { "bin" } else { "test" };
            let configured = manifest
                .get(table_name)
                .and_then(toml::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(toml::Value::as_table)
                .find(|table| table.get("name").and_then(toml::Value::as_str) == Some(target_name));
            let default = if target_kind == "bin" {
                if target_name == package_name {
                    PathBuf::from("src/main.rs")
                } else {
                    PathBuf::from("src/bin").join(format!("{target_name}.rs"))
                }
            } else {
                PathBuf::from("tests").join(format!("{target_name}.rs"))
            };
            let path = configured
                .and_then(|table| table.get("path"))
                .and_then(toml::Value::as_str)
                .map_or(default, PathBuf::from);
            (target_name.replace('-', "_"), path)
        }
        kind => return Err(format!("unsupported registered target kind {kind}")),
    };
    if crate_name != target_name.replace('-', "_") {
        return Err(format!(
            "registered target {target_name} disagrees with manifest crate name {crate_name}"
        ));
    }
    let path = package.join(relative);
    if !path.is_file() {
        return Err(format!(
            "registered target root does not exist: {}",
            path.display()
        ));
    }
    Ok(CargoTarget { crate_name, path })
}
