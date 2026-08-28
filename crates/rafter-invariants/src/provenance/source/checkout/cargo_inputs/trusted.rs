//! Canonical resolution checks for the trusted in-tree Cargo packages.
//!
//! Each trusted crate must resolve to exactly one workspace path package with
//! its reviewed manifest, library source, and dependency edge shape, so a
//! lookalike package cannot supply the detector vocabulary.

use std::{error::Error, fs, path::Path};

use super::TrustedCargoPackage;

pub(super) fn validate_trusted_cargo_package(
    root: &Path,
    packages: &[serde_json::Value],
    trusted: TrustedCargoPackage,
) -> Result<(), Box<dyn Error>> {
    let expected_root = root.join(trusted.relative_root);
    if fs::canonicalize(&expected_root)? != expected_root {
        return Err(format!(
            "trusted Cargo package {} traverses a filesystem alias or symlink",
            trusted.package
        )
        .into());
    }
    let expected_manifest = expected_root.join("Cargo.toml");
    let named = packages
        .iter()
        .filter(|package| {
            package.get("name").and_then(serde_json::Value::as_str) == Some(trusted.package)
        })
        .collect::<Vec<_>>();
    let [package] = named.as_slice() else {
        return Err(format!(
            "trusted Cargo package {} resolves to {} workspace packages",
            trusted.package,
            named.len()
        )
        .into());
    };
    if !package
        .get("source")
        .is_some_and(serde_json::Value::is_null)
    {
        return Err(format!(
            "trusted Cargo package {} is not an in-tree path package",
            trusted.package
        )
        .into());
    }
    let manifest = package
        .get("manifest_path")
        .and_then(serde_json::Value::as_str)
        .ok_or("trusted Cargo package omitted its manifest_path")?;
    if Path::new(manifest) != expected_manifest || fs::canonicalize(manifest)? != expected_manifest
    {
        return Err(format!(
            "trusted Cargo package {} does not use its canonical workspace manifest",
            trusted.package
        )
        .into());
    }
    let targets = package
        .get("targets")
        .and_then(serde_json::Value::as_array)
        .ok_or("trusted Cargo package omitted its target inventory")?;
    let targets = targets
        .iter()
        .filter(|target| {
            target.get("name").and_then(serde_json::Value::as_str) == Some(trusted.rust_crate)
                && target
                    .get("kind")
                    .and_then(serde_json::Value::as_array)
                    .is_some_and(|kinds| {
                        kinds
                            .iter()
                            .filter_map(serde_json::Value::as_str)
                            .eq([trusted.target_kind])
                    })
        })
        .collect::<Vec<_>>();
    let [target] = targets.as_slice() else {
        return Err(format!(
            "trusted Cargo package {} resolves to {} canonical targets",
            trusted.package,
            targets.len()
        )
        .into());
    };
    let source = target
        .get("src_path")
        .and_then(serde_json::Value::as_str)
        .ok_or("trusted Cargo target omitted its src_path")?;
    let expected_source = expected_root.join("src/lib.rs");
    if Path::new(source) != expected_source || fs::canonicalize(source)? != expected_source {
        return Err(format!(
            "trusted Cargo package {} does not use its canonical library source",
            trusted.package
        )
        .into());
    }
    Ok(())
}

pub(super) fn validate_trusted_cargo_dependency(
    root: &Path,
    dependency: &serde_json::Value,
    trusted: TrustedCargoPackage,
) -> Result<(), Box<dyn Error>> {
    let package = dependency
        .get("name")
        .and_then(serde_json::Value::as_str)
        .ok_or("trusted Cargo dependency omitted its package name")?;
    let source_is_path = dependency
        .get("source")
        .is_some_and(serde_json::Value::is_null);
    let path = dependency
        .get("path")
        .and_then(serde_json::Value::as_str)
        .ok_or("trusted Cargo dependency omitted its path")?;
    let optional = dependency
        .get("optional")
        .and_then(serde_json::Value::as_bool)
        .ok_or("trusted Cargo dependency omitted its optional flag")?;
    let target_is_unconditional = dependency
        .get("target")
        .is_some_and(serde_json::Value::is_null);
    let kind = match dependency.get("kind") {
        Some(value) if value.is_null() => None,
        Some(value) => Some(
            value
                .as_str()
                .ok_or("trusted Cargo dependency kind is not a string")?,
        ),
        None => return Err("trusted Cargo dependency omitted its kind".into()),
    };
    let expected_root = root.join(trusted.relative_root);
    if package != trusted.package
        || !source_is_path
        || Path::new(path) != expected_root
        || fs::canonicalize(path)? != expected_root
        || optional
        || !target_is_unconditional
        || kind != trusted.dependency_kind
    {
        return Err(format!(
            "Cargo dependency exposed as {} does not resolve to canonical package {}",
            trusted.rust_crate, trusted.package
        )
        .into());
    }
    Ok(())
}
