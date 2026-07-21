//! Extracted Cargo manifest identity validation.

use std::path::Path;

use crate::execution::filesystem::HeldDirectory;

use super::super::lock::LockedPackage;

pub(super) fn require_identity(
    root: &HeldDirectory,
    vendor_root: &Path,
    package: &LockedPackage,
) -> Result<(), String> {
    let manifest_path = vendor_root.join("Cargo.toml");
    let manifest = root.read(&manifest_path).map_err(|error| {
        format!(
            "read registry manifest {}: {error}",
            manifest_path.display()
        )
    })?;
    let manifest: toml::Value = std::str::from_utf8(&manifest)
        .map_err(|_| "registry Cargo.toml is not UTF-8".to_owned())?
        .parse()
        .map_err(|error| format!("parse registry Cargo.toml: {error}"))?;
    let table = manifest
        .get("package")
        .and_then(toml::Value::as_table)
        .ok_or_else(|| "registry Cargo.toml omitted [package]".to_owned())?;
    if table.get("name").and_then(toml::Value::as_str) != Some(&package.name)
        || table.get("version").and_then(toml::Value::as_str) != Some(&package.version)
    {
        return Err(format!(
            "registry manifest identity does not match {} {}",
            package.name, package.version
        ));
    }
    Ok(())
}
