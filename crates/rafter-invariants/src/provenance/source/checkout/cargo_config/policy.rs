//! Reviewed Cargo configuration settings a source receipt cannot bind.
//!
//! A configuration key that names an executable or a filesystem input outside
//! the recorded tree is rejected here rather than hashed, so the receipt never
//! records a bind it cannot honour.

use std::{error::Error, path::Path};

pub(super) fn validate_bound_cargo_config(
    configuration: &toml::Value,
    path: &Path,
) -> Result<(), Box<dyn Error>> {
    // These settings can name executables or filesystem inputs whose bytes are
    // outside the source receipt. Reject them instead of recording a false bind.
    let table = configuration.as_table().ok_or_else(|| {
        format!(
            "Cargo configuration root must be a table: {}",
            path.display()
        )
    })?;
    for key in [
        "paths",
        "path-bases",
        "patch",
        "source",
        "env",
        "resolver",
        "unstable",
    ] {
        if table.contains_key(key) {
            return unbound_cargo_config_error(path, key);
        }
    }
    if let Some(target) = table.get("target") {
        if let Some(setting) = nested_unbound_target_setting(target, "target") {
            return unbound_cargo_config_error(path, &setting);
        }
        return unbound_cargo_config_error(path, "target");
    }
    let Some(build) = table.get("build") else {
        return Ok(());
    };
    let build = build.as_table().ok_or_else(|| {
        format!(
            "Cargo configuration build setting must be a table: {}",
            path.display()
        )
    })?;
    for key in [
        "rustc",
        "rustc-wrapper",
        "rustc-workspace-wrapper",
        "rustdoc",
        "target",
        "target-dir",
        "rustflags",
        "rustdocflags",
    ] {
        if build.contains_key(key) {
            return unbound_cargo_config_error(path, &format!("build.{key}"));
        }
    }
    Ok(())
}

fn nested_unbound_target_setting(value: &toml::Value, prefix: &str) -> Option<String> {
    let table = value.as_table()?;
    for (key, value) in table {
        let setting = format!("{prefix}.{key}");
        if matches!(
            key.as_str(),
            "linker" | "runner" | "rustflags" | "rustdocflags"
        ) {
            return Some(setting);
        }
        if let Some(setting) = nested_unbound_target_setting(value, &setting) {
            return Some(setting);
        }
    }
    None
}

fn unbound_cargo_config_error<T>(path: &Path, setting: &str) -> Result<T, Box<dyn Error>> {
    Err(format!(
        "Cargo configuration {} uses unbound build input setting {setting}",
        path.display()
    )
    .into())
}
