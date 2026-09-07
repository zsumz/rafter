//! Cargo configuration include resolution and file discovery.
//!
//! Parses the reviewed `include` array shape and locates the `config` or
//! `config.toml` a directory contributes, refusing anything that is present
//! but not a regular file.

use std::{
    error::Error,
    fs,
    path::{Path, PathBuf},
};

use super::CargoConfigInclude;

pub(super) fn cargo_config_includes(
    configuration: &toml::Value,
) -> Result<Vec<CargoConfigInclude>, Box<dyn Error>> {
    let Some(include) = configuration.get("include") else {
        return Ok(Vec::new());
    };
    let entries = include
        .as_array()
        .ok_or("Cargo configuration include must be an array")?;
    entries
        .iter()
        .map(|entry| {
            let (path, optional) = if let Some(path) = entry.as_str() {
                (path, false)
            } else {
                let table = entry
                    .as_table()
                    .ok_or("Cargo configuration include entry must be a path or table")?;
                if table.keys().any(|key| key != "path" && key != "optional") {
                    return Err("Cargo configuration include table has an unknown field".into());
                }
                let path = table
                    .get("path")
                    .and_then(toml::Value::as_str)
                    .ok_or("Cargo configuration include table requires a string path")?;
                let optional = table
                    .get("optional")
                    .map(|value| {
                        value
                            .as_bool()
                            .ok_or("Cargo configuration include optional must be a boolean")
                    })
                    .transpose()?
                    .unwrap_or(false);
                (path, optional)
            };
            if Path::new(path)
                .extension()
                .and_then(std::ffi::OsStr::to_str)
                != Some("toml")
            {
                return Err(
                    format!("Cargo configuration include path must end in .toml: {path}").into(),
                );
            }
            Ok(CargoConfigInclude {
                path: PathBuf::from(path),
                optional,
            })
        })
        .collect()
}

pub(super) fn cargo_config_in(directory: &Path) -> Result<Option<PathBuf>, Box<dyn Error>> {
    let config = directory.join("config");
    if cargo_config_file_exists(&config)? {
        return Ok(Some(config));
    }
    let config_toml = directory.join("config.toml");
    if cargo_config_file_exists(&config_toml)? {
        Ok(Some(config_toml))
    } else {
        Ok(None)
    }
}

fn cargo_config_file_exists(path: &Path) -> Result<bool, Box<dyn Error>> {
    match fs::metadata(path) {
        Ok(metadata) if metadata.is_file() => Ok(true),
        Ok(_) => Err(format!("Cargo configuration path is not a file: {}", path.display()).into()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}
