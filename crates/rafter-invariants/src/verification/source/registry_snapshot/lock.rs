//! Fail-closed parsing of registry packages from the authenticated lockfile.

use std::{collections::BTreeSet, fs, path::Path};

use sha2::{Digest, Sha256};

const CRATES_IO_SOURCE: &str = "registry+https://github.com/rust-lang/crates.io-index";

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct LockedPackage {
    pub(super) name: String,
    pub(super) version: String,
    pub(super) source: String,
    pub(super) checksum: String,
}

impl LockedPackage {
    pub(super) fn archive_name(&self) -> String {
        format!("{}-{}.crate", self.name, self.version)
    }

    pub(super) fn package_root(&self) -> String {
        format!("{}-{}", self.name, self.version)
    }
}

pub(super) struct LockedRegistry {
    pub(super) lock_sha256: String,
    pub(super) packages: Vec<LockedPackage>,
}

pub(super) fn parse(path: &Path) -> Result<LockedRegistry, String> {
    let bytes =
        fs::read(path).map_err(|error| format!("read authenticated registry lockfile: {error}"))?;
    let lock: toml::Value = std::str::from_utf8(&bytes)
        .map_err(|_| "authenticated Cargo.lock is not UTF-8".to_owned())?
        .parse()
        .map_err(|error| format!("parse authenticated Cargo.lock: {error}"))?;
    if lock.get("version").and_then(toml::Value::as_integer) != Some(4) {
        return Err("authenticated Cargo.lock must use format version 4".to_owned());
    }
    let entries = lock
        .get("package")
        .and_then(toml::Value::as_array)
        .ok_or_else(|| "authenticated Cargo.lock omitted package inventory".to_owned())?;
    let mut identities = BTreeSet::new();
    let mut packages = Vec::new();
    for entry in entries {
        let Some(source) = entry.get("source").and_then(toml::Value::as_str) else {
            continue;
        };
        if source != CRATES_IO_SOURCE {
            return Err(format!(
                "unsupported external Cargo source in lockfile: {source}"
            ));
        }
        let name = field(entry, "name")?;
        let version = field(entry, "version")?;
        let checksum = field(entry, "checksum")?;
        validate_component(name, "package name")?;
        validate_component(version, "package version")?;
        if checksum.len() != 64
            || checksum
                .bytes()
                .any(|byte| !byte.is_ascii_hexdigit() || byte.is_ascii_uppercase())
        {
            return Err(format!(
                "registry package {name} {version} has an invalid checksum"
            ));
        }
        if !identities.insert((name.to_owned(), version.to_owned())) {
            return Err(format!(
                "duplicate registry package identity {name} {version}"
            ));
        }
        packages.push(LockedPackage {
            name: name.to_owned(),
            version: version.to_owned(),
            source: source.to_owned(),
            checksum: checksum.to_owned(),
        });
    }
    if packages.is_empty() {
        return Err("authenticated Cargo.lock contains no registry packages".to_owned());
    }
    packages.sort_by(|left, right| {
        (&left.name, &left.version, &left.source).cmp(&(&right.name, &right.version, &right.source))
    });
    Ok(LockedRegistry {
        lock_sha256: format!("{:x}", Sha256::digest(bytes)),
        packages,
    })
}

fn field<'a>(entry: &'a toml::Value, name: &str) -> Result<&'a str, String> {
    entry
        .get(name)
        .and_then(toml::Value::as_str)
        .ok_or_else(|| format!("registry lock entry omitted string field {name}"))
}

fn validate_component(value: &str, label: &str) -> Result<(), String> {
    if value.is_empty()
        || value.len() > 128
        || value.bytes().any(|byte| {
            !(byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'+'))
        })
    {
        return Err(format!(
            "registry {label} is not a safe path component: {value:?}"
        ));
    }
    Ok(())
}
