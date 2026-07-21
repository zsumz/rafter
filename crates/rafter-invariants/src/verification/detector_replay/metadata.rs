//! Strict package and source-path authority derived from private Cargo metadata.

use std::{collections::BTreeMap, collections::BTreeSet, fs, path::Path, path::PathBuf};

use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::verification::source::AuthenticatedCompilationSource;

const CRATES_IO_SOURCE: &str = "registry+https://github.com/rust-lang/crates.io-index";

#[derive(Debug)]
pub(super) struct CompilationGraph {
    packages: BTreeMap<String, PackageAuthority>,
    pub(super) sha256: String,
}

#[derive(Debug)]
struct PackageAuthority {
    name: String,
    root: PathBuf,
    targets: BTreeSet<TargetAuthority>,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct TargetAuthority {
    name: String,
    kind: Vec<String>,
    source: PathBuf,
}

#[derive(Deserialize)]
struct CargoMetadata {
    packages: Vec<CargoPackage>,
    workspace_root: PathBuf,
}

#[derive(Deserialize)]
struct CargoPackage {
    name: String,
    version: String,
    id: String,
    source: Option<String>,
    manifest_path: PathBuf,
    targets: Vec<CargoTarget>,
}

#[derive(Deserialize)]
struct CargoTarget {
    name: String,
    kind: Vec<String>,
    src_path: PathBuf,
}

impl CompilationGraph {
    pub(super) fn parse(
        bytes: &[u8],
        source: &AuthenticatedCompilationSource<'_>,
    ) -> Result<Self, String> {
        let workspace = canonical(source.workspace(), "authenticated workspace")?;
        let vendor = canonical(&source.vendor_root(), "authenticated registry vendor")?;
        Self::parse_with_authority(bytes, &workspace, &vendor, source.registry_package_count())
    }

    fn parse_with_authority(
        bytes: &[u8],
        workspace: &Path,
        vendor: &Path,
        required_registry_packages: usize,
    ) -> Result<Self, String> {
        let metadata: CargoMetadata = serde_json::from_slice(bytes)
            .map_err(|error| format!("parse private Cargo metadata: {error}"))?;
        if canonical(&metadata.workspace_root, "Cargo metadata workspace root")? != workspace {
            return Err("Cargo metadata resolved outside the authenticated workspace".to_owned());
        }
        let mut packages = BTreeMap::new();
        let mut registry_packages = 0_usize;
        for package in metadata.packages {
            let expected_root = match package.source.as_deref() {
                None => path_package_root(workspace, &package)?,
                Some(CRATES_IO_SOURCE) => {
                    registry_packages += 1;
                    vendor.join(format!("{}-{}", package.name, package.version))
                }
                Some(source) => {
                    return Err(format!(
                        "Cargo metadata uses unsupported package source {source}"
                    ));
                }
            };
            let observed_manifest = canonical(&package.manifest_path, "Cargo package manifest")?;
            if observed_manifest != expected_root.join("Cargo.toml") {
                return Err(format!(
                    "Cargo metadata package {} uses an unauthenticated manifest",
                    package.name
                ));
            }
            let mut targets = BTreeSet::new();
            for target in package.targets {
                let source = canonical(&target.src_path, "Cargo target source")?;
                if !source.starts_with(&expected_root) {
                    return Err(format!(
                        "Cargo metadata target for {} escapes its authenticated package",
                        package.name
                    ));
                }
                if target.name.is_empty() || target.kind.is_empty() {
                    return Err(format!(
                        "Cargo metadata target for {} omitted its name or kind",
                        package.name
                    ));
                }
                if !targets.insert(TargetAuthority {
                    name: target.name,
                    kind: target.kind,
                    source,
                }) {
                    return Err(format!(
                        "Cargo metadata package {} contains a duplicate target",
                        package.name
                    ));
                }
            }
            if packages
                .insert(
                    package.id,
                    PackageAuthority {
                        name: package.name,
                        root: expected_root,
                        targets,
                    },
                )
                .is_some()
            {
                return Err("Cargo metadata contains a duplicate package ID".to_owned());
            }
        }
        if registry_packages != required_registry_packages {
            return Err(format!(
                "Cargo metadata resolved {registry_packages} registry packages, authenticated lock contains {required_registry_packages}"
            ));
        }
        Ok(Self {
            packages,
            sha256: format!("{:x}", Sha256::digest(bytes)),
        })
    }

    pub(super) fn verify_target(
        &self,
        package_id: &str,
        name: &str,
        kind: &[String],
        source: &Path,
    ) -> Result<(), String> {
        let package = self.packages.get(package_id).ok_or_else(|| {
            format!("Cargo compiler message uses unresolved package ID {package_id}")
        })?;
        let source = canonical(source, "Cargo compiler target source")?;
        if !source.starts_with(&package.root) {
            return Err(format!(
                "Cargo compiler target for {} escapes its authenticated package",
                package.name
            ));
        }
        let target = TargetAuthority {
            name: name.to_owned(),
            kind: kind.to_vec(),
            source,
        };
        if !package.targets.contains(&target) {
            return Err(format!(
                "Cargo compiler target for {} does not match authenticated metadata",
                package.name
            ));
        }
        Ok(())
    }

    pub(super) fn package_name(&self, package_id: &str) -> Result<&str, String> {
        self.packages
            .get(package_id)
            .map(|package| package.name.as_str())
            .ok_or_else(|| {
                format!("Cargo compiler message uses unresolved package ID {package_id}")
            })
    }

    pub(super) fn contains(&self, package_id: &str) -> bool {
        self.packages.contains_key(package_id)
    }

    #[cfg(test)]
    pub(super) fn fixture(package_id: &str, name: &str, root: PathBuf) -> Self {
        let source = root.join("src/lib.rs");
        Self {
            packages: BTreeMap::from([(
                package_id.to_owned(),
                PackageAuthority {
                    name: name.to_owned(),
                    root,
                    targets: BTreeSet::from([TargetAuthority {
                        name: name.to_owned(),
                        kind: vec!["lib".to_owned()],
                        source,
                    }]),
                },
            )]),
            sha256: "0".repeat(64),
        }
    }

    #[cfg(test)]
    pub(super) fn parse_fixture(
        bytes: &[u8],
        workspace: &Path,
        vendor: &Path,
        required_registry_packages: usize,
    ) -> Result<Self, String> {
        Self::parse_with_authority(bytes, workspace, vendor, required_registry_packages)
    }
}

fn path_package_root(workspace: &Path, package: &CargoPackage) -> Result<PathBuf, String> {
    let manifest = canonical(&package.manifest_path, "workspace package manifest")?;
    let root = manifest
        .parent()
        .ok_or_else(|| "workspace package manifest has no parent".to_owned())?
        .to_owned();
    if !root.starts_with(workspace) {
        return Err(format!(
            "Cargo metadata path package {} escapes the authenticated workspace",
            package.name
        ));
    }
    Ok(root)
}

fn canonical(path: &Path, label: &str) -> Result<PathBuf, String> {
    fs::canonicalize(path)
        .map_err(|error| format!("canonicalize {label} {}: {error}", path.display()))
}

#[cfg(test)]
#[path = "metadata/tests.rs"]
mod tests;
