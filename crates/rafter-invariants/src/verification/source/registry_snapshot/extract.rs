//! Strict extraction of authenticated `.crate` archives into a Cargo directory source.

use std::{
    collections::{BTreeMap, BTreeSet},
    io::Cursor,
    path::{Path, PathBuf},
};

mod archive_path;
mod entry;
mod manifest;
mod model;

use flate2::read::GzDecoder;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::execution::filesystem::HeldDirectory;

use super::{lock::LockedPackage, permissions, FilePlan};

pub(super) use archive_path::{package_relative, require_unique_path};
pub(super) use model::{ExtractedPackage, ExtractionBudget};

const MAX_EXPANDED_PACKAGE_BYTES: u64 = 128 * 1024 * 1024;
const MAX_PACKAGE_ENTRIES: u64 = 16 * 1024;
const MAX_FILE_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Serialize)]
struct CargoChecksum<'a> {
    files: &'a BTreeMap<String, String>,
    package: &'a str,
}

struct ExtractionState {
    package_root: String,
    vendor_root: PathBuf,
    seen: BTreeSet<PathBuf>,
    materialized_directories: BTreeSet<PathBuf>,
    folded: BTreeMap<String, PathBuf>,
    plans: BTreeMap<PathBuf, FilePlan>,
    cargo_files: BTreeMap<String, String>,
    expanded_bytes: u64,
    archive_entries: u64,
    entries: u64,
    budget: ExtractionBudget,
}

pub(super) fn package(
    root: &HeldDirectory,
    package: &LockedPackage,
    archive: &[u8],
    budget: ExtractionBudget,
) -> Result<ExtractedPackage, String> {
    let mut extraction = ExtractionState::new(root, package, budget)?;
    let mut tar = tar::Archive::new(GzDecoder::new(Cursor::new(archive)));
    let entries = tar
        .entries()
        .map_err(|error| format!("read registry tar entries: {error}"))?;
    for entry in entries {
        let mut entry = entry.map_err(|error| format!("read registry tar entry: {error}"))?;
        extraction.accept(root, &mut entry)?;
    }
    extraction.finish(root, package)
}

impl ExtractionState {
    fn new(
        root: &HeldDirectory,
        package: &LockedPackage,
        budget: ExtractionBudget,
    ) -> Result<Self, String> {
        budget.check()?;
        let package_root = package.package_root();
        let vendor_root = Path::new("vendor").join(&package_root);
        root.create_dir_all(&vendor_root)
            .map_err(|error| format!("create registry package root: {error}"))?;
        let state = Self {
            package_root,
            vendor_root,
            seen: BTreeSet::new(),
            materialized_directories: BTreeSet::new(),
            folded: BTreeMap::new(),
            plans: BTreeMap::new(),
            cargo_files: BTreeMap::new(),
            expanded_bytes: 0,
            archive_entries: 0,
            entries: 1,
            budget,
        };
        state.check_entry_limit()?;
        Ok(state)
    }

    fn finish(
        mut self,
        root: &HeldDirectory,
        package: &LockedPackage,
    ) -> Result<ExtractedPackage, String> {
        manifest::require_identity(root, &self.vendor_root, package)?;
        self.publish_checksum(root, package)?;
        Ok(ExtractedPackage {
            plans: self.plans,
            expanded_bytes: self.expanded_bytes,
            entries: self.entries,
        })
    }

    fn publish_checksum(
        &mut self,
        root: &HeldDirectory,
        package: &LockedPackage,
    ) -> Result<(), String> {
        self.budget.check()?;
        let checksum_path = self.vendor_root.join(".cargo-checksum.json");
        let checksum = serde_json::to_vec(&CargoChecksum {
            files: &self.cargo_files,
            package: &package.checksum,
        })
        .map_err(|error| format!("serialize Cargo directory checksum: {error}"))?;
        self.reserve_generated_file(checksum.len())?;
        self.budget.check()?;
        root.write_atomic(&checksum_path, &checksum)
            .map_err(|error| format!("write Cargo directory checksum: {error}"))?;
        self.budget.check()?;
        permissions::harden_file(&root.external_path().join(&checksum_path), false)
            .map_err(|error| format!("harden Cargo directory checksum: {error}"))?;
        self.plans.insert(
            checksum_path,
            FilePlan {
                digest: Sha256::digest(&checksum).into(),
                #[cfg(unix)]
                executable: false,
            },
        );
        Ok(())
    }

    fn reserve_generated_file(&mut self, bytes: usize) -> Result<(), String> {
        self.reserve_entry()?;

        let bytes = u64::try_from(bytes).map_err(|_| "registry file length overflow")?;
        self.expanded_bytes = self
            .expanded_bytes
            .checked_add(bytes)
            .ok_or_else(|| "registry expanded byte count overflow".to_owned())?;
        if self.expanded_bytes > MAX_EXPANDED_PACKAGE_BYTES
            || self.expanded_bytes > self.budget.expanded_bytes
        {
            return Err(format!(
                "registry package {} exceeds its expanded size limit",
                self.package_root
            ));
        }
        Ok(())
    }

    fn reserve_parent_directories(&mut self, path: &Path) -> Result<(), String> {
        let mut parents = path.ancestors().skip(1).collect::<Vec<_>>();
        parents.reverse();
        for parent in parents {
            if parent.as_os_str().is_empty() {
                continue;
            }
            if self.materialized_directories.insert(parent.to_owned()) {
                self.reserve_entry()?;
            }
        }
        Ok(())
    }

    fn reserve_entry(&mut self) -> Result<(), String> {
        self.entries = self
            .entries
            .checked_add(1)
            .ok_or_else(|| "registry entry count overflow".to_owned())?;
        self.check_entry_limit()
    }

    fn check_entry_limit(&self) -> Result<(), String> {
        if self.entries > MAX_PACKAGE_ENTRIES || self.entries > self.budget.entries {
            return Err(format!(
                "registry package {} exceeds its entry limit",
                self.package_root
            ));
        }
        Ok(())
    }
}
