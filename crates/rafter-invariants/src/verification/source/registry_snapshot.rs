//! Authenticated Cargo directory source materialized from locked registry archives.

use std::{collections::BTreeMap, error::Error, fs, path::Path, time::Instant};

use sha2::{Digest, Sha256};

use crate::execution::filesystem::{HeldDirectory, OperationDeadline};

use super::{
    permissions,
    sealed::{self, FilePlan, SealedTree},
    RegistryMaterializationPolicy,
};

mod cache;
mod extract;
mod lock;
mod model;

use model::RegistryMaterialization;
pub(in crate::verification) use model::RegistryReceipt;

const REGISTRY_PARENT: &str = "target/rafter-invariants/verified-registry";

pub(super) struct RegistrySnapshot {
    _parent: HeldDirectory,
    _directory: tempfile::TempDir,
    vendor: HeldDirectory,
    integrity: SealedTree,
    receipt: RegistryReceipt,
}

impl RegistrySnapshot {
    pub(super) fn materialize(
        source_root: &Path,
        policy: RegistryMaterializationPolicy,
    ) -> Result<Self, Box<dyn Error>> {
        require_time(policy.deadline)?;
        let locked = lock::parse(&source_root.join("Cargo.lock"))?;
        if locked.packages.len() != policy.required_packages {
            return Err(format!(
                "authenticated lock contains {} registry packages instead of the required {}",
                locked.packages.len(),
                policy.required_packages
            )
            .into());
        }
        require_time(policy.deadline)?;
        let archives = cache::ArchiveInventory::discover(policy.deadline, policy.maximum_entries)?;
        let parent = HeldDirectory::create_all(Path::new(REGISTRY_PARENT))?;
        parent.verify_path_binding()?;
        let directory = tempfile::Builder::new()
            .prefix("rafter-registry-source-")
            .tempdir_in(parent.external_path())?;
        parent.verify_path_binding()?;
        let root = fs::canonicalize(directory.path())?;
        let root_handle = HeldDirectory::open(&root)?;
        root_handle.create_dir_all(Path::new("archives"))?;
        root_handle.create_dir_all(Path::new("vendor"))?;

        let materialized = materialize_packages(&locked, &archives, &root_handle, policy)?;
        let planned_nodes = sealed::planned_node_count(&materialized.plans)?;
        if planned_nodes != materialized.entries {
            permissions::restore_tree(&root);
            return Err(format!(
                "authenticated registry accounted for {} nodes but planned {planned_nodes}",
                materialized.entries
            )
            .into());
        }
        let audit_deadline = OperationDeadline::at(
            policy.deadline,
            "harden and seal authenticated registry source",
        );
        if let Err(error) =
            permissions::harden_directories_bounded(&root, audit_deadline, planned_nodes)
        {
            permissions::restore_tree(&root);
            return Err(error);
        }
        let integrity = match SealedTree::capture_bounded(
            "authenticated registry source",
            &root,
            materialized.plans,
            audit_deadline,
            planned_nodes,
        ) {
            Ok(integrity) => integrity,
            Err(error) => {
                permissions::restore_tree(&root);
                return Err(error.into());
            }
        };
        let vendor = HeldDirectory::open(&root.join("vendor"))?;
        Ok(Self {
            _parent: parent,
            _directory: directory,
            vendor,
            integrity,
            receipt: RegistryReceipt {
                lock_sha256: locked.lock_sha256,
                package_count: locked.packages.len(),
                archive_bytes: materialized.archive_bytes,
                expanded_bytes: materialized.expanded_bytes,
                entries: materialized.entries,
                materialization_sha256: materialized.materialization_sha256,
            },
        })
    }

    pub(super) fn vendor_root(&self) -> std::path::PathBuf {
        self.vendor.external_path()
    }

    pub(super) fn bind_vendor_for_child(
        &self,
    ) -> Result<crate::execution::filesystem::ChildDirectory, Box<dyn Error>> {
        self.vendor.bind_for_child()
    }

    pub(super) fn receipt(&self) -> &RegistryReceipt {
        &self.receipt
    }

    pub(super) fn revalidate(&self) -> Result<(), String> {
        self.vendor
            .verify_path_binding()
            .map_err(|error| format!("authenticated registry vendor binding changed: {error}"))?;
        self.integrity.revalidate()
    }
}

impl Drop for RegistrySnapshot {
    fn drop(&mut self) {
        permissions::restore_tree(self.integrity.root());
    }
}

fn materialize_packages(
    locked: &lock::LockedRegistry,
    archives: &cache::ArchiveInventory,
    root: &HeldDirectory,
    policy: RegistryMaterializationPolicy,
) -> Result<RegistryMaterialization, Box<dyn Error>> {
    let mut plans = BTreeMap::new();
    let mut materialization = Sha256::new();
    let mut archive_bytes = 0_u64;
    let mut expanded_bytes = 0_u64;
    let mut entries = 2_u64;
    if entries > policy.maximum_entries {
        return Err("authenticated registry entry budget cannot hold its root directories".into());
    }
    for package in &locked.packages {
        require_time(policy.deadline)?;
        let archive = archives.acquire(package, policy.deadline)?;
        entries = entries
            .checked_add(1)
            .ok_or("registry entry count overflow")?;
        if entries > policy.maximum_entries {
            return Err("authenticated registry exceeds its aggregate entry limit".into());
        }
        archive_bytes = archive_bytes
            .checked_add(u64::try_from(archive.bytes.len())?)
            .ok_or("registry archive byte count overflow")?;
        if archive_bytes > policy.maximum_archive_bytes {
            return Err("authenticated registry exceeds its aggregate archive byte limit".into());
        }
        let archive_path = Path::new("archives").join(package.archive_name());
        root.write_atomic(&archive_path, &archive.bytes)?;
        permissions::harden_file(&root.external_path().join(&archive_path), false)?;
        plans.insert(
            archive_path,
            FilePlan {
                digest: archive.digest,
                #[cfg(unix)]
                executable: false,
            },
        );
        let extracted = extract::package(
            root,
            package,
            &archive.bytes,
            extract::ExtractionBudget {
                expanded_bytes: policy
                    .maximum_expanded_bytes
                    .checked_sub(expanded_bytes)
                    .ok_or("registry expanded byte budget underflow")?,
                entries: policy
                    .maximum_entries
                    .checked_sub(entries)
                    .ok_or("registry entry budget underflow")?,
                deadline: policy.deadline,
            },
        )?;
        expanded_bytes = expanded_bytes
            .checked_add(extracted.expanded_bytes)
            .ok_or("registry expanded byte count overflow")?;
        entries = entries
            .checked_add(extracted.entries)
            .ok_or("registry entry count overflow")?;
        digest_frame(&mut materialization, package.name.as_bytes());
        digest_frame(&mut materialization, package.version.as_bytes());
        digest_frame(&mut materialization, package.checksum.as_bytes());
        for (path, plan) in extracted.plans {
            digest_frame(&mut materialization, path.to_string_lossy().as_bytes());
            digest_frame(&mut materialization, &plan.digest);
            if plans.insert(path, plan).is_some() {
                return Err("registry materialization produced a duplicate path".into());
            }
        }
    }
    Ok(RegistryMaterialization {
        plans,
        archive_bytes,
        expanded_bytes,
        entries,
        materialization_sha256: format!("{:x}", materialization.finalize()),
    })
}

fn require_time(deadline: Instant) -> Result<(), Box<dyn Error>> {
    if Instant::now() >= deadline {
        return Err("authenticated registry materialization deadline expired".into());
    }
    Ok(())
}

fn digest_frame(digest: &mut Sha256, bytes: &[u8]) {
    digest.update((bytes.len() as u64).to_be_bytes());
    digest.update(bytes);
}

#[cfg(test)]
#[path = "registry_snapshot/tests.rs"]
mod tests;
