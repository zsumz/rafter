//! Acceptance and materialization of one archive entry into the vendor tree.
//!
//! Entries are admitted only as canonical package-relative regular files or
//! directories, and every accepted file is written, hardened, and recorded in
//! the Cargo checksum inventory under the extraction budget.

use std::{io::Read, path::Path};

use sha2::{Digest, Sha256};

use crate::execution::filesystem::HeldDirectory;

use super::{
    package_relative, permissions, require_unique_path, ExtractionState, FilePlan,
    MAX_EXPANDED_PACKAGE_BYTES, MAX_FILE_BYTES, MAX_PACKAGE_ENTRIES,
};

impl ExtractionState {
    pub(super) fn accept<R: Read>(
        &mut self,
        root: &HeldDirectory,
        entry: &mut tar::Entry<'_, R>,
    ) -> Result<(), String> {
        self.budget.check()?;
        if entry
            .pax_extensions()
            .map_err(|error| format!("read registry PAX extensions: {error}"))?
            .is_some()
        {
            return Err(format!(
                "registry package {} contains unsupported PAX metadata",
                self.package_root
            ));
        }
        self.archive_entries = self
            .archive_entries
            .checked_add(1)
            .ok_or_else(|| "registry archive entry count overflow".to_owned())?;
        if self.archive_entries > MAX_PACKAGE_ENTRIES {
            return Err(format!(
                "registry package {} exceeds its archive entry limit",
                self.package_root
            ));
        }
        let archive_path = entry
            .path()
            .map_err(|error| format!("decode registry archive path: {error}"))?;
        let relative = package_relative(&archive_path, &self.package_root)?;
        let entry_type = entry.header().entry_type();
        if relative.as_os_str().is_empty() {
            if !entry_type.is_dir() {
                return Err(format!(
                    "registry package root {} is not a directory",
                    self.package_root
                ));
            }
            return Ok(());
        }
        if relative
            .file_name()
            .is_some_and(|name| name == ".cargo-checksum.json")
        {
            return Err(format!(
                "registry package {} contains a preexisting Cargo checksum file",
                self.package_root
            ));
        }
        require_unique_path(&relative, &mut self.seen, &mut self.folded)?;
        if entry_type.is_dir() {
            return Ok(());
        }
        if !entry_type.is_file() {
            return Err(format!(
                "registry package {} contains a non-regular archive entry: {}",
                self.package_root,
                relative.display()
            ));
        }
        self.reserve_parent_directories(&relative)?;
        self.reserve_entry()?;
        self.extract_file(root, entry, &relative)
    }

    fn extract_file<R: Read>(
        &mut self,
        root: &HeldDirectory,
        entry: &mut tar::Entry<'_, R>,
        relative: &Path,
    ) -> Result<(), String> {
        let declared = entry
            .header()
            .size()
            .map_err(|error| format!("read registry entry size: {error}"))?;
        if declared > MAX_FILE_BYTES {
            return Err(format!(
                "registry file {} exceeds its size limit",
                relative.display()
            ));
        }
        self.expanded_bytes = self
            .expanded_bytes
            .checked_add(declared)
            .ok_or_else(|| "registry expanded byte count overflow".to_owned())?;
        if self.expanded_bytes > MAX_EXPANDED_PACKAGE_BYTES
            || self.expanded_bytes > self.budget.expanded_bytes
        {
            return Err(format!(
                "registry package {} exceeds its expanded size limit",
                self.package_root
            ));
        }
        let mut bytes = Vec::with_capacity(
            usize::try_from(declared).map_err(|_| "registry file length overflow")?,
        );
        entry
            .by_ref()
            .take(MAX_FILE_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|error| format!("read registry file {}: {error}", relative.display()))?;
        self.budget.check()?;
        if u64::try_from(bytes.len()).map_err(|_| "registry file length overflow")? != declared {
            return Err(format!(
                "registry file {} has a truncated body",
                relative.display()
            ));
        }
        let target = self.vendor_root.join(relative);
        root.write_atomic(&target, &bytes)
            .map_err(|error| format!("write registry file {}: {error}", target.display()))?;
        let executable = entry
            .header()
            .mode()
            .map_err(|error| format!("read registry file mode: {error}"))?
            & 0o111
            != 0;
        permissions::harden_file(&root.external_path().join(&target), executable)
            .map_err(|error| format!("harden registry file {}: {error}", target.display()))?;
        let digest_value = Sha256::digest(&bytes);
        self.cargo_files.insert(
            relative
                .to_str()
                .ok_or_else(|| "registry source path is not UTF-8".to_owned())?
                .replace(std::path::MAIN_SEPARATOR, "/"),
            format!("{digest_value:x}"),
        );
        let digest = digest_value.into();
        self.plans.insert(
            target,
            FilePlan {
                digest,
                #[cfg(unix)]
                executable,
            },
        );
        Ok(())
    }
}
