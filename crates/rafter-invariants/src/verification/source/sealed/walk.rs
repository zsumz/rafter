//! Bounded traversal of a sealed tree and its directory permission checks.
//!
//! The walk is deadline- and node-bounded so a tree that grows underneath the
//! verifier fails closed, and every directory it reports must still be
//! unwritable.

use std::{
    collections::BTreeSet,
    fs,
    path::{Path, PathBuf},
};

use crate::execution::filesystem::OperationDeadline;

use super::check_deadline;

pub(super) fn inventory(
    root: &Path,
    context: &str,
    deadline: OperationDeadline,
    maximum_nodes: u64,
) -> Result<(BTreeSet<PathBuf>, BTreeSet<PathBuf>), String> {
    let mut directories = BTreeSet::from([PathBuf::new()]);
    let mut files = BTreeSet::new();
    let mut pending = vec![root.to_owned()];
    let mut nodes = 0_u64;
    while let Some(directory) = pending.pop() {
        check_deadline(deadline)?;
        let entries = fs::read_dir(&directory).map_err(|error| {
            format!("read {context} directory {}: {error}", directory.display())
        })?;
        for entry in entries {
            check_deadline(deadline)?;
            let entry = entry.map_err(|error| format!("read {context} entry: {error}"))?;
            nodes = nodes
                .checked_add(1)
                .ok_or_else(|| format!("{context} node count overflow"))?;
            if nodes > maximum_nodes {
                return Err(format!(
                    "{context} tree exceeds its node limit of {maximum_nodes}"
                ));
            }
            let metadata = fs::symlink_metadata(entry.path()).map_err(|error| {
                format!(
                    "inspect {context} entry {}: {error}",
                    entry.path().display()
                )
            })?;
            let relative = entry
                .path()
                .strip_prefix(root)
                .map_err(|_| format!("{context} entry escaped its root"))?
                .to_owned();
            if metadata.file_type().is_dir() {
                directories.insert(relative);
                pending.push(entry.path());
            } else if metadata.file_type().is_file() {
                files.insert(relative);
            } else {
                return Err(format!(
                    "{context} contains a non-regular entry: {}",
                    entry.path().display()
                ));
            }
        }
    }
    check_deadline(deadline)?;
    Ok((directories, files))
}

#[cfg(unix)]
pub(super) fn verify_directory_permissions(
    root: &Path,
    directories: &BTreeSet<PathBuf>,
    context: &str,
    deadline: OperationDeadline,
) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;

    for relative in directories {
        check_deadline(deadline)?;
        let path = root.join(relative);
        let mode = fs::symlink_metadata(&path)
            .map_err(|error| format!("inspect {context} permissions {}: {error}", path.display()))?
            .permissions()
            .mode();
        if mode & 0o222 != 0 {
            return Err(format!(
                "{context} directory became writable: {}",
                path.display()
            ));
        }
    }
    Ok(())
}

#[cfg(not(unix))]
pub(super) fn verify_directory_permissions(
    _root: &Path,
    _directories: &BTreeSet<PathBuf>,
    _context: &str,
    _deadline: OperationDeadline,
) -> Result<(), String> {
    Ok(())
}
