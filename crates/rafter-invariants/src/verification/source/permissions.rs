//! Best-effort read-only hardening backed by mandatory sealed-tree checks.

use std::{error::Error, fs, path::Path};

use crate::execution::filesystem::OperationDeadline;

pub(super) fn harden_file(path: &Path, executable: bool) -> Result<(), std::io::Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(
            path,
            fs::Permissions::from_mode(if executable { 0o500 } else { 0o400 }),
        )?;
    }
    #[cfg(not(unix))]
    {
        let _ = (path, executable);
    }
    Ok(())
}

pub(super) fn harden_directories(root: &Path) -> Result<(), Box<dyn Error>> {
    harden_directories_bounded(
        root,
        OperationDeadline::none("harden verifier-owned directories"),
        u64::MAX,
    )
}

pub(super) fn harden_directories_bounded(
    root: &Path,
    deadline: OperationDeadline,
    maximum_nodes: u64,
) -> Result<(), Box<dyn Error>> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut directories = directories_bounded(root, deadline, maximum_nodes)?;
        directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
        for directory in directories {
            deadline.check()?;
            fs::set_permissions(directory, fs::Permissions::from_mode(0o500))?;
        }
        deadline.check()?;
    }
    #[cfg(not(unix))]
    {
        let _ = (root, deadline, maximum_nodes);
    }
    Ok(())
}

pub(super) fn restore_tree(root: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let _ = fs::set_permissions(root, fs::Permissions::from_mode(0o700));
        let Ok(mut directories) = directories(root) else {
            return;
        };
        directories.sort_by_key(|path| path.components().count());
        for directory in &directories {
            let _ = fs::set_permissions(directory, fs::Permissions::from_mode(0o700));
        }
        for directory in directories {
            let Ok(entries) = fs::read_dir(directory) else {
                continue;
            };
            for entry in entries.flatten() {
                if entry.file_type().is_ok_and(|kind| kind.is_file()) {
                    let _ = fs::set_permissions(entry.path(), fs::Permissions::from_mode(0o600));
                }
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = root;
    }
}

#[cfg(unix)]
fn directories(root: &Path) -> Result<Vec<std::path::PathBuf>, std::io::Error> {
    let mut found = vec![root.to_owned()];
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                found.push(entry.path());
                pending.push(entry.path());
            }
        }
    }
    Ok(found)
}

#[cfg(unix)]
fn directories_bounded(
    root: &Path,
    deadline: OperationDeadline,
    maximum_nodes: u64,
) -> Result<Vec<std::path::PathBuf>, Box<dyn Error>> {
    let mut found = vec![root.to_owned()];
    let mut pending = vec![root.to_owned()];
    let mut nodes = 0_u64;
    while let Some(directory) = pending.pop() {
        deadline.check()?;
        for entry in fs::read_dir(directory)? {
            deadline.check()?;
            let entry = entry?;
            nodes = nodes
                .checked_add(1)
                .ok_or("verifier-owned directory node count overflow")?;
            if nodes > maximum_nodes {
                return Err(format!(
                    "verifier-owned directory tree exceeds its node limit of {maximum_nodes}"
                )
                .into());
            }
            if entry.file_type()?.is_dir() {
                found.push(entry.path());
                pending.push(entry.path());
            }
        }
    }
    deadline.check()?;
    Ok(found)
}
