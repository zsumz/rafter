//! Best-effort filesystem hardening backed by mandatory integrity checks.

use std::{fs, path::Path};

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

pub(super) fn harden_directories(root: &Path) -> Result<(), std::io::Error> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let mut directories = directories(root)?;
        directories.sort_by_key(|path| std::cmp::Reverse(path.components().count()));
        for directory in directories {
            fs::set_permissions(directory, fs::Permissions::from_mode(0o500))?;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = root;
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
