//! Atomic file publication and descriptor-confined renames.

use std::{
    error::Error,
    ffi::{OsStr, OsString},
    io::Write,
    path::Path,
    sync::atomic::{AtomicU64, Ordering},
};

use cap_fs_ext::{DirExt, FollowSymlinks, OpenOptionsFollowExt};
use cap_std::fs::OpenOptions;

use super::{paths::workspace_relative, sync::sync_directory, HeldDirectory};

static NEXT_TEMPORARY: AtomicU64 = AtomicU64::new(0);

impl HeldDirectory {
    pub(crate) fn write_atomic(&self, path: &Path, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
        let (parent, name) = self.parent_and_name(path, true)?;
        let temporary = temporary_name(&name);
        let mut options = OpenOptions::new();
        options
            .create_new(true)
            .write(true)
            .follow(FollowSymlinks::No);
        let mut file = parent.dir.open_with(&temporary, &options)?;
        let publish = (|| -> Result<(), Box<dyn Error>> {
            file.write_all(bytes)?;
            file.sync_all()?;
            drop(file);
            parent.dir.rename(&temporary, &parent.dir, &name)?;
            sync_directory(&parent.dir, &name)?;
            Ok(())
        })();
        let _ = parent.dir.remove_file_or_symlink(&temporary);
        publish
    }

    pub(crate) fn rename(&self, from: &Path, to: &Path) -> Result<(), Box<dyn Error>> {
        let (from_parent, from_name) = self.parent_and_name(from, false)?;
        let (to_parent, to_name) = self.parent_and_name(to, true)?;
        from_parent
            .dir
            .rename(&from_name, &to_parent.dir, &to_name)?;
        Ok(())
    }
}

pub(crate) fn path_exists(path: &Path) -> Result<bool, Box<dyn Error>> {
    let workspace = HeldDirectory::workspace()?;
    let relative = workspace_relative(&workspace, path)?;
    let (parent, name) = workspace.parent_and_name(&relative, false)?;
    Ok(parent.entry_kind(&name)?.is_some())
}

fn temporary_name(name: &OsStr) -> OsString {
    let sequence = NEXT_TEMPORARY.fetch_add(1, Ordering::Relaxed);
    let mut temporary = OsString::from(".");
    temporary.push(name);
    temporary.push(format!(".tmp-{}-{sequence}", std::process::id()));
    temporary
}
