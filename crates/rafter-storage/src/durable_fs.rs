//! Durable file and parent-directory synchronization primitives.
//!
//! This module owns the fsync mechanics shared by file-backed stores and the
//! batching of distinct parent-directory syncs during bundle construction.

use std::{
    collections::BTreeSet,
    fs::File,
    io,
    path::{Path, PathBuf},
};

pub(crate) fn sync_parent_directory(path: &Path) -> io::Result<()> {
    let parent = parent_directory(path);
    sync_directory(&parent)
}

fn parent_directory(path: &Path) -> PathBuf {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf()
}

fn sync_directory(path: &Path) -> io::Result<()> {
    let directory = File::open(path)?;
    directory.sync_all()
}

#[derive(Debug, Default)]
pub(crate) struct ParentDirectorySyncBatch {
    parents: BTreeSet<PathBuf>,
}

impl ParentDirectorySyncBatch {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn record_parent_of(&mut self, path: &Path) {
        self.parents.insert(parent_directory(path));
    }

    pub(crate) fn flush(&mut self) -> io::Result<()> {
        for parent in &self.parents {
            sync_directory(parent)?;
        }
        self.parents.clear();
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn pending_count(&self) -> usize {
        self.parents.len()
    }
}

#[cfg(test)]
#[path = "durable_fs_test.rs"]
mod tests;
