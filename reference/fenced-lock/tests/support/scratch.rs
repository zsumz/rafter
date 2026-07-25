//! Scratch directories for the tests that touch a real filesystem.
//!
//! The reference workspace depends on Rafter crates and nothing else, so this
//! is consumer-written rather than a temporary-directory dependency. It is
//! deliberately small: a uniquely named directory under the system temporary
//! directory, removed when its handle drops.
//!
//! A failing crash test reproduces from its printed fault plan, not from
//! surviving files, so nothing here tries to preserve a directory across a
//! panic.

use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

/// Distinguishes directories within one process, since a test binary runs its
/// tests on several threads at once.
static NEXT_SCRATCH_ID: AtomicU64 = AtomicU64::new(0);

/// A uniquely named directory that is removed when this handle drops.
#[derive(Debug)]
pub struct ScratchDir {
    path: PathBuf,
}

impl ScratchDir {
    /// Creates an empty scratch directory tagged with `label`.
    pub fn new(label: &str) -> Self {
        let id = NEXT_SCRATCH_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "rafter-reference-fenced-lock.{}.{label}.{id}",
            std::process::id()
        ));
        // A previous run that died without unwinding could have left this name
        // behind; the tests want an empty directory either way.
        drop(fs::remove_dir_all(&path));
        fs::create_dir_all(&path).expect("a scratch directory is creatable");
        Self { path }
    }

    /// Returns the directory's path.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        drop(fs::remove_dir_all(&self.path));
    }
}
