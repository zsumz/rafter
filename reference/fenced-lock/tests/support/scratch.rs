//! Scratch directories for the tests that touch a real filesystem.
//!
//! A failing crash test reproduces from its printed fault plan, not from
//! surviving files, so nothing here tries to preserve a directory across a
//! panic.

use std::path::Path;

use rafter_reference_harness::process::ScratchSpace;

/// A fenced-lock-namespaced scratch directory.
#[derive(Debug)]
pub struct ScratchDir {
    space: ScratchSpace,
}

impl ScratchDir {
    /// Creates an empty scratch directory tagged with `label`.
    pub fn new(label: &str) -> Self {
        Self {
            space: ScratchSpace::create("rafter-reference-fenced-lock", label)
                .expect("a scratch directory is creatable"),
        }
    }

    /// Returns the directory's path.
    pub fn path(&self) -> &Path {
        self.space.path()
    }
}
