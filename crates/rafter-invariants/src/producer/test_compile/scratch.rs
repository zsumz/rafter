//! Held scratch directory for producer-side Cargo compilation.

use std::{error::Error, path::Path, path::PathBuf};

use crate::execution::filesystem::{HeldDirectory, OperationDeadline, TREE_LIMITS};

pub(in crate::producer) struct PreparedTargetDir {
    handle: HeldDirectory,
}

impl PreparedTargetDir {
    pub(in crate::producer) fn external_path(&self) -> PathBuf {
        self.handle.external_path()
    }

    pub(in crate::producer) fn verify(&self) -> Result<(), Box<dyn Error>> {
        self.handle.verify_path_binding()
    }
}

pub(in crate::producer) fn prepare_target_dir(
    profile: &str,
    source_ref: &str,
    deadline: std::time::Instant,
) -> Result<PreparedTargetDir, Box<dyn Error>> {
    let source_prefix = source_ref.get(..12).unwrap_or(source_ref);
    let directory = Path::new("target/rafter-invariants/build")
        .join(source_prefix)
        .join(format!("{profile}-tests"));
    let directory_guard = HeldDirectory::replace_tree(
        &directory,
        TREE_LIMITS,
        OperationDeadline::at(deadline, "test compile scratch cleanup"),
    )?;
    directory_guard.verify_path_binding()?;
    Ok(PreparedTargetDir {
        handle: directory_guard,
    })
}
