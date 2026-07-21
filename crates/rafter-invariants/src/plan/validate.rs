//! Exact active-plan, bound-input, and repository-path validation.

use std::{
    error::Error,
    fs,
    path::{Component, Path, PathBuf},
};

use sha2::{Digest, Sha256};

use crate::evidence::PlanInput;

#[cfg(test)]
use crate::evidence::{ExecutionPlanReceipt, ResultBundle};

/// Requires a bundle to carry the exact active execution plan.
///
/// # Errors
///
/// Returns an error when any plan path, digest, size, profile, or selected
/// contract differs from the plan loaded by the current invocation.
#[cfg(test)]
pub(crate) fn verify_bundle_plan(
    bundle: &ResultBundle,
    expected: &ExecutionPlanReceipt,
) -> Result<(), Box<dyn Error>> {
    if bundle.execution.plan != *expected {
        return Err(format!(
            "runner {} execution plan does not match the active registry and manifest",
            bundle.runner
        )
        .into());
    }
    Ok(())
}

pub(crate) fn verify_plan_input(input: &PlanInput, root: &Path) -> Result<(), Box<dyn Error>> {
    let root = fs::canonicalize(root)?;
    let path = confined_path(&PathBuf::from(&input.path), &root)?;
    let bytes = fs::read(&path)?;
    if input.size_bytes != bytes.len() as u64
        || input.sha256 != format!("{:x}", Sha256::digest(&bytes))
    {
        return Err(format!("execution-plan input changed: {}", input.path).into());
    }
    Ok(())
}

pub(super) fn confined_path(path: &Path, root: &Path) -> Result<PathBuf, Box<dyn Error>> {
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(format!(
            "execution-plan path must be repository-relative: {}",
            path.display()
        )
        .into());
    }
    let canonical = fs::canonicalize(root.join(path))?;
    canonical
        .strip_prefix(root)
        .map_err(|_| format!("execution-plan path escapes repository: {}", path.display()))?;
    Ok(canonical)
}
