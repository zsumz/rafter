//! Clean producer-to-verifier workspace path mapping for simulator evidence.

use std::{
    fs,
    path::{Component, Path, PathBuf},
};

use crate::{evidence::ResultBundle, verification::AggregateError};

pub(super) struct SimulatorRoots {
    pub(super) producer: PathBuf,
    pub(super) active: PathBuf,
}

pub(super) fn simulator_roots(
    bundle: &ResultBundle,
    root: &Path,
) -> Result<SimulatorRoots, AggregateError> {
    let producer = PathBuf::from(&bundle.execution.invocation.current_dir);
    if !clean_absolute_path(&producer) {
        return Err(AggregateError::new(
            "simulator producer root must be a clean absolute path".to_owned(),
        ));
    }
    let active = fs::canonicalize(root)
        .map_err(|error| AggregateError::new(format!("canonicalize simulator root: {error}")))?;
    if !clean_absolute_path(&active) {
        return Err(AggregateError::new(
            "simulator active root is not a clean canonical path".to_owned(),
        ));
    }
    Ok(SimulatorRoots { producer, active })
}

pub(super) fn resolve_producer_path(
    root: &Path,
    recorded: &Path,
) -> Result<PathBuf, AggregateError> {
    let resolved = if recorded.is_absolute() {
        recorded.to_owned()
    } else {
        if recorded
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(AggregateError::new(
                "simulator compile invocation recorded an unsafe relative Cargo target directory"
                    .to_owned(),
            ));
        }
        root.join(recorded)
    };
    if !clean_absolute_path(&resolved) {
        return Err(AggregateError::new(
            "simulator compile invocation recorded a non-canonical Cargo target directory"
                .to_owned(),
        ));
    }
    Ok(resolved)
}

pub(super) fn map_producer_path(
    path: &Path,
    producer_root: &Path,
    active_root: &Path,
    context: &str,
) -> Result<PathBuf, AggregateError> {
    if !clean_absolute_path(path) {
        return Err(AggregateError::new(format!(
            "{context} is not a clean absolute producer path"
        )));
    }
    let relative = path.strip_prefix(producer_root).map_err(|_| {
        AggregateError::new(format!("{context} escapes the recorded producer root"))
    })?;
    if relative.as_os_str().is_empty()
        || relative
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(AggregateError::new(format!(
            "{context} cannot be safely mapped into the active root"
        )));
    }
    Ok(active_root.join(relative))
}

pub(super) fn verify_active_path(
    path: &Path,
    directory: bool,
    context: &str,
) -> Result<(), AggregateError> {
    let metadata = fs::symlink_metadata(path)
        .map_err(|error| AggregateError::new(format!("read active {context}: {error}")))?;
    let canonical = fs::canonicalize(path)
        .map_err(|error| AggregateError::new(format!("canonicalize active {context}: {error}")))?;
    let expected_type = if directory {
        metadata.file_type().is_dir()
    } else {
        metadata.file_type().is_file()
    };
    if !expected_type || canonical != path {
        return Err(AggregateError::new(format!(
            "active {context} is not the exact non-symlink workspace path"
        )));
    }
    Ok(())
}

pub(super) fn clean_absolute_path(path: &Path) -> bool {
    if !path.is_absolute() || path.components().collect::<PathBuf>() != path {
        return false;
    }
    let mut has_normal_component = false;
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => {}
            Component::Normal(_) => has_normal_component = true,
            Component::CurDir | Component::ParentDir => return false,
        }
    }
    has_normal_component
}
