//! Authenticated mapping from recorded producer paths into the active checkout.

use std::{
    fs,
    path::{Component, Path, PathBuf},
};

use crate::evidence::ResultBundle;

use super::AggregateError;

pub(crate) struct RecordedWorkspace {
    producer: PathBuf,
    active: PathBuf,
}

impl RecordedWorkspace {
    pub(crate) fn new(bundle: &ResultBundle, active: &Path) -> Result<Self, AggregateError> {
        let producer = PathBuf::from(&bundle.execution.invocation.current_dir);
        if !clean_absolute_path(&producer) {
            return Err(AggregateError::new(
                "producer workspace root is not a clean absolute path".to_owned(),
            ));
        }
        let active = fs::canonicalize(active).map_err(|error| {
            AggregateError::new(format!("canonicalize active workspace: {error}"))
        })?;
        if !clean_absolute_path(&active) {
            return Err(AggregateError::new(
                "active workspace root is not a clean canonical path".to_owned(),
            ));
        }
        Ok(Self { producer, active })
    }

    pub(crate) fn producer(&self) -> &Path {
        &self.producer
    }

    pub(crate) fn active(&self) -> &Path {
        &self.active
    }

    pub(crate) fn producer_path(&self, relative: &Path) -> Result<PathBuf, AggregateError> {
        if relative.as_os_str().is_empty()
            || relative.is_absolute()
            || relative
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(AggregateError::new(
                "recorded workspace-relative path is not canonical".to_owned(),
            ));
        }
        Ok(self.producer.join(relative))
    }

    pub(crate) fn map(&self, recorded: &Path, context: &str) -> Result<PathBuf, AggregateError> {
        if !clean_absolute_path(recorded) {
            return Err(AggregateError::new(format!(
                "{context} is not a clean absolute producer path"
            )));
        }
        let relative = recorded.strip_prefix(&self.producer).map_err(|_| {
            AggregateError::new(format!("{context} escapes the recorded producer workspace"))
        })?;
        if relative.as_os_str().is_empty()
            || relative
                .components()
                .any(|component| !matches!(component, Component::Normal(_)))
        {
            return Err(AggregateError::new(format!(
                "{context} cannot be safely mapped into the active workspace"
            )));
        }
        Ok(self.active.join(relative))
    }

    pub(crate) fn verify_active_file(
        &self,
        recorded: &Path,
        context: &str,
    ) -> Result<PathBuf, AggregateError> {
        self.verify_active_path(recorded, false, context)
    }

    pub(crate) fn verify_active_directory(
        &self,
        recorded: &Path,
        context: &str,
    ) -> Result<PathBuf, AggregateError> {
        self.verify_active_path(recorded, true, context)
    }

    fn verify_active_path(
        &self,
        recorded: &Path,
        directory: bool,
        context: &str,
    ) -> Result<PathBuf, AggregateError> {
        let active = self.map(recorded, context)?;
        let metadata = fs::symlink_metadata(&active)
            .map_err(|error| AggregateError::new(format!("read active {context}: {error}")))?;
        let canonical = fs::canonicalize(&active).map_err(|error| {
            AggregateError::new(format!("canonicalize active {context}: {error}"))
        })?;
        let expected_type = if directory {
            metadata.file_type().is_dir()
        } else {
            metadata.file_type().is_file()
        };
        if !expected_type || canonical != active {
            return Err(AggregateError::new(format!(
                "active {context} is not the exact non-symlink workspace path"
            )));
        }
        Ok(active)
    }
}

fn clean_absolute_path(path: &Path) -> bool {
    path.is_absolute()
        && path
            .components()
            .all(|component| !matches!(component, Component::CurDir | Component::ParentDir))
}

#[cfg(test)]
#[path = "workspace/tests.rs"]
mod tests;
