//! Private materialization of authenticated source bytes.

use std::{
    collections::{BTreeMap, HashSet},
    error::Error,
    fs::{self, OpenOptions},
    io::Write,
    path::{Component, Path, PathBuf},
    sync::Arc,
};

use sha2::{Digest, Sha256};

use crate::provenance::source::CapturedSourceFile;

mod identity;
mod integrity;
mod permissions;

use integrity::{SnapshotFilePlan, SnapshotIntegrity};

pub(super) struct SourceSnapshot {
    _directory: tempfile::TempDir,
    integrity: Arc<SnapshotIntegrity>,
}

impl SourceSnapshot {
    pub(super) fn materialize(files: Vec<CapturedSourceFile>) -> Result<Self, Box<dyn Error>> {
        let directory = tempfile::Builder::new()
            .prefix("rafter-verified-source-")
            .tempdir()?;
        let root = fs::canonicalize(directory.path())?;
        let plans = materialize_files(&root, files)?;
        if let Err(error) = permissions::harden_directories(&root) {
            permissions::restore_tree(&root);
            return Err(error.into());
        }
        let integrity = match SnapshotIntegrity::capture(&root, plans) {
            Ok(integrity) => Arc::new(integrity),
            Err(error) => {
                permissions::restore_tree(&root);
                return Err(error.into());
            }
        };
        if let Err(error) = integrity::register(&integrity) {
            permissions::restore_tree(&root);
            return Err(error.into());
        }
        Ok(Self {
            _directory: directory,
            integrity,
        })
    }

    pub(super) fn root(&self) -> &Path {
        self.integrity.root()
    }

    pub(super) fn revalidate(&self) -> Result<(), String> {
        self.integrity.revalidate()
    }
}

impl Drop for SourceSnapshot {
    fn drop(&mut self) {
        integrity::unregister(&self.integrity);
        permissions::restore_tree(self.integrity.root());
    }
}

pub(super) fn tracked_paths_at(root: &Path) -> Result<Option<HashSet<PathBuf>>, String> {
    integrity::registered(root)
        .map(|snapshot| {
            snapshot.map(|snapshot| {
                snapshot.revalidate()?;
                Ok(snapshot.tracked_paths())
            })
        })?
        .transpose()
}

pub(super) fn revalidate_at(root: &Path) -> Result<(), String> {
    if let Some(snapshot) = integrity::registered(root)? {
        snapshot.revalidate()?;
    }
    Ok(())
}

fn materialize_files(
    root: &Path,
    files: Vec<CapturedSourceFile>,
) -> Result<BTreeMap<PathBuf, SnapshotFilePlan>, Box<dyn Error>> {
    let mut plans = BTreeMap::new();
    for source in files {
        validate_relative_path(&source.path)?;
        let path = root.join(&source.path);
        let parent = path
            .parent()
            .ok_or_else(|| format!("captured source has no parent: {}", source.path.display()))?;
        fs::create_dir_all(parent)?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)?;
        file.write_all(&source.bytes)?;
        file.sync_all()?;
        permissions::harden_file(&path, source.executable)?;
        let digest = Sha256::digest(&source.bytes).into();
        if plans
            .insert(
                source.path,
                SnapshotFilePlan {
                    digest,
                    #[cfg(unix)]
                    executable: source.executable,
                },
            )
            .is_some()
        {
            return Err("captured source contains a duplicate path".into());
        }
    }
    Ok(plans)
}

fn validate_relative_path(path: &Path) -> Result<(), Box<dyn Error>> {
    if path.as_os_str().is_empty()
        || path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::Prefix(_)
                    | Component::RootDir
                    | Component::CurDir
                    | Component::ParentDir
            )
        })
    {
        return Err(format!(
            "captured source path is not canonical repository-relative: {}",
            path.display()
        )
        .into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "snapshot/tests.rs"]
mod tests;
