//! Snapshot identities, authenticated inventories, and fail-closed revalidation.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Mutex, Weak},
};

use sha2::{Digest, Sha256};

use super::identity::FileIdentity;

#[derive(Clone, Debug)]
pub(super) struct SnapshotFilePlan {
    pub(super) digest: [u8; 32],
    #[cfg(unix)]
    pub(super) executable: bool,
}

#[derive(Debug)]
struct SnapshotFile {
    identity: FileIdentity,
    digest: [u8; 32],
    #[cfg(unix)]
    executable: bool,
}

#[derive(Debug)]
pub(super) struct SnapshotIntegrity {
    root: PathBuf,
    root_identity: FileIdentity,
    directories: BTreeSet<PathBuf>,
    files: BTreeMap<PathBuf, SnapshotFile>,
}

type SnapshotRegistry = BTreeMap<PathBuf, Weak<SnapshotIntegrity>>;

static SNAPSHOTS: LazyLock<Mutex<SnapshotRegistry>> = LazyLock::new(|| Mutex::new(BTreeMap::new()));

impl SnapshotIntegrity {
    pub(super) fn capture(
        root: &Path,
        plans: BTreeMap<PathBuf, SnapshotFilePlan>,
    ) -> Result<Self, String> {
        let root_identity = FileIdentity::capture(root, true)?;
        let directories = expected_directories(plans.keys());
        let mut files = BTreeMap::new();
        for (relative, plan) in plans {
            let identity = FileIdentity::capture(&root.join(&relative), false)?;
            files.insert(
                relative,
                SnapshotFile {
                    identity,
                    digest: plan.digest,
                    #[cfg(unix)]
                    executable: plan.executable,
                },
            );
        }
        let snapshot = Self {
            root: root.to_owned(),
            root_identity,
            directories,
            files,
        };
        snapshot.revalidate()?;
        Ok(snapshot)
    }

    pub(super) fn root(&self) -> &Path {
        &self.root
    }

    pub(super) fn tracked_paths(&self) -> HashSet<PathBuf> {
        self.files.keys().cloned().collect()
    }

    pub(super) fn revalidate(&self) -> Result<(), String> {
        if FileIdentity::capture(&self.root, true)? != self.root_identity {
            return Err("authenticated source snapshot root identity changed".to_owned());
        }
        let (directories, files) = inventory(&self.root)?;
        if directories != self.directories || files != self.files.keys().cloned().collect() {
            return Err("authenticated source snapshot path inventory changed".to_owned());
        }
        verify_directory_permissions(&self.root, &directories)?;
        for (relative, expected) in &self.files {
            revalidate_file(&self.root.join(relative), expected)?;
        }
        Ok(())
    }
}

pub(super) fn register(snapshot: &Arc<SnapshotIntegrity>) -> Result<(), String> {
    let mut snapshots = SNAPSHOTS
        .lock()
        .map_err(|_| "authenticated source snapshot registry is poisoned".to_owned())?;
    if snapshots
        .get(&snapshot.root)
        .and_then(Weak::upgrade)
        .is_some()
    {
        return Err("authenticated source snapshot root is already registered".to_owned());
    }
    snapshots.insert(snapshot.root.clone(), Arc::downgrade(snapshot));
    Ok(())
}

pub(super) fn registered(root: &Path) -> Result<Option<Arc<SnapshotIntegrity>>, String> {
    let root = fs::canonicalize(root)
        .map_err(|error| format!("canonicalize source snapshot {}: {error}", root.display()))?;
    let mut snapshots = SNAPSHOTS
        .lock()
        .map_err(|_| "authenticated source snapshot registry is poisoned".to_owned())?;
    let snapshot = snapshots.get(&root).and_then(Weak::upgrade);
    if snapshot.is_none() {
        snapshots.remove(&root);
    }
    Ok(snapshot)
}

pub(super) fn unregister(snapshot: &Arc<SnapshotIntegrity>) {
    let Ok(mut snapshots) = SNAPSHOTS.lock() else {
        return;
    };
    let Some(registered) = snapshots.get(&snapshot.root) else {
        return;
    };
    if Weak::ptr_eq(registered, &Arc::downgrade(snapshot)) {
        snapshots.remove(&snapshot.root);
    }
}

fn revalidate_file(path: &Path, expected: &SnapshotFile) -> Result<(), String> {
    if FileIdentity::capture(path, false)? != expected.identity {
        return Err(format!(
            "authenticated source snapshot file identity changed: {}",
            path.display()
        ));
    }
    let bytes = fs::read(path)
        .map_err(|error| format!("read authenticated source {}: {error}", path.display()))?;
    let digest: [u8; 32] = Sha256::digest(bytes).into();
    if digest != expected.digest {
        return Err(format!(
            "authenticated source snapshot file bytes changed: {}",
            path.display()
        ));
    }
    #[cfg(unix)]
    verify_unix_mode(path, expected.executable)?;
    Ok(())
}

fn expected_directories<'a>(paths: impl Iterator<Item = &'a PathBuf>) -> BTreeSet<PathBuf> {
    let mut directories = BTreeSet::from([PathBuf::new()]);
    for path in paths {
        let mut parent = path.parent();
        while let Some(directory) = parent.filter(|directory| !directory.as_os_str().is_empty()) {
            directories.insert(directory.to_owned());
            parent = directory.parent();
        }
    }
    directories
}

fn inventory(root: &Path) -> Result<(BTreeSet<PathBuf>, BTreeSet<PathBuf>), String> {
    let mut directories = BTreeSet::from([PathBuf::new()]);
    let mut files = BTreeSet::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        let entries = fs::read_dir(&directory)
            .map_err(|error| format!("read snapshot directory {}: {error}", directory.display()))?;
        for entry in entries {
            let entry = entry.map_err(|error| format!("read snapshot entry: {error}"))?;
            let metadata = fs::symlink_metadata(entry.path()).map_err(|error| {
                format!("inspect snapshot entry {}: {error}", entry.path().display())
            })?;
            let relative = entry
                .path()
                .strip_prefix(root)
                .map_err(|_| "snapshot entry escaped its root".to_owned())?
                .to_owned();
            if metadata.file_type().is_dir() {
                directories.insert(relative);
                pending.push(entry.path());
            } else if metadata.file_type().is_file() {
                files.insert(relative);
            } else {
                return Err(format!(
                    "authenticated source snapshot contains a non-regular entry: {}",
                    entry.path().display()
                ));
            }
        }
    }
    Ok((directories, files))
}

#[cfg(unix)]
fn verify_directory_permissions(
    root: &Path,
    directories: &BTreeSet<PathBuf>,
) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;

    for relative in directories {
        let path = root.join(relative);
        let mode = fs::symlink_metadata(&path)
            .map_err(|error| format!("inspect snapshot permissions {}: {error}", path.display()))?
            .permissions()
            .mode();
        if mode & 0o222 != 0 {
            return Err(format!(
                "authenticated source snapshot directory became writable: {}",
                path.display()
            ));
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn verify_directory_permissions(
    _root: &Path,
    _directories: &BTreeSet<PathBuf>,
) -> Result<(), String> {
    Ok(())
}

#[cfg(unix)]
fn verify_unix_mode(path: &Path, executable: bool) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;

    let mode = fs::symlink_metadata(path)
        .map_err(|error| format!("inspect snapshot permissions {}: {error}", path.display()))?
        .permissions()
        .mode()
        & 0o777;
    let expected = if executable { 0o500 } else { 0o400 };
    if mode != expected {
        return Err(format!(
            "authenticated source snapshot permissions changed: {}",
            path.display()
        ));
    }
    Ok(())
}
