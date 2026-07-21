//! Active authenticated-snapshot registration for source graph verification.

use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Mutex, Weak},
};

pub(super) use super::super::sealed::FilePlan as SnapshotFilePlan;
pub(super) type SnapshotIntegrity = super::super::sealed::SealedTree;

type SnapshotRegistry = BTreeMap<PathBuf, Weak<SnapshotIntegrity>>;

static SNAPSHOTS: LazyLock<Mutex<SnapshotRegistry>> = LazyLock::new(|| Mutex::new(BTreeMap::new()));

pub(super) fn register(snapshot: &Arc<SnapshotIntegrity>) -> Result<(), String> {
    let mut snapshots = SNAPSHOTS
        .lock()
        .map_err(|_| "authenticated source snapshot registry is poisoned".to_owned())?;
    if snapshots
        .get(snapshot.root())
        .and_then(Weak::upgrade)
        .is_some()
    {
        return Err("authenticated source snapshot root is already registered".to_owned());
    }
    snapshots.insert(snapshot.root().to_owned(), Arc::downgrade(snapshot));
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
    let Some(registered) = snapshots.get(snapshot.root()) else {
        return;
    };
    if Weak::ptr_eq(registered, &Arc::downgrade(snapshot)) {
        snapshots.remove(snapshot.root());
    }
}
