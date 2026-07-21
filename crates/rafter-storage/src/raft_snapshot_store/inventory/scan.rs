//! Deterministic classification of snapshot-directory artifacts.

use std::{fs, path::PathBuf};

use rafter::{LogIndex, NodeId, Term};

use super::super::FileRaftSnapshotStore;
use super::model::{
    SnapshotFileIdentity, SnapshotFileInfo, SnapshotInventory, SnapshotInventoryError,
    SnapshotTemporaryFileInfo, SnapshotTemporaryFileKind,
};

const CURRENT_MANIFEST_FILE_NAME: &str = "current.snapshot";
const PENDING_TRANSFER_MANIFEST_FILE_NAME: &str = "pending.snapshot-transfer";
const PENDING_TRANSFER_BODY_FILE_NAME: &str = "pending.snapshot-transfer.body";

impl FileRaftSnapshotStore {
    /// Inspects current, unreferenced, temporary, foreign, and pending-transfer
    /// artifacts without materializing snapshot payloads.
    ///
    /// Only canonically named unreferenced snapshots and recognized temporary
    /// files are eligible for maintenance. Unrecognized entries are reported
    /// but never deleted.
    ///
    /// # Errors
    ///
    /// Returns [`SnapshotInventoryError::StoreRequiresReopen`] when an earlier
    /// mutation has an ambiguous durable outcome, or a typed filesystem error
    /// when the directory cannot be inspected consistently.
    pub fn snapshot_inventory(&self) -> Result<SnapshotInventory, SnapshotInventoryError> {
        if self.requires_reopen() {
            return Err(SnapshotInventoryError::StoreRequiresReopen);
        }

        InventoryBuilder::new(self)
            .scan_directory()?
            .finish(self.pending_snapshot_transfer_staging_status())
    }
}

struct InventoryBuilder<'a> {
    store: &'a FileRaftSnapshotStore,
    selected_name: Option<String>,
    current: Option<SnapshotFileInfo>,
    canonical: Vec<SnapshotFileInfo>,
    temporary: Vec<SnapshotTemporaryFileInfo>,
    unrecognized: Vec<String>,
}

impl<'a> InventoryBuilder<'a> {
    fn new(store: &'a FileRaftSnapshotStore) -> Self {
        Self {
            store,
            selected_name: store.current_snapshot_file_name().map(str::to_owned),
            current: None,
            canonical: Vec::new(),
            temporary: Vec::new(),
            unrecognized: Vec::new(),
        }
    }

    fn scan_directory(mut self) -> Result<Self, SnapshotInventoryError> {
        let entries = fs::read_dir(&self.store.directory).map_err(|error| {
            inventory_io_error(
                "read raft snapshot directory",
                self.store.directory.clone(),
                error,
            )
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| {
                inventory_io_error(
                    "read raft snapshot directory entry",
                    self.store.directory.clone(),
                    error,
                )
            })?;
            self.classify_entry(&entry)?;
        }
        Ok(self)
    }

    fn classify_entry(&mut self, entry: &fs::DirEntry) -> Result<(), SnapshotInventoryError> {
        let path = entry.path();
        let os_name = entry.file_name();
        let display_name = os_name.to_string_lossy().into_owned();
        let Some(file_name) = os_name.to_str().map(str::to_owned) else {
            self.unrecognized.push(display_name);
            return Ok(());
        };
        let file_type = entry.file_type().map_err(|error| {
            inventory_io_error("inspect raft snapshot directory entry", path.clone(), error)
        })?;

        if self.selected_name.as_deref() == Some(file_name.as_str()) {
            return self.classify_current(entry, path, file_name, file_type);
        }
        if is_stable_snapshot_store_file(&file_name) {
            if !file_type.is_file() {
                self.unrecognized.push(file_name);
            }
            return Ok(());
        }
        if !file_type.is_file() {
            self.unrecognized.push(file_name);
            return Ok(());
        }

        let bytes = entry
            .metadata()
            .map_err(|error| inventory_io_error("stat raft snapshot directory entry", path, error))?
            .len();
        if let Some(identity) = parse_snapshot_file_identity(&file_name) {
            self.canonical.push(SnapshotFileInfo {
                file_name,
                bytes,
                identity: Some(identity),
            });
        } else if let Some((kind, process_id)) = parse_temporary_file(&file_name) {
            self.temporary.push(SnapshotTemporaryFileInfo {
                file_name,
                bytes,
                kind,
                process_id,
            });
        } else {
            self.unrecognized.push(file_name);
        }
        Ok(())
    }

    fn classify_current(
        &mut self,
        entry: &fs::DirEntry,
        path: PathBuf,
        file_name: String,
        file_type: fs::FileType,
    ) -> Result<(), SnapshotInventoryError> {
        if !file_type.is_file() {
            return Err(SnapshotInventoryError::CurrentSnapshotNotRegularFile { path });
        }
        let bytes = entry
            .metadata()
            .map_err(|error| inventory_io_error("stat current raft snapshot", path, error))?
            .len();
        self.current = Some(SnapshotFileInfo {
            identity: parse_snapshot_file_identity(&file_name),
            file_name,
            bytes,
        });
        Ok(())
    }

    fn finish(
        mut self,
        pending_transfer: super::super::PendingSnapshotTransferStagingStatus,
    ) -> Result<SnapshotInventory, SnapshotInventoryError> {
        if let Some(file_name) = self.selected_name {
            if self.current.is_none() {
                return Err(SnapshotInventoryError::CurrentSnapshotMissing {
                    path: self.store.directory.join(file_name),
                });
            }
        }

        sort_snapshot_files(&mut self.canonical);
        let current_sequence = self
            .current
            .as_ref()
            .and_then(|file| file.identity)
            .map(|identity| identity.sequence);
        let (retained, unreferenced) = self.canonical.into_iter().partition(|file| {
            snapshot_sequence(file)
                .zip(current_sequence)
                .is_some_and(|(file_sequence, current_sequence)| file_sequence < current_sequence)
        });
        self.temporary
            .sort_by(|left, right| left.file_name.cmp(&right.file_name));
        self.unrecognized.sort();

        Ok(SnapshotInventory {
            current: self.current,
            retained,
            unreferenced,
            temporary: self.temporary,
            unrecognized: self.unrecognized,
            pending_transfer,
        })
    }
}

fn sort_snapshot_files(files: &mut [SnapshotFileInfo]) {
    files.sort_by(|left, right| {
        snapshot_sequence(left)
            .cmp(&snapshot_sequence(right))
            .then(left.file_name.cmp(&right.file_name))
    });
}

fn snapshot_sequence(snapshot: &SnapshotFileInfo) -> Option<u64> {
    snapshot.identity.map(|identity| identity.sequence)
}

fn parse_snapshot_file_identity(file_name: &str) -> Option<SnapshotFileIdentity> {
    let fields = file_name.strip_prefix("snapshot-")?.strip_suffix(".rfsn")?;
    let mut fields = fields.split('-');
    let identity = SnapshotFileIdentity {
        sequence: fields.next()?.parse().ok()?,
        last_included_index: LogIndex(fields.next()?.parse().ok()?),
        last_included_term: Term(fields.next()?.parse().ok()?),
        writer_id: NodeId(fields.next()?.parse().ok()?),
    };
    fields.next().is_none().then_some(identity)
}

fn parse_temporary_file(file_name: &str) -> Option<(SnapshotTemporaryFileKind, u64)> {
    for (prefix, kind) in [
        (".snapshot-", SnapshotTemporaryFileKind::SnapshotEnvelope),
        (
            ".current.snapshot-",
            SnapshotTemporaryFileKind::CurrentManifest,
        ),
        (
            ".pending.snapshot-transfer-",
            SnapshotTemporaryFileKind::PendingTransferManifest,
        ),
    ] {
        let Some(process_id) = file_name
            .strip_prefix(prefix)
            .and_then(|rest| rest.strip_suffix(".tmp"))
            .and_then(|value| value.parse().ok())
        else {
            continue;
        };
        return Some((kind, process_id));
    }
    None
}

fn is_stable_snapshot_store_file(file_name: &str) -> bool {
    matches!(
        file_name,
        CURRENT_MANIFEST_FILE_NAME
            | PENDING_TRANSFER_MANIFEST_FILE_NAME
            | PENDING_TRANSFER_BODY_FILE_NAME
    )
}

fn inventory_io_error(
    operation: &'static str,
    path: PathBuf,
    error: std::io::Error,
) -> SnapshotInventoryError {
    SnapshotInventoryError::Io {
        operation,
        path,
        source: error.into(),
    }
}
