//! Snapshot-directory inventory and maintenance facade.
//!
//! Scanning, public maintenance vocabulary, and directory-synced deletion live
//! in focused child modules. Only canonical unreferenced snapshots and known
//! temporary files are ever eligible for removal.

mod model;
mod prune;
mod scan;

pub use model::{
    SnapshotFileIdentity, SnapshotFileInfo, SnapshotInventory, SnapshotInventoryError,
    SnapshotPruneError, SnapshotPruneReport, SnapshotRetention, SnapshotTemporaryFileInfo,
    SnapshotTemporaryFileKind,
};
