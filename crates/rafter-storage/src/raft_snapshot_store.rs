//! Durable snapshot-store facade.
//!
//! The public API stays flat while focused modules own the store contract,
//! error vocabulary, inventory and maintenance, concrete file state,
//! mutations, publication, chunk sourcing, validation, opening, and
//! pending-transfer filesystem mechanics.

mod contract;
mod error;
mod file;
mod health;
mod in_memory;
mod inventory;
mod manifest;
mod open;
mod open_report;
mod pending_transfer;
mod publish;
mod source;
mod state;
mod validation;

pub use contract::RaftSnapshotStore;
pub use error::{OpenRaftSnapshotStoreError, RaftSnapshotStoreWriteError};
pub use in_memory::InMemoryRaftSnapshotStore;
pub use inventory::{
    SnapshotFileIdentity, SnapshotFileInfo, SnapshotInventory, SnapshotInventoryError,
    SnapshotPruneError, SnapshotPruneReport, SnapshotRetention, SnapshotTemporaryFileInfo,
    SnapshotTemporaryFileKind,
};
pub use manifest::{RaftSnapshotManifestDecodeError, RaftSnapshotManifestEncodeError};
pub use open_report::{
    FileRaftSnapshotStoreOpenReport, OpenedFileRaftSnapshotStore, PendingSnapshotTransferRecovery,
};
pub use pending_transfer::{
    DecodePendingSnapshotTransferError, PendingSnapshotTransferStagingStatus,
};
pub use state::FileRaftSnapshotStore;

#[cfg(test)]
use crate::{EncodeRaftSnapshotError, PersistedRaftSnapshot};
use manifest::{encode_manifest, manifest_path, read_manifest, SnapshotManifest};
use pending_transfer::read_pending_snapshot_transfer;
use source::{source_chunk, stream_chunk_len, SNAPSHOT_STREAM_CHUNK_BYTES};
use state::{snapshot_path, CurrentSnapshot, StagedTransfer};

#[cfg(test)]
#[path = "raft_snapshot_store/health_test.rs"]
mod health_test;
#[cfg(test)]
#[path = "raft_snapshot_store/inventory_test.rs"]
mod inventory_test;
#[cfg(test)]
#[path = "raft_snapshot_store/pending_transfer_cleanup_test.rs"]
mod pending_transfer_cleanup_test;
#[cfg(test)]
#[path = "raft_snapshot_store/pending_transfer_test.rs"]
mod pending_transfer_test;
#[cfg(test)]
#[path = "raft_snapshot_store_test.rs"]
mod raft_snapshot_store_test;
#[cfg(test)]
#[path = "raft_snapshot_store/test_support.rs"]
mod test_support;
#[cfg(test)]
#[path = "raft_snapshot_store/validation_test.rs"]
mod validation_test;
