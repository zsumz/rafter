//! Durable inbound snapshot-transfer staging and restart recovery.
//!
//! This facade separates body publication, manifest encoding, recovery,
//! cleanup, and operator-facing status while exposing one store-internal API.

mod body;
mod cleanup;
mod codec;
mod constants;
mod error;
mod filesystem;
mod manifest;
mod paths;
mod read;
mod status;
mod write;

#[cfg(test)]
mod recovery_test;

pub use error::DecodePendingSnapshotTransferError;
pub use status::PendingSnapshotTransferStagingStatus;

pub(super) use body::open_staged_body;
pub(super) use cleanup::{
    clear_pending_snapshot_transfer, remove_abandoned_pending_snapshot_transfer_staging,
};
pub(super) use paths::pending_snapshot_transfer_body_path;
pub(super) use paths::staging_status;
pub(super) use read::read_pending_snapshot_transfer;
pub(super) use write::stage_pending_snapshot_chunk;
