//! Operator-facing pending-transfer staging status.
//!
//! This diagnostic vocabulary reports physical artifacts independently of
//! whether they form a resumable logical transfer.

/// File-level status of pending snapshot-transfer staging data.
///
/// This is diagnostic state; it may report abandoned files that are not a
/// resumable [`rafter::PendingSnapshotTransfer`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PendingSnapshotTransferStagingStatus {
    /// Whether the canonical pending-transfer manifest exists.
    pub manifest_present: bool,
    /// Whether the canonical pending-transfer payload body exists.
    pub body_present: bool,
    /// Body length in bytes when metadata inspection succeeded.
    pub body_bytes: Option<u64>,
    /// Whether a body exists without a valid resumable manifest.
    pub abandoned_body: bool,
}
