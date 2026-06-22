/// File-level status of pending snapshot-transfer staging data.
///
/// This is diagnostic state; it may report abandoned files that are not a
/// resumable [`rafter::PendingSnapshotTransfer`].
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct PendingSnapshotTransferStagingStatus {
    pub manifest_present: bool,
    pub body_present: bool,
    pub body_bytes: Option<u64>,
    pub abandoned_body: bool,
}
