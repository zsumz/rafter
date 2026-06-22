pub(super) const PENDING_SNAPSHOT_TRANSFER_MANIFEST_MAGIC: [u8; 4] = *b"RFPT";
pub(super) const PENDING_SNAPSHOT_TRANSFER_MANIFEST_VERSION: u8 = 1;
pub(super) const PENDING_SNAPSHOT_TRANSFER_MANIFEST_CHECKSUM_LEN: usize = 4;
pub(super) const PENDING_SNAPSHOT_TRANSFER_PATH: &str = "pending.snapshot-transfer";
pub(super) const PENDING_SNAPSHOT_TRANSFER_BODY_PATH: &str = "pending.snapshot-transfer.body";
