#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct PendingTransferManifest {
    pub(super) leader_id: rafter::NodeId,
    pub(super) transfer_id: rafter::SnapshotTransferId,
    pub(super) metadata: rafter::RaftSnapshotMetadata,
    pub(super) total_payload_len: u64,
    pub(super) application_payload_crc32: u32,
    pub(super) received_payload_len: u64,
    pub(super) body_checksum: u32,
}
