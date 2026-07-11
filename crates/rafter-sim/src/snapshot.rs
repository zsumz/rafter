use rafter::{NodeId, RaftSnapshot, StagedSnapshotChunk};

use crate::{records::StagedSnapshotTransfer, Cluster};

impl Cluster {
    /// Registers `payload` as `snapshot`'s content in `node_id`'s store.
    ///
    /// # Panics
    ///
    /// Panics when the payload length does not match the descriptor.
    pub fn seed_snapshot_payload(
        &mut self,
        node_id: NodeId,
        snapshot: &RaftSnapshot,
        payload: Vec<u8>,
    ) {
        assert!(
            self.configs.contains_key(&node_id),
            "simulated node {node_id} must exist in cluster"
        );
        self.snapshot_sources
            .entry(node_id)
            .or_default()
            .insert(snapshot, payload)
            .expect("seeded snapshot payload must match its descriptor length");
    }

    /// Returns the payload bytes `node_id`'s snapshot store holds for
    /// `snapshot`, if any.
    #[must_use]
    pub fn snapshot_payload(&self, node_id: NodeId, snapshot: &RaftSnapshot) -> Option<&[u8]> {
        self.snapshot_sources
            .get(&node_id)?
            .payload(snapshot.transfer_id())
    }

    /// Appends one validated inbound chunk to `node_id`'s staging area,
    /// enforcing the kernel's staging contract: chunks arrive strictly in
    /// offset order within one transfer, and offset zero begins or replaces
    /// the staged transfer.
    pub(crate) fn stage_snapshot_chunk(&mut self, node_id: NodeId, chunk: StagedSnapshotChunk) {
        let StagedSnapshotChunk {
            leader_id,
            transfer_id,
            metadata,
            total_payload_len,
            application_payload_crc32,
            offset,
            bytes,
            done,
        } = chunk;

        if offset == 0 {
            self.snapshot_staging.insert(
                node_id,
                StagedSnapshotTransfer {
                    leader_id,
                    transfer_id,
                    metadata,
                    total_payload_len,
                    application_payload_crc32,
                    bytes,
                },
            );
        } else {
            let staged = self.snapshot_staging.get_mut(&node_id).unwrap_or_else(|| {
                panic!(
                    "kernel staging contract breach: {node_id} staged a chunk of transfer \
                     {transfer_id} at offset {offset} with no transfer in progress"
                )
            });
            assert!(
                staged.leader_id == leader_id
                    && staged.transfer_id == transfer_id
                    && staged.metadata == metadata
                    && staged.total_payload_len == total_payload_len
                    && staged.application_payload_crc32 == application_payload_crc32,
                "kernel staging contract breach: {node_id} staged a chunk of transfer \
                 {transfer_id} from {leader_id} while transfer {} from {} is in progress",
                staged.transfer_id,
                staged.leader_id
            );
            assert_eq!(
                offset,
                staged.bytes.len() as u64,
                "kernel staging contract breach: {node_id} staged an out-of-order chunk of \
                 transfer {transfer_id}"
            );
            staged.bytes.extend_from_slice(&bytes);
        }

        if done {
            let staged = &self.snapshot_staging[&node_id];
            assert_eq!(
                staged.bytes.len() as u64,
                staged.total_payload_len,
                "kernel staging contract breach: {node_id} finished transfer {transfer_id} \
                 with an incomplete staged payload"
            );
        }
    }

    /// Promotes the completed staged transfer backing `snapshot` into
    /// `node_id`'s durable snapshot store and returns its bytes.
    ///
    /// Cross-node content invariant: the bytes a follower assembled must be
    /// the bytes the transfer's leader registered for the same descriptor.
    /// The comparison is skipped only when the leader's store no longer
    /// holds that transfer's payload.
    pub(crate) fn take_installed_snapshot_payload(
        &mut self,
        node_id: NodeId,
        snapshot: &RaftSnapshot,
    ) -> Vec<u8> {
        let transfer_id = snapshot.transfer_id();
        let staged = self.snapshot_staging.remove(&node_id).unwrap_or_else(|| {
            panic!(
                "kernel staging contract breach: {node_id} applied snapshot transfer \
                 {transfer_id} with no staged transfer"
            )
        });
        assert_eq!(
            staged.transfer_id, transfer_id,
            "kernel staging contract breach: {node_id} applied snapshot transfer \
             {transfer_id} while transfer {} is staged",
            staged.transfer_id
        );
        assert_eq!(
            staged.bytes.len() as u64,
            snapshot.application_payload_len,
            "kernel staging contract breach: {node_id} applied snapshot transfer \
             {transfer_id} with an incomplete staged payload"
        );
        assert_eq!(
            staged.application_payload_crc32, snapshot.application_payload_crc32,
            "kernel staging contract breach: {node_id} applied snapshot transfer \
             {transfer_id} with a mismatched payload checksum"
        );

        if let Some(expected) = self
            .snapshot_sources
            .get(&staged.leader_id)
            .and_then(|source| source.payload(transfer_id))
        {
            assert!(
                staged.bytes == expected,
                "snapshot content invariant violated: {node_id} installed bytes for transfer \
                 {transfer_id} that differ from the payload leader {} serves for the same \
                 descriptor",
                staged.leader_id
            );
        }

        self.snapshot_sources
            .entry(node_id)
            .or_default()
            .insert(snapshot, staged.bytes.clone())
            .expect("completed staged payload length was validated against the descriptor");
        staged.bytes
    }
}
