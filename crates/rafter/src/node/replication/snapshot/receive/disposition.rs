//! Classification and establishment of inbound snapshot transfer state.

use std::cmp::Ordering;

use crate::{InstallSnapshotChunk, LogIndex, NodeId};

use super::super::super::super::{state::IncomingSnapshotTransfer, Node};
use super::super::validate::SnapshotChunkRejection;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ChunkDisposition {
    AlreadyCovered { covered_through: LogIndex },
    Retransmission { next_offset: u64 },
    OutOfOrder { expected_offset: u64 },
    Accept { received_len: u64 },
}

impl Node {
    pub(super) fn classify_snapshot_chunk(
        &mut self,
        leader_id: NodeId,
        request: &InstallSnapshotChunk,
    ) -> Result<ChunkDisposition, SnapshotChunkRejection> {
        let covered_through = self.snapshot_covered_through();
        if request.metadata.last_included_index <= covered_through {
            return Ok(ChunkDisposition::AlreadyCovered { covered_through });
        }

        let expected_offset = self.prepare_incoming_snapshot_transfer(leader_id, request)?;
        Ok(match request.offset.cmp(&expected_offset) {
            Ordering::Less => ChunkDisposition::Retransmission {
                next_offset: expected_offset,
            },
            Ordering::Greater => ChunkDisposition::OutOfOrder { expected_offset },
            Ordering::Equal => ChunkDisposition::Accept {
                received_len: expected_offset,
            },
        })
    }

    fn prepare_incoming_snapshot_transfer(
        &mut self,
        leader_id: NodeId,
        request: &InstallSnapshotChunk,
    ) -> Result<u64, SnapshotChunkRejection> {
        let current_matches = self
            .volatile
            .incoming_snapshot
            .as_ref()
            .is_some_and(|transfer| {
                transfer.leader_id == leader_id
                    && transfer.transfer_id == request.transfer_id
                    && transfer.metadata == request.metadata
                    && transfer.total_payload_len == request.total_payload_len
                    && transfer.application_payload_crc32 == request.application_payload_crc32
            });

        if !current_matches {
            self.replace_or_reject_snapshot_transfer(leader_id, request)?;
        }

        self.volatile
            .incoming_snapshot
            .as_ref()
            .map(IncomingSnapshotTransfer::next_offset)
            .ok_or(SnapshotChunkRejection::WrongTransfer)
    }

    fn replace_or_reject_snapshot_transfer(
        &mut self,
        leader_id: NodeId,
        request: &InstallSnapshotChunk,
    ) -> Result<(), SnapshotChunkRejection> {
        if request.offset == 0 {
            self.volatile.incoming_snapshot = Some(IncomingSnapshotTransfer::new(
                leader_id,
                request.transfer_id,
                request.metadata.clone(),
                request.total_payload_len,
                request.application_payload_crc32,
            ));
            return Ok(());
        }

        let Some(transfer) = self.volatile.incoming_snapshot.as_ref() else {
            return Err(SnapshotChunkRejection::OutOfOrderOffset);
        };
        if transfer.leader_id != leader_id || transfer.transfer_id != request.transfer_id {
            Err(SnapshotChunkRejection::WrongTransfer)
        } else {
            Err(SnapshotChunkRejection::MetadataMismatch)
        }
    }
}
