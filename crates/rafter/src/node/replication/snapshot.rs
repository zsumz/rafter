use crate::{
    InstallSnapshotResponse, LogIndex, Message, NodeId, RaftSnapshot, RaftSnapshotMetadata,
    SnapshotChunkSend, SnapshotTransferId,
};

mod receive;
mod response;
mod transfer;

pub use transfer::PendingSnapshotTransferResumeError;
pub(super) use transfer::{validate_snapshot_chunk_shape, SnapshotChunkRejection};

use super::super::{Node, Output};

/// Bytes of snapshot payload per chunk directive. Comfortably below the
/// default append budget (`DEFAULT_MAX_APPEND_ENTRIES_BYTES`, 512 KiB), so a
/// transport whose frame limit accommodates the append budget accommodates
/// snapshot chunks too.
const INSTALL_SNAPSHOT_CHUNK_BYTES: u32 = 64 * 1024;

impl Node {
    pub(super) fn install_snapshot_chunk_to(
        &self,
        peer: NodeId,
        snapshot: &RaftSnapshot,
    ) -> Output {
        let total_payload_len = snapshot.application_payload_len;
        let offset = match self
            .leader
            .progress
            .get(peer)
            .map(|progress| &progress.mode)
        {
            Some(super::super::state::ProgressMode::Snapshot { next_offset }) => *next_offset,
            _ => 0,
        }
        .min(total_payload_len);
        let len = match u32::try_from(std::cmp::min(
            u64::from(INSTALL_SNAPSHOT_CHUNK_BYTES),
            total_payload_len - offset,
        )) {
            Ok(len) => len,
            Err(_) => INSTALL_SNAPSHOT_CHUNK_BYTES,
        };
        let done = offset + u64::from(len) == total_payload_len;

        Output::SendSnapshotChunk {
            to: peer,
            chunk: SnapshotChunkSend {
                term: self.current_term(),
                leader_id: self.id(),
                transfer_id: snapshot.transfer_id(),
                metadata: snapshot.metadata.clone(),
                total_payload_len,
                application_payload_crc32: snapshot.application_payload_crc32,
                offset,
                len,
                done,
            },
        }
    }

    fn install_snapshot_response(
        &self,
        leader_id: NodeId,
        success: bool,
        last_included_index: LogIndex,
        transfer_id: Option<SnapshotTransferId>,
        next_offset: u64,
    ) -> Output {
        Output::Send {
            to: leader_id,
            message: Message::InstallSnapshotResponse(InstallSnapshotResponse {
                term: self.current_term(),
                follower_id: self.id(),
                success,
                last_included_index,
                transfer_id,
                next_offset,
            }),
        }
    }

    fn valid_snapshot_metadata(&self, metadata: &RaftSnapshotMetadata, term: crate::Term) -> bool {
        metadata.hard_state_term <= term
            && metadata.committed_membership().map_or_else(
                || {
                    self.config
                        .voters()
                        .any(|voter| voter == metadata.writer_id)
                },
                |membership| membership.contains_voter(metadata.writer_id),
            )
    }

    fn validate_snapshot_transfer_header(
        &self,
        leader_id: NodeId,
        metadata: &RaftSnapshotMetadata,
        term: crate::Term,
    ) -> Result<(), SnapshotTransferHeaderRejection> {
        if !self.valid_snapshot_metadata(metadata, term) {
            return Err(SnapshotTransferHeaderRejection::InvalidMetadata);
        }
        if !self.valid_snapshot_transfer_leader(leader_id, metadata) {
            return Err(SnapshotTransferHeaderRejection::LeaderNotAuthorized);
        }
        Ok(())
    }

    /// Snapshot senders are authorized by the snapshot boundary membership
    /// when it is present. This intentionally rejects older-boundary
    /// snapshots from leaders that joined after that boundary; those leaders
    /// must serve a snapshot whose Raft metadata includes them as voters.
    fn valid_snapshot_transfer_leader(
        &self,
        leader_id: NodeId,
        metadata: &RaftSnapshotMetadata,
    ) -> bool {
        metadata.committed_membership().map_or_else(
            || self.config.is_peer(leader_id),
            |membership| membership.contains_voter(leader_id),
        )
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::node::replication) enum SnapshotTransferHeaderRejection {
    InvalidMetadata,
    LeaderNotAuthorized,
}
