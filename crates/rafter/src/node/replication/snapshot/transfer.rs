//! Durable recovery and validation of partial snapshot transfers.

use std::{error::Error, fmt};

use crate::{
    types::snapshot_transfer_id_from_parts, LogIndex, NodeId, PendingSnapshotTransfer,
    SnapshotTransferId,
};

use super::super::super::{state::IncomingSnapshotTransfer, Node};
use super::validate::SnapshotTransferHeaderRejection;

impl Node {
    /// Returns the durable shape of an incomplete inbound snapshot transfer.
    #[must_use]
    pub fn pending_snapshot_transfer(&self) -> Option<PendingSnapshotTransfer> {
        self.volatile
            .incoming_snapshot
            .as_ref()
            .map(IncomingSnapshotTransfer::to_pending)
    }

    /// Rehydrates a durable, partially received snapshot transfer.
    ///
    /// The transfer is not installed. It only restores the follower-side
    /// received byte count so the leader can resume from the acknowledged
    /// offset after process restart; the staged byte prefix itself lives in
    /// the application's snapshot store.
    ///
    /// # Errors
    ///
    /// Returns [`PendingSnapshotTransferResumeError`] when the transfer does
    /// not belong to an authorized leader, is stale behind the current
    /// snapshot, or has impossible offsets.
    pub fn resume_pending_snapshot_transfer(
        &mut self,
        transfer: PendingSnapshotTransfer,
    ) -> Result<(), PendingSnapshotTransferResumeError> {
        let received_bytes = transfer.received_bytes();
        if received_bytes > transfer.total_payload_len {
            return Err(PendingSnapshotTransferResumeError::ReceivedPayloadTooLong {
                received_bytes,
                total_payload_len: transfer.total_payload_len,
            });
        }
        let expected_transfer_id = snapshot_transfer_id_from_parts(
            &transfer.metadata,
            transfer.total_payload_len,
            transfer.application_payload_crc32,
        );
        if transfer.transfer_id != expected_transfer_id {
            return Err(PendingSnapshotTransferResumeError::TransferIdMismatch {
                expected: expected_transfer_id,
                actual: transfer.transfer_id,
            });
        }
        if let Err(rejection) = self.validate_snapshot_transfer_header(
            transfer.leader_id,
            &transfer.metadata,
            self.current_term(),
        ) {
            return Err(match rejection {
                SnapshotTransferHeaderRejection::InvalidMetadata => {
                    PendingSnapshotTransferResumeError::InvalidMetadata
                }
                SnapshotTransferHeaderRejection::LeaderNotAuthorized => {
                    PendingSnapshotTransferResumeError::LeaderNotAuthorized {
                        leader_id: transfer.leader_id,
                    }
                }
            });
        }
        if transfer.metadata.last_included_index <= self.snapshot_index() {
            return Err(PendingSnapshotTransferResumeError::StaleSnapshot {
                snapshot_index: self.snapshot_index(),
                transfer_last_included_index: transfer.metadata.last_included_index,
            });
        }
        if transfer.is_complete() {
            return Err(
                PendingSnapshotTransferResumeError::CompleteTransferNotInstalled {
                    last_included_index: transfer.metadata.last_included_index,
                },
            );
        }

        self.volatile.incoming_snapshot = Some(IncomingSnapshotTransfer::from_pending(transfer));
        Ok(())
    }
}

/// Error returned while resuming a pending inbound snapshot transfer.
///
/// This enum is exhaustive because resume validation is closed over these
/// snapshot identity, authorization, and length checks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingSnapshotTransferResumeError {
    LeaderNotAuthorized {
        leader_id: NodeId,
    },
    ReceivedPayloadTooLong {
        received_bytes: u64,
        total_payload_len: u64,
    },
    TransferIdMismatch {
        expected: SnapshotTransferId,
        actual: SnapshotTransferId,
    },
    InvalidMetadata,
    StaleSnapshot {
        snapshot_index: LogIndex,
        transfer_last_included_index: LogIndex,
    },
    CompleteTransferNotInstalled {
        last_included_index: LogIndex,
    },
}

impl fmt::Display for PendingSnapshotTransferResumeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LeaderNotAuthorized { leader_id } => write!(
                formatter,
                concat!(
                    "pending snapshot transfer leader {leader_id} is not authorized by ",
                    "the snapshot membership"
                ),
                leader_id = leader_id,
            ),
            Self::ReceivedPayloadTooLong {
                received_bytes,
                total_payload_len,
            } => write!(
                formatter,
                concat!(
                    "pending snapshot transfer received {received_bytes} bytes, more than ",
                    "the total payload length {total_payload_len}"
                ),
                received_bytes = received_bytes,
                total_payload_len = total_payload_len,
            ),
            Self::TransferIdMismatch { expected, actual } => write!(
                formatter,
                concat!(
                    "pending snapshot transfer id {actual} does not match id {expected} ",
                    "derived from its metadata"
                ),
                actual = actual,
                expected = expected,
            ),
            Self::InvalidMetadata => {
                formatter.write_str("pending snapshot transfer metadata is not valid for this node")
            }
            Self::StaleSnapshot {
                snapshot_index,
                transfer_last_included_index,
            } => write!(
                formatter,
                concat!(
                    "pending snapshot transfer through index {transfer_last_included_index} ",
                    "is stale behind current snapshot index {snapshot_index}"
                ),
                transfer_last_included_index = transfer_last_included_index,
                snapshot_index = snapshot_index,
            ),
            Self::CompleteTransferNotInstalled {
                last_included_index,
            } => write!(
                formatter,
                concat!(
                    "pending snapshot transfer through index {last_included_index} is ",
                    "complete and must be installed, not resumed"
                ),
                last_included_index = last_included_index,
            ),
        }
    }
}

impl Error for PendingSnapshotTransferResumeError {}
