//! Correlation and hashing for terminal read-index outputs.
//!
//! Rejection and cancellation reasons come from the kernel without `Hash`, so
//! this module writes their exact discriminants by hand: two terminal outputs
//! hash equally exactly when they carry the same reason.

use std::hash::{Hash, Hasher};

use rafter::{ReadIndexCancelReason, ReadIndexRejection};

use super::ReadTerminalOutput;

impl ReadTerminalOutput {
    pub(crate) fn matches_operation(self, operation_id: u64) -> bool {
        match self {
            Self::Rejected {
                operation_id: recorded,
                ..
            }
            | Self::Canceled {
                operation_id: recorded,
                ..
            } => recorded == Some(operation_id),
        }
    }

    pub(crate) const fn operation_id(self) -> Option<u64> {
        match self {
            Self::Rejected { operation_id, .. } | Self::Canceled { operation_id, .. } => {
                operation_id
            }
        }
    }
}

impl Hash for ReadTerminalOutput {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Self::Rejected {
                node_id,
                operation_id,
                request_id,
                reason,
            } => {
                0_u8.hash(state);
                node_id.hash(state);
                operation_id.hash(state);
                request_id.hash(state);
                hash_read_rejection(*reason, state);
            }
            Self::Canceled {
                node_id,
                operation_id,
                request_id,
                reason,
            } => {
                1_u8.hash(state);
                node_id.hash(state);
                operation_id.hash(state);
                request_id.hash(state);
                hash_read_cancellation(*reason, state);
            }
        }
    }
}

fn hash_read_rejection<H: Hasher>(reason: ReadIndexRejection, state: &mut H) {
    match reason {
        ReadIndexRejection::NotLeader { role, term } => {
            0_u8.hash(state);
            role.hash(state);
            term.hash(state);
        }
        ReadIndexRejection::NoCommitInCurrentTerm => 1_u8.hash(state),
        ReadIndexRejection::LeadershipTransferInProgress { target } => {
            2_u8.hash(state);
            target.hash(state);
        }
        ReadIndexRejection::TooManyPendingReads => 3_u8.hash(state),
    }
}

fn hash_read_cancellation<H: Hasher>(reason: ReadIndexCancelReason, state: &mut H) {
    match reason {
        ReadIndexCancelReason::LeadershipLost => 0_u8.hash(state),
        ReadIndexCancelReason::LeaderStateReset => 1_u8.hash(state),
        ReadIndexCancelReason::LeadershipTransfer { target } => {
            2_u8.hash(state);
            target.hash(state);
        }
    }
}
