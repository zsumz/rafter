//! Follower-side construction of accepted and rejected snapshot responses.

use crate::{InstallSnapshotResponse, LogIndex, Message, NodeId, SnapshotTransferId, Term};

use crate::node::{Node, Output};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum SnapshotReply {
    Accepted(SnapshotProgress),
    Rejected(SnapshotProgress),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SnapshotProgress {
    last_included_index: LogIndex,
    transfer_id: Option<SnapshotTransferId>,
    next_offset: u64,
}

impl SnapshotReply {
    pub(super) const fn accepted_transfer(
        last_included_index: LogIndex,
        transfer_id: SnapshotTransferId,
        next_offset: u64,
    ) -> Self {
        Self::Accepted(SnapshotProgress::transfer(
            last_included_index,
            transfer_id,
            next_offset,
        ))
    }

    pub(super) const fn rejected_current(last_included_index: LogIndex) -> Self {
        Self::Rejected(SnapshotProgress::current(last_included_index))
    }

    pub(super) const fn rejected_transfer(
        last_included_index: LogIndex,
        transfer_id: SnapshotTransferId,
        next_offset: u64,
    ) -> Self {
        Self::Rejected(SnapshotProgress::transfer(
            last_included_index,
            transfer_id,
            next_offset,
        ))
    }

    fn into_response(self, term: Term, follower_id: NodeId) -> InstallSnapshotResponse {
        let (success, progress) = match self {
            Self::Accepted(progress) => (true, progress),
            Self::Rejected(progress) => (false, progress),
        };
        InstallSnapshotResponse {
            term,
            follower_id,
            success,
            last_included_index: progress.last_included_index,
            transfer_id: progress.transfer_id,
            next_offset: progress.next_offset,
        }
    }
}

impl SnapshotProgress {
    const fn current(last_included_index: LogIndex) -> Self {
        Self {
            last_included_index,
            transfer_id: None,
            next_offset: 0,
        }
    }

    const fn transfer(
        last_included_index: LogIndex,
        transfer_id: SnapshotTransferId,
        next_offset: u64,
    ) -> Self {
        Self {
            last_included_index,
            transfer_id: Some(transfer_id),
            next_offset,
        }
    }
}

impl Node {
    pub(super) fn snapshot_reply(&self, leader_id: NodeId, reply: SnapshotReply) -> Output {
        Output::Send {
            to: leader_id,
            message: Message::InstallSnapshotResponse(
                reply.into_response(self.current_term(), self.id()),
            ),
        }
    }
}
