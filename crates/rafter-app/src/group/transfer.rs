//! Leadership-transfer helpers for the group driver.

use super::{LeadershipTransferRejection, NodeId};

/// Leadership-transfer lifecycle events surfaced by the app layer.
///
/// This lifecycle stream is `#[non_exhaustive]`: the app layer may gain another
/// observable transfer state, and report routers can preserve unknown events
/// without treating them as a rejection.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LeadershipTransferEvent {
    /// The local leader accepted a transfer request.
    Started {
        /// Intended successor.
        target: NodeId,
    },
    /// The local node proved the transfer did not start.
    Rejected {
        /// Requested successor.
        target: NodeId,
        /// Protocol refusal reason.
        reason: LeadershipTransferRejection,
        /// Best-effort leader identity observed with the refusal.
        leader_hint: Option<NodeId>,
    },
}
