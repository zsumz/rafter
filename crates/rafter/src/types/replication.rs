//! Public leader-side replication progress vocabulary.

use super::{LogIndex, NodeId};

/// Leader replication progress for one follower.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicationProgress {
    pub follower_id: NodeId,
    pub match_index: LogIndex,
    pub next_index: LogIndex,
    pub state: ReplicationState,
}

/// The send discipline a leader currently applies to one follower.
///
/// This enum is exhaustive because replication progress has a closed set of
/// protocol states.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReplicationState {
    /// One bounded append at a time until the follower confirms its log
    /// position (leadership start, or after a rejection).
    Probing,
    /// Confirmed position: appends fill the in-flight window and the send
    /// index advances optimistically.
    Replicating,
    /// The follower is behind the snapshot boundary; the current snapshot is
    /// streaming to it and log replication is paused.
    Snapshotting {
        /// The next payload byte offset to send.
        next_offset: u64,
    },
}
