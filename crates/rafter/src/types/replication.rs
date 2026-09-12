//! Public leader-side replication progress vocabulary.

use super::{LogIndex, NodeId};

/// Leader replication progress for one follower.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicationProgress {
    /// Follower whose progress is described.
    pub follower_id: NodeId,
    /// Greatest index known replicated on the follower.
    pub match_index: LogIndex,
    /// Next log index the leader will attempt to send.
    pub next_index: LogIndex,
    /// Current leader-side send discipline.
    pub state: ReplicationState,
}

/// Leader-side bounded in-flight replication-window usage for one follower.
///
/// This is a read-only observation. It reports batches sent optimistically but
/// not yet acknowledged; it does not grant authority to mutate replication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ReplicationWindowProgress {
    /// Follower whose in-flight window is described.
    pub follower_id: NodeId,
    /// Append batches currently awaiting acknowledgement.
    pub in_flight_batches: usize,
    /// Encoded entry bytes currently awaiting acknowledgement.
    pub in_flight_bytes: usize,
    /// Configured upper bound on in-flight append batches.
    pub max_in_flight_batches: usize,
    /// Configured upper bound on in-flight encoded entry bytes.
    pub max_in_flight_bytes: usize,
    /// Whether the window currently refuses another append batch.
    pub full: bool,
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
