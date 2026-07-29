//! `AppendEntries` request and response frames.

use crate::{LogIndex, NodeId, Term};

use super::shared_entries::SharedEntries;

/// `AppendEntries` request carrying heartbeats, replication batches, or both.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AppendEntries {
    /// Leader term.
    pub term: Term,
    /// Node sending the request as leader.
    pub leader_id: NodeId,
    /// Log index immediately before the replicated batch.
    pub prev_log_index: LogIndex,
    /// Term stored at `prev_log_index`.
    pub prev_log_term: Term,
    /// Heartbeat round this append belongs to; echoed by the response so
    /// leaders can order acknowledgements against read-index registrations.
    /// Zero means unknown and by construction never satisfies a read barrier.
    pub sequence: u64,
    /// Contiguous entries following `prev_log_index`.
    pub entries: SharedEntries,
    /// Greatest index the leader knows committed.
    pub leader_commit: LogIndex,
}

/// Response to an [`AppendEntries`] request.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct AppendEntriesResponse {
    /// Current follower term.
    pub term: Term,
    /// Node sending the response.
    pub follower_id: NodeId,
    /// Whether the follower matched the preceding log position and appended.
    pub success: bool,
    /// Greatest log index confirmed by this response.
    pub match_index: LogIndex,
    /// Echo of the request's heartbeat round; zero is an unknown sequence echo.
    pub sequence: u64,
}
