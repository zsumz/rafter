//! `AppendEntries` request and response frames.

use crate::{LogIndex, NodeId, Term};

use super::shared_entries::SharedEntries;

/// `AppendEntries` request carrying heartbeats, replication batches, or both.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct AppendEntries {
    pub term: Term,
    pub leader_id: NodeId,
    pub prev_log_index: LogIndex,
    pub prev_log_term: Term,
    /// Heartbeat round this append belongs to; echoed by the response so
    /// leaders can order acknowledgements against read-index registrations.
    /// Zero means unknown and by construction never satisfies a read barrier.
    pub sequence: u64,
    pub entries: SharedEntries,
    pub leader_commit: LogIndex,
}

/// Response to an [`AppendEntries`] request.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct AppendEntriesResponse {
    pub term: Term,
    pub follower_id: NodeId,
    pub success: bool,
    pub match_index: LogIndex,
    /// Echo of the request's heartbeat round; zero is an unknown sequence echo.
    pub sequence: u64,
}
