//! Closed Raft peer-message vocabulary.
//!
//! The public facade groups messages by protocol purpose while keeping the
//! crate-level API flat. Message values contain protocol data only; transports
//! and codecs remain outside the deterministic kernel.

mod append;
mod election;
mod entry;
mod shared_entries;
mod snapshot;

#[cfg(test)]
mod shared_entries_test;

pub use append::{AppendEntries, AppendEntriesResponse};
pub use election::{PreVote, PreVoteResponse, RequestVote, RequestVoteResponse, TimeoutNow};
pub use entry::LogEntry;
pub use shared_entries::SharedEntries;
pub use snapshot::{InstallSnapshot, InstallSnapshotChunk, InstallSnapshotResponse};

/// Raft protocol message exchanged between nodes.
///
/// This enum is exhaustive because the protocol message vocabulary is closed
/// over these request and response frames.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Message {
    /// Heartbeat or log-replication request.
    AppendEntries(AppendEntries),
    /// Follower acknowledgement or rejection of an append request.
    AppendEntriesResponse(AppendEntriesResponse),
    /// Complete in-memory snapshot installation request.
    InstallSnapshot(InstallSnapshot),
    /// Streaming snapshot installation chunk.
    InstallSnapshotChunk(InstallSnapshotChunk),
    /// Follower snapshot-install progress response.
    InstallSnapshotResponse(InstallSnapshotResponse),
    /// Non-binding poll before a real election.
    PreVote(PreVote),
    /// Response to a pre-vote poll.
    PreVoteResponse(PreVoteResponse),
    /// Leadership-transfer instruction to begin an election immediately.
    TimeoutNow(TimeoutNow),
    /// Binding election vote request.
    RequestVote(RequestVote),
    /// Response to a binding vote request.
    RequestVoteResponse(RequestVoteResponse),
}
