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
    AppendEntries(AppendEntries),
    AppendEntriesResponse(AppendEntriesResponse),
    InstallSnapshot(InstallSnapshot),
    InstallSnapshotChunk(InstallSnapshotChunk),
    InstallSnapshotResponse(InstallSnapshotResponse),
    PreVote(PreVote),
    PreVoteResponse(PreVoteResponse),
    TimeoutNow(TimeoutNow),
    RequestVote(RequestVote),
    RequestVoteResponse(RequestVoteResponse),
}
