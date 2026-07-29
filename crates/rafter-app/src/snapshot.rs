//! Snapshot side-effect events for manual runtimes.

use rafter::{NodeId, RaftSnapshot, SnapshotChunkSend, StagedSnapshotChunk};

/// Snapshot side effects observed by a manual group driver.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SnapshotEvent<G> {
    /// Install a completed application snapshot.
    Apply {
        /// Group receiving the snapshot.
        group_id: G,
        /// Validated Raft snapshot whose application state must be installed.
        snapshot: RaftSnapshot,
    },
    /// Persist one bounded chunk of an incoming snapshot transfer.
    StageChunk {
        /// Group receiving the chunk.
        group_id: G,
        /// Chunk and transfer metadata to stage durably.
        chunk: StagedSnapshotChunk,
    },
    /// Route one bounded snapshot chunk to a follower.
    SendChunk {
        /// Group sending the chunk.
        group_id: G,
        /// Destination follower.
        to: NodeId,
        /// Snapshot bytes and transfer metadata to send.
        chunk: SnapshotChunkSend,
    },
}
