//! Snapshot side-effect events for manual runtimes.

use rafter::{NodeId, RaftSnapshot, SnapshotChunkSend, StagedSnapshotChunk};

/// Snapshot side effects observed by a manual group driver.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum SnapshotEvent<G> {
    Apply {
        group_id: G,
        snapshot: RaftSnapshot,
    },
    StageChunk {
        group_id: G,
        chunk: StagedSnapshotChunk,
    },
    SendChunk {
        group_id: G,
        to: NodeId,
        chunk: SnapshotChunkSend,
    },
}
