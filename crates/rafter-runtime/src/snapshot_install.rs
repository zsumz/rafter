//! One recoverable ordering for live and interrupted snapshot installation.

use rafter::{LogIndex, RaftSnapshot};
use rafter_storage::{RaftLogSegment, RaftSnapshotStore};

use crate::RaftRuntimeError;

/// Installs an already complete staged transfer without losing the evidence
/// needed to decide whether the durable suffix belongs to its history.
///
/// A matching boundary preserves the suffix. Otherwise remove the boundary
/// entry AND its suffix before promotion: a crash still leaves the complete
/// staging, so recovery repeats this operation. Once promoted, the log either
/// has a matching boundary or ends below it; ordinary hydration can validate
/// the former and finish compaction for the latter. Only then may compaction
/// erase the boundary evidence. No new on-disk intent or format is needed.
pub(crate) fn install_staged_snapshot<L: RaftLogSegment, S: RaftSnapshotStore>(
    log_segment: &mut L,
    snapshot_store: &mut S,
    snapshot: &RaftSnapshot,
    commit_floor: LogIndex,
) -> Result<(), RaftRuntimeError> {
    let boundary = snapshot.metadata.last_included_index;
    let boundary_entry = log_segment
        .replay_entries()
        .into_iter()
        .find(|entry| entry.index == boundary);
    if boundary_entry.is_some_and(|entry| entry.term != snapshot.metadata.last_included_term) {
        if boundary <= commit_floor {
            return Err(RaftRuntimeError::LogPrefixDiverged { index: boundary });
        }
        log_segment
            .truncate_suffix(boundary)
            .map_err(RaftRuntimeError::LogTruncate)?;
    }
    snapshot_store
        .promote_staged_snapshot(snapshot)
        .map_err(RaftRuntimeError::SnapshotWrite)?;
    log_segment
        .compact_prefix_through(boundary)
        .map_err(RaftRuntimeError::LogCompact)
}
