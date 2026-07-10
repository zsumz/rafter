use rafter::{LogIndex, Node as RaftNode, Term};
use rafter_storage::RaftLogSegment;

use crate::RaftRuntimeError;

/// The persisted log's tail as of the last successful persist: the runtime
/// is the segment's only writer, so this is exact, and by the Log Matching
/// Property a kernel that still holds this exact (index, term) entry has an
/// unchanged log through it — repairing then needs no scan at all.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct PersistedTail {
    pub index: LogIndex,
    pub term: Term,
}

impl PersistedTail {
    /// The tail matching `node`'s current log end; index zero (or the
    /// snapshot boundary) is always "matched", so a fresh or fully
    /// compacted log takes the fast path too.
    pub fn of_node(node: &RaftNode) -> Self {
        let index = node.last_log_index();
        Self {
            index,
            term: node.term_at_index(index).unwrap_or_default(),
        }
    }

    fn still_matches(self, node: &RaftNode) -> bool {
        node.term_at_index(self.index) == Some(self.term)
    }
}

/// Truncates any persisted suffix the kernel has since rewritten.
///
/// Fast path, the common case: the kernel still holds the persisted tail
/// entry, so the whole persisted prefix is unchanged (Log Matching) and
/// nothing needs repair. The full divergence scan runs only when the tail
/// no longer matches — a conflicting splice or a snapshot replaced it.
pub(crate) fn repair_persisted_log_suffix<L: RaftLogSegment>(
    log_segment: &mut L,
    node: &RaftNode,
    persisted_tail: Option<PersistedTail>,
    commit_floor: LogIndex,
) -> Result<(), RaftRuntimeError> {
    if persisted_tail.is_some_and(|tail| tail.still_matches(node)) {
        return Ok(());
    }
    if let Some(index) = first_persisted_divergence(log_segment, node) {
        // The fatal-divergence floor is the commit index as of the LAST
        // PERSIST, not the batch that just stepped: a catch-up append may
        // legitimately splice out a persisted uncommitted suffix and
        // advance the commit past it in the same batch. Entries committed
        // before this batch changing durably is real corruption.
        if index <= commit_floor {
            return Err(RaftRuntimeError::LogPrefixDiverged { index });
        }
        log_segment
            .truncate_suffix(index)
            .map_err(RaftRuntimeError::LogTruncate)?;
    }
    Ok(())
}

fn first_persisted_divergence<L: RaftLogSegment>(
    log_segment: &L,
    node: &RaftNode,
) -> Option<LogIndex> {
    log_segment
        .replay_entries()
        .into_iter()
        .filter(|persisted| persisted.index > node.snapshot_index())
        .find_map(|persisted| {
            let node_entry = node.log_entries_slice_from(persisted.index).first();
            match node_entry {
                Some(entry) if entry.term == persisted.term && entry.kind == persisted.kind => None,
                _ => Some(persisted.index),
            }
        })
}
