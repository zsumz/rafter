//! Public contract for durable retained-log mutation and replay.
//!
//! The contract names logical append, suffix truncation, prefix compaction, and
//! recovery observability. Concrete publication and replay mechanics belong to
//! the file-backed and in-memory implementations.

use rafter::LogIndex;

use crate::{BorrowedPersistedRaftLogEntry, PersistedRaftLogEntry};

use super::{RaftLogSegmentAppendError, RaftLogSegmentCompactError, RaftLogSegmentTruncateError};

/// Durable append-only Raft log segment with suffix truncation and prefix
/// compaction.
///
/// Implementations must make successful mutations durable before returning.
/// A file-backed implementation may fail after accepting some or all bytes; the
/// reference handle then rejects every later mutation until it is reopened.
///
/// # The maximum log index
///
/// No implementation may store an entry at `u64::MAX` or compact through it.
/// [`RaftLogSegment::next_index`] is the stored index plus one and every
/// retained suffix starts at `compacted_through.next()`, so either would ask for
/// a successor of `u64::MAX`: a panic in debug builds, and in release builds a
/// wrap to index zero — the sentinel meaning "before the first entry" — which is
/// durable corruption of the segment's whole index space.
///
/// This is a rule on the trait, not on any one implementation. It is not
/// inherited from a codec: a segment that stores nothing on disk encodes
/// nothing, so it must apply the bound itself. `u64::MAX - 1` is ordinary state
/// on both sides — a segment may legitimately report
/// `next_index() == LogIndex(u64::MAX)`, and it is the append into that slot
/// that is refused.
///
/// Both shipped implementations enforce it, and
/// `tests/log_boundary_bounds.rs` exercises each of them through this trait.
/// Those tests are written to hold in debug and release alike, because release
/// is where this overflow wraps silently instead of panicking. Nothing
/// mechanically holds a *third* implementation to the rule; it is stated here so
/// an implementer is told, and reviewed like any other trait requirement.
pub trait RaftLogSegment {
    /// Appends persisted Raft log entries to the segment.
    ///
    /// # Errors
    ///
    /// Returns [`RaftLogSegmentAppendError::NonContiguous`] when the batch does
    /// not start at the segment's next expected log index. Every implementation
    /// returns [`RaftLogSegmentAppendError::IndexAtMaximum`] for a batch naming
    /// `u64::MAX`, per the maximum-index rule on this trait. A file-backed
    /// handle returns [`RaftLogSegmentAppendError::StoreRequiresReopen`] after
    /// an earlier mutating I/O failure.
    fn append_entries(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), RaftLogSegmentAppendError>;

    /// Appends borrowed persisted Raft log entries to the segment.
    ///
    /// This lets durable runtimes stamp log indexes onto borrowed kernel
    /// entries without first materializing a separate persisted-entry batch.
    ///
    /// # Errors
    ///
    /// The same errors as [`RaftLogSegment::append_entries`], which the default
    /// body delegates to.
    fn append_entries_borrowed<'a, I>(
        &mut self,
        entries: I,
    ) -> Result<(), RaftLogSegmentAppendError>
    where
        Self: Sized,
        I: IntoIterator<Item = BorrowedPersistedRaftLogEntry<'a>>,
    {
        let entries = entries
            .into_iter()
            .map(PersistedRaftLogEntry::from)
            .collect::<Vec<_>>();
        self.append_entries(&entries)
    }

    /// Removes persisted entries at `from_index` and later.
    ///
    /// # Errors
    ///
    /// Returns [`RaftLogSegmentTruncateError::OutOfBounds`] when `from_index`
    /// is greater than the segment's next expected log index. Returns
    /// [`RaftLogSegmentTruncateError::BeforeCompactedPrefix`] when the request
    /// would erase through the already-compacted durable snapshot boundary. A
    /// file-backed handle returns
    /// [`RaftLogSegmentTruncateError::StoreRequiresReopen`] after an earlier
    /// mutating I/O failure.
    fn truncate_suffix(&mut self, from_index: LogIndex) -> Result<(), RaftLogSegmentTruncateError>;

    /// Removes persisted entries through `through_index`.
    ///
    /// This may advance the compacted prefix beyond the local tail when a
    /// follower installs a leader snapshot that replaces missing local log.
    /// The file-backed implementation publishes the compacted-prefix marker
    /// before reclaiming old frame bytes. If reclamation fails after that
    /// commit point, it returns
    /// [`RaftLogSegmentCompactError::CompactedButReclamationFailed`].
    ///
    /// # Errors
    ///
    /// Returns [`RaftLogSegmentCompactError`] when encoding, marker publication,
    /// or physical reclamation fails. A file-backed handle returns
    /// [`RaftLogSegmentCompactError::StoreRequiresReopen`] after an earlier
    /// mutating I/O failure. Every implementation returns
    /// [`RaftLogSegmentCompactError::ThroughIndexAtMaximum`] for a boundary of
    /// `u64::MAX`, which leaves no index for the retained suffix to start at,
    /// per the maximum-index rule on this trait.
    fn compact_prefix_through(
        &mut self,
        through_index: LogIndex,
    ) -> Result<(), RaftLogSegmentCompactError>;

    /// Returns every retained persisted entry in ascending log-index order.
    fn replay_entries(&self) -> Vec<PersistedRaftLogEntry>;

    /// Returns the next log index that would be assigned by a contiguous
    /// append.
    fn next_index(&self) -> LogIndex;

    /// The durable compacted-prefix boundary: entries at or below this index
    /// are covered by a snapshot and are no longer stored. When this returns
    /// Ok, a crash immediately after preserves this boundary.
    fn compacted_through(&self) -> LogIndex;
}
