//! The range rule every decoded log position shares.
//!
//! This module owns the successor bound on `LogIndex` values this crate builds
//! out of bytes it read. It does not own field order, framing, or any codec's
//! typed error vocabulary: each codec maps a rejection here onto its own error.

use rafter::LogIndex;

/// Converts a raw `u64` into a log position this crate is allowed to advance,
/// returning `None` for the one value that cannot be advanced.
///
/// # Scope
///
/// This is the *successor* bound and nothing else. [`LogIndex::next`] is
/// `LogIndex(self.0 + 1)`, which is total for every input except `u64::MAX`:
/// there it overflows, panicking in debug builds and wrapping to
/// [`LogIndex::ZERO`] in release builds. A wrapped position is durable
/// corruption, because the wrapped value re-enters the log's index space at the
/// sentinel that means "before the first entry".
///
/// Every log position this crate reads from disk is advanced somewhere: the
/// RFLC compacted-prefix marker becomes the retained-suffix floor
/// (`compacted_through.next()`), and each RFLE entry index is walked by the
/// contiguity check and by `next_index()`. So the bound applies to both, and to
/// the caller-supplied compaction boundary that publishes such a marker.
///
/// Deliberately *not* in scope:
///
/// - `LogIndex::ZERO`. Zero is a valid, non-advancing sentinel. It is rejected
///   where it is meaningless — a snapshot boundary, by
///   `RaftSnapshotMetadata::new` — rather than here.
/// - Log positions this crate stores and compares but never advances: the
///   hard state's commit index and both committed-configuration indexes.
/// - `Term`, `NodeId`, and `ConfigurationId`. This crate computes no
///   successor of any of them. `Term` successors are taken by the kernel, so
///   this bound says nothing about them.
///
/// `every_decoded_log_position_is_bounded_or_explicitly_exempt` in
/// `tests/log_boundary_bounds.rs` fails if a decoded log position appears that
/// neither passes through this function nor is listed there with its reason.
pub(crate) fn advanceable_log_index(raw: u64) -> Option<LogIndex> {
    (raw != u64::MAX).then_some(LogIndex(raw))
}
