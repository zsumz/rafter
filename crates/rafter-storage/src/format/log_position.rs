//! The range rule every decoded log position shares.
//!
//! This module owns the successor bound on `LogIndex` values this crate builds
//! out of bytes it read. It does not own field order, framing, or any codec's
//! typed error vocabulary: each codec maps a rejection here onto its own error.
//!
//! # Coverage rests on review
//!
//! There is no mechanism that makes every decode site route through this
//! function, and this module claims none. `rafter::LogIndex` is a tuple struct
//! with a public field, so `LogIndex(raw)` is always in reach and no newtype
//! here can be the only way to build one. Adding a decoded log position is
//! therefore a change a reviewer has to catch, and the sites that exist are
//! listed below so a reviewer has something to compare against.
//!
//! Bounded here, because each is advanced:
//!
//! - `v1/log_compaction.rs` — the RFLC compacted-prefix marker, which becomes
//!   the retained-suffix floor (`compacted_through.next()`).
//! - `v1/log_entry.rs` — the RFLE entry index, walked by the contiguity check
//!   and by `next_index()`.
//!
//! Deliberately unbounded, because each is compared or stored and never
//! advanced:
//!
//! - `v1/snapshot_metadata.rs` — `last_included_index`, bounded downstream by
//!   `RaftSnapshotMetadata::new`, which rejects `u64::MAX`; and the
//!   committed-configuration index.
//! - `v1/hard_state.rs` — the commit index, the absent-configuration
//!   canonicality check (which must equal `LogIndex::ZERO`), and the
//!   committed-configuration index.
//! - `raft_snapshot_store/inventory/scan.rs` — the index parsed out of a
//!   snapshot file name, used for ordering only.
//!
//! An earlier revision asserted this list with a source-text scan for the token
//! `LogIndex(` containing one of four hard-coded substrings. It was evaded by
//! six of seven natural spellings — binding the raw `u64` first, renaming the
//! cursor, `.parse::<u64>()`, an import alias, a helper function, or manual byte
//! assembly — so it reported clean over genuinely unguarded sites while being
//! cited as proof that none existed. It has been removed rather than extended:
//! a check that cannot fail is worse than an obligation that is written down.

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
/// `tests/log_boundary_bounds.rs` exercises the behaviour of every read path
/// that reaches this bound, in both build profiles. Which sites reach it is a
/// review obligation; see this module's own documentation.
pub(crate) fn advanceable_log_index(raw: u64) -> Option<LogIndex> {
    (raw != u64::MAX).then_some(LogIndex(raw))
}
