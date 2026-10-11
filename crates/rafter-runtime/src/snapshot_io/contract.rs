//! Stream budgets, cooperative cancellation, and the verified descriptor receipt.

use rafter::{LogIndex, RaftSnapshot};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// Read limits selected by the embedding before touching a snapshot source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct SnapshotReadOptions {
    pub(super) max_payload_bytes: u64,
    pub(super) chunk_bytes: u32,
}

impl SnapshotReadOptions {
    /// Allows at most `max_payload_bytes` total, reading at most 64 KiB at once.
    /// Zero total bytes permits only an empty payload.
    #[must_use]
    pub const fn new(max_payload_bytes: u64) -> Self {
        Self {
            max_payload_bytes,
            chunk_bytes: 64 * 1024,
        }
    }

    /// Sets the maximum source chunk and internal retained buffer size.
    /// Zero is refused when the reader is constructed.
    #[must_use]
    pub const fn with_chunk_bytes(mut self, chunk_bytes: u32) -> Self {
        self.chunk_bytes = chunk_bytes;
        self
    }
}

/// A cloneable, monotonic cancellation signal for snapshot payload work.
///
/// Cancellation is checked before and after source calls and before accepting
/// completion. It cannot interrupt a blocking source call or undo publication
/// that the application has already started. A verified receipt is not revoked
/// by a later cancellation; the application owns its publication decision.
#[derive(Clone, Debug, Default)]
pub struct SnapshotCancellation(Arc<AtomicBool>);

impl SnapshotCancellation {
    /// Creates an uncancelled signal. Each snapshot attempt should use a new one.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Permanently requests cancellation of work using this signal.
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    /// Returns whether cancellation has been requested.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

/// Proof that a reader consumed and checked the complete descriptor-bound payload.
///
/// Only successful reader completion constructs this receipt. It proves byte
/// length, checksum, and the constructor's non-regressing applied floor; it
/// does not prove application schema validity, atomic installation, or fsync.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct VerifiedSnapshotPayload {
    pub(super) snapshot: RaftSnapshot,
}

impl VerifiedSnapshotPayload {
    /// Returns the exact descriptor whose payload was consumed and checked.
    #[must_use]
    pub const fn snapshot(&self) -> &RaftSnapshot {
        &self.snapshot
    }

    /// Returns the floor to publish atomically with the installed application state.
    #[must_use]
    pub const fn applied_index(&self) -> LogIndex {
        self.snapshot.metadata.last_included_index
    }
}
