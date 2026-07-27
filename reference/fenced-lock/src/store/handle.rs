//! What an open handle reports about itself.
//!
//! Every one of these is a read of state the opening path or the write path
//! already decided. Nothing here reaches the filesystem, and nothing here can
//! fail: a caller that wants to know what a *fresh* opener would find reopens.

use rafter::LogIndex;

use crate::{FencingToken, LockConfig, LockService, ResourceName};

use super::{fault::WriteFault, format::SlotIndex, report::RecoveryReport, Health, LockStore};

impl LockStore {
    /// Returns what opening this store found and did.
    #[must_use]
    pub const fn recovery(&self) -> &RecoveryReport {
        &self.recovery
    }

    /// Returns the resource bounds this store's slots were written under.
    #[must_use]
    pub const fn config(&self) -> LockConfig {
        self.config
    }

    /// Returns the durable lock service state.
    #[must_use]
    pub const fn service(&self) -> &LockService {
        &self.service
    }

    /// Returns the durable applied Raft index.
    #[must_use]
    pub const fn applied_index(&self) -> LogIndex {
        self.applied_index
    }

    /// Returns the publication generation of the live image.
    ///
    /// Zero means no publication has committed.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Returns the slot the live image occupies, if one has committed.
    #[must_use]
    pub const fn live_slot(&self) -> Option<SlotIndex> {
        self.live_slot
    }

    /// Returns the slot the next publication will write.
    #[must_use]
    pub const fn next_slot(&self) -> SlotIndex {
        match self.live_slot {
            Some(slot) => slot.other(),
            None => SlotIndex::Zero,
        }
    }

    /// Returns the fencing high-water mark this store has durably acknowledged
    /// for `resource`.
    ///
    /// This is the value the whole design exists to protect: no state this
    /// store publishes or adopts may ever carry a lower one.
    #[must_use]
    pub fn acknowledged_mark(&self, resource: ResourceName) -> Option<FencingToken> {
        self.acknowledged_marks.get(&resource).copied()
    }

    /// Whether an earlier write poisoned this handle.
    #[must_use]
    pub const fn requires_reopen(&self) -> bool {
        matches!(self.health, Health::ReopenRequired)
    }

    /// Returns the injected fault that fired on this handle, if any.
    ///
    /// A crash test asserts on this the way a failpoint scenario asserts that
    /// its guard triggered: a plan that never fired proves nothing, and a suite
    /// of such plans would pass while testing an uninterrupted store.
    #[must_use]
    pub const fn fired_fault(&self) -> Option<WriteFault> {
        self.fired_fault
    }
}
