//! What an open handle reports about itself.
//!
//! Every one of these is a read of state the opening path or a write path
//! already decided. Nothing here reaches the filesystem, and nothing here can
//! fail: a caller that wants to know what a *fresh* opener would find reopens.

use rafter::LogIndex;

use crate::{Ledger, LedgerConfig};

use super::{fault::WriteFault, report::RecoveryReport, Health, LedgerStore};

impl LedgerStore {
    /// Returns what opening this store found and did.
    #[must_use]
    pub const fn recovery(&self) -> &RecoveryReport {
        &self.recovery
    }

    /// Returns the resource bounds this journal was created under.
    #[must_use]
    pub const fn config(&self) -> LedgerConfig {
        self.config
    }

    /// Returns the durable ledger state.
    #[must_use]
    pub const fn ledger(&self) -> &Ledger {
        &self.ledger
    }

    /// Returns the durable applied Raft index.
    #[must_use]
    pub const fn applied_index(&self) -> LogIndex {
        self.applied_index
    }

    /// Returns the journal's committed length in bytes.
    #[must_use]
    pub const fn journal_len(&self) -> u64 {
        self.journal_len
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
