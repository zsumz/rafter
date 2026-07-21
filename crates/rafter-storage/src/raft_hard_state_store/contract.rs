//! Durable hard-state behavior shared by all store implementations.
//!
//! This module defines successful-write durability and the acknowledged-state
//! read contract. It does not own file publication, encoding, or recovery.

use crate::RaftHardState;

use super::error::RaftHardStateStoreWriteError;

/// Storage contract for the durable Raft hard state.
///
/// Implementations must make successful writes durable before returning and
/// must report the latest successfully acknowledged state through
/// [`RaftHardStateStore::current`].
pub trait RaftHardStateStore {
    /// Writes the latest Raft hard state.
    ///
    /// # Errors
    ///
    /// Returns [`RaftHardStateStoreWriteError`] when the state cannot be
    /// durably written.
    ///
    /// A file-backed implementation may fail after the filesystem accepted
    /// part or all of the replacement. After an I/O error, callers must reopen
    /// that store before another mutation. The reference implementation
    /// enforces this by returning
    /// [`RaftHardStateStoreWriteError::StoreRequiresReopen`] on later writes.
    fn write_hard_state(
        &mut self,
        state: RaftHardState,
    ) -> Result<(), RaftHardStateStoreWriteError>;

    /// Returns the latest hard state known to this store.
    fn current(&self) -> RaftHardState;
}
