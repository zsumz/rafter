//! Volatile hard-state storage for tests and in-memory runtimes.
//!
//! This implementation mirrors the logical store contract without filesystem
//! durability, recovery, or post-I/O health state.

use crate::RaftHardState;

use super::{contract::RaftHardStateStore, error::RaftHardStateStoreWriteError};

/// In-memory hard-state store for tests and volatile runtimes.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InMemoryRaftHardStateStore {
    current: RaftHardState,
}

impl InMemoryRaftHardStateStore {
    /// Creates an empty in-memory hard-state store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl RaftHardStateStore for InMemoryRaftHardStateStore {
    fn write_hard_state(
        &mut self,
        state: RaftHardState,
    ) -> Result<(), RaftHardStateStoreWriteError> {
        self.current = state;
        Ok(())
    }

    fn current(&self) -> RaftHardState {
        self.current
    }
}
