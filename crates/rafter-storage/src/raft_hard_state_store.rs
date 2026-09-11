//! Durable hard-state contract and reference implementations.
//!
//! This facade exposes the storage trait plus file-backed and in-memory stores.
//! Encoding belongs to `format::v1::hard_state`; filesystem publication belongs
//! to `file`; operational errors and volatile behavior have separate owners.

mod contract;
mod error;
mod file;
mod journal;
mod journal_checkpoint;
mod journal_error;
mod journal_recovery;
mod memory;

pub use contract::RaftHardStateStore;
pub use error::{OpenRaftHardStateStoreError, RaftHardStateStoreWriteError};
pub use file::FileRaftHardStateStore;
pub use memory::InMemoryRaftHardStateStore;

#[cfg(test)]
mod file_test;
#[cfg(test)]
mod memory_test;
#[cfg(test)]
pub(crate) mod test_support;

pub use journal::JournalRaftHardStateStore;
pub use journal_error::OpenJournalRaftHardStateStoreError;

#[cfg(test)]
mod journal_checkpoint_test;
#[cfg(test)]
mod journal_test;
