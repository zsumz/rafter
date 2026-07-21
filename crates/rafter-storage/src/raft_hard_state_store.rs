//! Durable hard-state contract and reference implementations.
//!
//! This facade exposes the storage trait plus file-backed and in-memory stores.
//! Encoding belongs to `format::v1::hard_state`; filesystem publication belongs
//! to `file`; operational errors and volatile behavior have separate owners.

mod contract;
mod error;
mod file;
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
mod test_support;
