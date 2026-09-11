//! A successful batch receipt names one recoverable log/hard-state publication.
//!
//! Existing store methods retain their synchronous durability guarantees.
//! A runtime opts into batching only when both handles share one domain.
mod codec;
mod contract;
mod error;
mod handles;
mod open;
mod state;
mod stores;

pub use contract::{DurableReceipt, PersistenceDomain, RaftPersistenceBatch};
pub use error::RaftPersistenceBatchError;

pub use handles::{WalRaftHardStateStore, WalRaftLogSegment};
pub use stores::WalRaftNodeStores;

#[cfg(test)]
mod tests;
