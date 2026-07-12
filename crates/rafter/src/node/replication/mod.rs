//! Log replication and leader authority evidence.
//!
//! Replication is split by direction and responsibility: followers receive
//! append frames, leaders process acknowledgements, the send path fills
//! per-follower windows, and snapshot transfer owns its separate byte stream.

mod authority;
mod progress;
mod proposal;
mod receive;
mod response;
mod send;
mod snapshot;

#[cfg(test)]
mod response_test;

pub(in crate::node) use send::ReplicationDemand;
pub use snapshot::PendingSnapshotTransferResumeError;
