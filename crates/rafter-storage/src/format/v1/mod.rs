//! Version-1 durable-format grammar shared across storage artifacts.
//!
//! Each child owns one artifact's exact byte grammar or a field grammar shared
//! by version-1 artifacts. Root-level codec modules are compatibility facades
//! that preserve the crate's flat public API.

pub(crate) mod hard_state;
pub(crate) mod log_compaction;
pub(crate) mod log_entry;
pub(crate) mod pending_transfer;
pub(crate) mod snapshot;
pub(crate) mod snapshot_manifest;
pub(crate) mod snapshot_metadata;

#[cfg(test)]
mod pending_transfer_test;
#[cfg(test)]
mod snapshot_manifest_test;
#[cfg(test)]
mod tests;
