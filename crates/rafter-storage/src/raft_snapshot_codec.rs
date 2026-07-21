//! Compatibility facade for the version-1 persisted-snapshot format.
//!
//! The RFSN byte grammar lives in `format::v1::snapshot`; this module keeps the
//! established crate module path and flat public API stable.

pub use crate::format::v1::snapshot::{
    decode_raft_snapshot, encode_raft_snapshot, DecodeRaftSnapshotError, EncodeRaftSnapshotError,
    PersistedRaftSnapshot, RAFT_SNAPSHOT_MAGIC, RAFT_SNAPSHOT_VERSION,
};

pub(crate) use crate::format::v1::snapshot::{
    decode_raft_snapshot_header, encode_raft_snapshot_header, SnapshotEnvelopeHeader,
};

#[cfg(test)]
pub(crate) use crate::format::v1::snapshot::encode_raft_snapshot_metadata_envelope;
