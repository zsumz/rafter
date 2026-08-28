//! Version-1 persisted-snapshot value types.
//!
//! One type names a complete in-memory snapshot envelope; the other names the
//! decoded prefix streaming readers parse before touching payload bytes.

use rafter::RaftSnapshotMetadata;

/// A complete persisted Raft snapshot envelope in memory.
///
/// The metadata is Raft-visible snapshot state. The payload is opaque
/// application state protected by the envelope checksum.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistedRaftSnapshot {
    /// Raft-visible identity, position, membership, and payload metadata.
    pub metadata: RaftSnapshotMetadata,
    /// Opaque application state bytes.
    pub application_payload: Vec<u8>,
}

/// The decoded prefix of a snapshot envelope: everything before the payload
/// bytes. `header_len` is where the payload starts, so streaming readers can
/// verify or serve the payload without materializing it.
///
/// Deliberately carries no payload checksum. The checksum lives in the
/// envelope's trailer, past `payload_len` bytes this type has not seen, so
/// parsing a prefix cannot know it. A field here could only ever hold a
/// placeholder, and a placeholder that flows into `RaftSnapshot::new` produces
/// a wrong `transfer_id()` in silence. Callers that need the checksum verify
/// the payload and receive it from that verification instead.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct SnapshotEnvelopeHeader {
    pub metadata: RaftSnapshotMetadata,
    pub payload_len: u64,
    pub header_len: u64,
}
