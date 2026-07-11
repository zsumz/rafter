//! Runtime-provided access to application snapshot payload bytes.

use std::collections::BTreeMap;
use std::{error::Error, fmt};

use super::{RaftSnapshot, RaftSnapshotMetadata, SnapshotTransferId};

/// Read access to snapshot payload bytes, supplied by the runtime or storage
/// layer.
///
/// The kernel never holds application snapshot payloads: a
/// [`RaftSnapshot`] carries only metadata and a payload length, and a leader
/// streams content by emitting
/// [`Output::SendSnapshotChunk`](crate::Output::SendSnapshotChunk)
/// directives. The transport resolves each directive against a source with
/// [`SnapshotChunkSend::resolve`](crate::SnapshotChunkSend::resolve) before
/// putting the chunk on the wire, so payload bytes flow from the
/// application's snapshot store to the network without entering kernel
/// state.
pub trait SnapshotChunkSource {
    /// Returns exactly `request.len` bytes at `request.offset` within the
    /// payload identified by `request.transfer_id`, or `None` when this
    /// source cannot serve that snapshot — because it holds a different
    /// snapshot, or none at all. An unserved request is dropped like a lost
    /// message: the transfer resumes from the follower's acknowledged offset
    /// once the source and the kernel agree on the current snapshot again.
    fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>>;
}

/// One bounded read of snapshot payload bytes, described by the transfer
/// identity every chunk of the transfer carries.
#[derive(Clone, Copy, Debug)]
pub struct SnapshotChunkRequest<'a> {
    pub transfer_id: SnapshotTransferId,
    pub metadata: &'a RaftSnapshotMetadata,
    pub total_payload_len: u64,
    pub application_payload_crc32: u32,
    pub offset: u64,
    pub len: u32,
}

/// A [`SnapshotChunkSource`] over payloads held in memory, keyed by transfer
/// identity. Suits tests, simulations, and state machines whose snapshots
/// are small enough that streaming from memory is streaming enough.
#[derive(Clone, Debug, Default, Eq, Hash, PartialEq)]
pub struct InMemorySnapshotChunkSource {
    payloads: BTreeMap<SnapshotTransferId, Vec<u8>>,
}

impl InMemorySnapshotChunkSource {
    /// Builds an empty in-memory snapshot source.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers `payload` as the content of `snapshot`.
    ///
    /// # Errors
    ///
    /// Returns [`InMemorySnapshotSourceError::PayloadLengthMismatch`] when
    /// the payload does not have the length the snapshot declares.
    pub fn insert(
        &mut self,
        snapshot: &RaftSnapshot,
        payload: Vec<u8>,
    ) -> Result<(), InMemorySnapshotSourceError> {
        if payload.len() as u64 != snapshot.application_payload_len {
            return Err(InMemorySnapshotSourceError::PayloadLengthMismatch {
                declared: snapshot.application_payload_len,
                actual: payload.len() as u64,
            });
        }
        self.payloads.insert(snapshot.transfer_id(), payload);
        Ok(())
    }

    /// Removes and returns the payload registered for `transfer_id`.
    pub fn remove(&mut self, transfer_id: SnapshotTransferId) -> Option<Vec<u8>> {
        self.payloads.remove(&transfer_id)
    }

    /// Returns the payload registered for `transfer_id`.
    #[must_use]
    pub fn payload(&self, transfer_id: SnapshotTransferId) -> Option<&[u8]> {
        self.payloads.get(&transfer_id).map(Vec::as_slice)
    }
}

impl SnapshotChunkSource for InMemorySnapshotChunkSource {
    fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        let payload = self.payloads.get(&request.transfer_id)?;
        if payload.len() as u64 != request.total_payload_len {
            return None;
        }
        if super::application_payload_crc32(payload) != request.application_payload_crc32 {
            return None;
        }
        let start = usize::try_from(request.offset).ok()?;
        let end = start.checked_add(request.len as usize)?;
        payload.get(start..end).map(<[u8]>::to_vec)
    }
}

/// Error returned by the in-memory snapshot payload source.
///
/// This enum is exhaustive because the source only validates payload length
/// before accepting a payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InMemorySnapshotSourceError {
    PayloadLengthMismatch { declared: u64, actual: u64 },
}

impl fmt::Display for InMemorySnapshotSourceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PayloadLengthMismatch { declared, actual } => write!(
                formatter,
                concat!(
                    "snapshot payload of {actual} bytes does not match the declared ",
                    "payload length {declared}"
                ),
                actual = actual,
                declared = declared,
            ),
        }
    }
}

impl Error for InMemorySnapshotSourceError {}
