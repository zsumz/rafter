//! In-memory snapshot-store reference behavior.
//!
//! This implementation shares validation with the durable store so tests and
//! volatile runtimes observe the same staging and promotion contract.

use rafter::{
    PendingSnapshotTransfer, RaftSnapshot, SnapshotChunkRequest, SnapshotChunkSource,
    StagedSnapshotChunk,
};

use crate::{crc32, PersistedRaftSnapshot};

use super::{
    source_chunk, stream_chunk_len,
    validation::{pending_transfer_after_chunk, validate_staged_chunk, validate_staged_promotion},
    RaftSnapshotStore, RaftSnapshotStoreWriteError,
};

/// In-memory [`RaftSnapshotStore`] implementation for tests and volatile
/// runtimes.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InMemoryRaftSnapshotStore {
    current: Option<PersistedRaftSnapshot>,
    pending: Option<InMemoryStagedTransfer>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct InMemoryStagedTransfer {
    transfer: PendingSnapshotTransfer,
    bytes: Vec<u8>,
}

impl InMemoryRaftSnapshotStore {
    /// Creates an empty in-memory snapshot store.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates an in-memory snapshot store seeded with `snapshot`.
    #[must_use]
    pub fn with_snapshot(snapshot: PersistedRaftSnapshot) -> Self {
        Self {
            current: Some(snapshot),
            pending: None,
        }
    }

    /// The current snapshot with its payload. In-memory stores hold the
    /// payload by construction; the [`RaftSnapshotStore`] trait itself only
    /// hands out descriptors.
    #[must_use]
    pub fn current(&self) -> Option<&PersistedRaftSnapshot> {
        self.current.as_ref()
    }
}

impl RaftSnapshotStore for InMemoryRaftSnapshotStore {
    fn write_snapshot(
        &mut self,
        snapshot: PersistedRaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.current = Some(snapshot);
        self.pending = None;
        Ok(())
    }

    fn write_snapshot_from_source(
        &mut self,
        snapshot: &RaftSnapshot,
        source: &dyn SnapshotChunkSource,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        let transfer_id = snapshot.transfer_id();
        let total = snapshot.application_payload_len;
        let mut application_payload = Vec::new();
        let mut offset = 0u64;
        while offset < total {
            let len = stream_chunk_len(total - offset, super::SNAPSHOT_STREAM_CHUNK_BYTES);
            let bytes = source_chunk(source, snapshot, transfer_id, offset, len)?;
            application_payload.extend_from_slice(&bytes);
            offset += u64::from(len);
        }
        let actual = crc32(&application_payload);
        if actual != snapshot.application_payload_crc32 {
            return Err(
                RaftSnapshotStoreWriteError::SnapshotPayloadChecksumMismatch {
                    expected: snapshot.application_payload_crc32,
                    actual,
                },
            );
        }
        self.write_snapshot(PersistedRaftSnapshot {
            metadata: snapshot.metadata.clone(),
            application_payload,
        })
    }

    fn current_snapshot(&self) -> Option<RaftSnapshot> {
        self.current.as_ref().map(|current| {
            RaftSnapshot::from_payload(current.metadata.clone(), &current.application_payload)
        })
    }

    fn stage_snapshot_chunk(
        &mut self,
        chunk: &StagedSnapshotChunk,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        let validated = validate_staged_chunk(chunk, self.current_pending_snapshot_transfer())?;
        let transfer = pending_transfer_after_chunk(chunk, validated);
        if chunk.offset == 0 {
            self.pending = Some(InMemoryStagedTransfer {
                transfer,
                bytes: chunk.bytes.clone(),
            });
        } else {
            let Some(staged) = self.pending.as_mut() else {
                return Err(RaftSnapshotStoreWriteError::StagedChunkWithoutTransfer {
                    transfer_id: chunk.transfer_id,
                    offset: chunk.offset,
                });
            };
            staged.bytes.extend_from_slice(&chunk.bytes);
            staged.transfer = transfer;
        }
        Ok(())
    }

    fn promote_staged_snapshot(
        &mut self,
        snapshot: &RaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        validate_staged_promotion(snapshot, self.current_pending_snapshot_transfer())?;
        let requested = snapshot.transfer_id();
        let Some(staged) = self.pending.as_ref() else {
            return Err(RaftSnapshotStoreWriteError::PromoteWithoutStagedTransfer { requested });
        };
        let actual = crc32(&staged.bytes);
        if actual != snapshot.application_payload_crc32 {
            return Err(
                RaftSnapshotStoreWriteError::SnapshotPayloadChecksumMismatch {
                    expected: snapshot.application_payload_crc32,
                    actual,
                },
            );
        }
        let Some(staged) = self.pending.take() else {
            return Err(RaftSnapshotStoreWriteError::PromoteWithoutStagedTransfer { requested });
        };
        self.write_snapshot(PersistedRaftSnapshot {
            metadata: staged.transfer.metadata,
            application_payload: staged.bytes,
        })
    }

    fn clear_pending_snapshot_transfer(&mut self) -> Result<(), RaftSnapshotStoreWriteError> {
        self.pending = None;
        Ok(())
    }

    fn current_pending_snapshot_transfer(&self) -> Option<&PendingSnapshotTransfer> {
        self.pending.as_ref().map(|staged| &staged.transfer)
    }
}

impl SnapshotChunkSource for InMemoryRaftSnapshotStore {
    fn snapshot_chunk(&self, request: SnapshotChunkRequest<'_>) -> Option<Vec<u8>> {
        let current = self.current.as_ref()?;
        let payload_len = current.application_payload.len() as u64;
        if &current.metadata != request.metadata {
            return None;
        }
        if payload_len != request.total_payload_len {
            return None;
        }
        if crc32(&current.application_payload) != request.application_payload_crc32 {
            return None;
        }
        let served =
            RaftSnapshot::from_payload(current.metadata.clone(), &current.application_payload)
                .transfer_id()
                == request.transfer_id;
        if !served {
            return None;
        }
        let start = usize::try_from(request.offset).ok()?;
        let end = start.checked_add(request.len as usize)?;
        current
            .application_payload
            .get(start..end)
            .map(<[u8]>::to_vec)
    }
}
