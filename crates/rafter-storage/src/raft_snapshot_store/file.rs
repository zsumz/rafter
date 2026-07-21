//! File-backed snapshot-store mutations.
//!
//! This module implements the public store contract over the concrete file
//! state. It delegates validation, immutable publication, health transitions,
//! payload sourcing, and pending-transfer filesystem mechanics to their owners.

use std::io::Read;

use rafter::{PendingSnapshotTransfer, RaftSnapshot, SnapshotChunkSource, StagedSnapshotChunk};

use crate::{checksum::RunningCrc32, PersistedRaftSnapshot};

use super::{
    pending_transfer::{
        clear_pending_snapshot_transfer, open_staged_body, pending_snapshot_transfer_body_path,
        remove_abandoned_pending_snapshot_transfer_staging, stage_pending_snapshot_chunk,
    },
    source::source_chunk,
    validation::{pending_transfer_after_chunk, validate_staged_chunk, validate_staged_promotion},
    FileRaftSnapshotStore, RaftSnapshotStore, RaftSnapshotStoreWriteError, StagedTransfer,
};

impl FileRaftSnapshotStore {
    /// Removes an abandoned pending snapshot transfer body when no manifest is
    /// present.
    ///
    /// Returns `true` when a staging body was removed. Valid resumable pending
    /// transfers are left untouched.
    ///
    /// # Errors
    ///
    /// Returns [`RaftSnapshotStoreWriteError`] when the abandoned staging body
    /// or its parent directory cannot be durably updated.
    pub fn remove_abandoned_pending_snapshot_transfer_staging(
        &mut self,
    ) -> Result<bool, RaftSnapshotStoreWriteError> {
        self.ensure_writable()?;
        let result = remove_abandoned_pending_snapshot_transfer_staging(&self.directory);
        result.map_err(|error| self.poison_if_io(error))
    }
}

impl RaftSnapshotStore for FileRaftSnapshotStore {
    fn write_snapshot(
        &mut self,
        snapshot: PersistedRaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        let descriptor =
            RaftSnapshot::from_payload(snapshot.metadata.clone(), &snapshot.application_payload);
        let payload = snapshot.application_payload;
        self.write_snapshot_streamed(
            &descriptor,
            Some(descriptor.application_payload_crc32),
            |offset, len| {
                let start = usize::try_from(offset).map_err(|_| {
                    RaftSnapshotStoreWriteError::SourceChunkUnavailable {
                        transfer_id: descriptor.transfer_id(),
                        offset,
                    }
                })?;
                Ok(payload[start..start + len as usize].to_vec())
            },
        )
    }

    fn write_snapshot_from_source(
        &mut self,
        snapshot: &RaftSnapshot,
        source: &dyn SnapshotChunkSource,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        let transfer_id = snapshot.transfer_id();
        let descriptor = snapshot.clone();
        self.write_snapshot_streamed(
            snapshot,
            Some(snapshot.application_payload_crc32),
            move |offset, len| source_chunk(source, &descriptor, transfer_id, offset, len),
        )
    }

    fn current_snapshot(&self) -> Option<RaftSnapshot> {
        self.current
            .as_ref()
            .map(|current| current.descriptor.clone())
    }

    fn stage_snapshot_chunk(
        &mut self,
        chunk: &StagedSnapshotChunk,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.ensure_writable()?;
        let validated = validate_staged_chunk(chunk, self.current_pending_snapshot_transfer())?;
        let body_crc_before_chunk = if chunk.offset == 0 {
            RunningCrc32::new()
        } else {
            let Some(pending) = self.pending.as_ref() else {
                return Err(RaftSnapshotStoreWriteError::StagedChunkWithoutTransfer {
                    transfer_id: chunk.transfer_id,
                    offset: chunk.offset,
                });
            };
            pending.body_crc
        };
        let result = stage_pending_snapshot_chunk(
            &self.directory,
            &self.temp_pending_snapshot_transfer_path(),
            chunk,
            validated.received_len(),
            body_crc_before_chunk,
        );
        let body_crc = match result {
            Ok(body_crc) => body_crc,
            Err(error) => return Err(self.poison_if_io(error)),
        };
        self.pending = Some(StagedTransfer {
            transfer: pending_transfer_after_chunk(chunk, validated),
            body_crc,
        });
        Ok(())
    }

    fn promote_staged_snapshot(
        &mut self,
        snapshot: &RaftSnapshot,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.ensure_writable()?;
        validate_staged_promotion(snapshot, self.current_pending_snapshot_transfer())?;
        let requested = snapshot.transfer_id();
        let received_len = match self.pending.as_ref() {
            Some(staged) => staged.transfer.received_len,
            None => {
                return Err(RaftSnapshotStoreWriteError::PromoteWithoutStagedTransfer {
                    requested,
                });
            }
        };
        let expected_payload_crc = snapshot.application_payload_crc32;
        let body_result = open_staged_body(&self.directory, received_len);
        let mut body = match body_result {
            Ok(body) => body,
            Err(error) => return Err(self.poison_if_io(error)),
        };
        let body_path = pending_snapshot_transfer_body_path(&self.directory);
        self.write_snapshot_streamed(snapshot, Some(expected_payload_crc), |_, len| {
            let mut bytes = vec![0u8; len as usize];
            body.read_exact(&mut bytes)
                .map_err(|error| RaftSnapshotStoreWriteError::Io {
                    operation: "read pending snapshot transfer body for promotion",
                    path: body_path.clone(),
                    source: error.into(),
                })?;
            Ok(bytes)
        })
    }

    fn clear_pending_snapshot_transfer(&mut self) -> Result<(), RaftSnapshotStoreWriteError> {
        self.ensure_writable()?;
        let result = clear_pending_snapshot_transfer(&self.directory);
        if let Err(error) = result {
            return Err(self.poison_if_io(error));
        }
        self.pending = None;
        Ok(())
    }

    fn current_pending_snapshot_transfer(&self) -> Option<&PendingSnapshotTransfer> {
        self.pending.as_ref().map(|pending| &pending.transfer)
    }
}
