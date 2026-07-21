//! Immutable snapshot-file and current-manifest publication.
//!
//! The current manifest is the logical commit point. Snapshot-file preparation
//! and publication happen first; pending-transfer removal is cleanup after the
//! new snapshot has become authoritative.

use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::Path,
};

use rafter::RaftSnapshot;

use crate::{
    checksum::RunningCrc32, durable_fs::sync_parent_directory,
    raft_snapshot_codec::encode_raft_snapshot_header,
};

use super::{
    encode_manifest, manifest_path, stream_chunk_len, CurrentSnapshot, FileRaftSnapshotStore,
    RaftSnapshotStore, RaftSnapshotStoreWriteError, SnapshotManifest, SNAPSHOT_STREAM_CHUNK_BYTES,
};

impl FileRaftSnapshotStore {
    /// Streams an immutable snapshot envelope, publishes its current manifest,
    /// then clears obsolete inbound staging.
    pub(super) fn write_snapshot_streamed(
        &mut self,
        descriptor: &RaftSnapshot,
        expected_payload_crc: Option<u32>,
        mut read_chunk: impl FnMut(u64, u32) -> Result<Vec<u8>, RaftSnapshotStoreWriteError>,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        self.ensure_writable()?;

        let payload_len = descriptor.application_payload_len;
        let header = encode_raft_snapshot_header(&descriptor.metadata, payload_len)
            .map_err(RaftSnapshotStoreWriteError::EncodeSnapshot)?;
        let (snapshot_path, file_name, sequence) = self.snapshot_path_for_write(descriptor)?;
        let temp_path = self.temp_snapshot_path();

        self.write_snapshot_temp(
            &temp_path,
            &header,
            payload_len,
            expected_payload_crc,
            &mut read_chunk,
        )?;
        self.publish_snapshot_file(&temp_path, &snapshot_path)?;
        self.publish_current_manifest(sequence, &file_name)?;
        self.next_sequence = sequence.checked_add(1);

        let committed_file_name = file_name.clone();
        self.current = Some(CurrentSnapshot {
            file_name,
            descriptor: descriptor.clone(),
            payload_offset: header.len() as u64,
        });
        match self.clear_pending_snapshot_transfer() {
            Ok(()) => Ok(()),
            Err(error) => Err(self.snapshot_committed_cleanup_failure(committed_file_name, error)),
        }
    }

    fn write_snapshot_temp(
        &mut self,
        temp_path: &Path,
        header: &[u8],
        payload_len: u64,
        expected_payload_crc: Option<u32>,
        read_chunk: &mut impl FnMut(u64, u32) -> Result<Vec<u8>, RaftSnapshotStoreWriteError>,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        let mut file = match OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(temp_path)
        {
            Ok(file) => file,
            Err(error) => {
                return Err(self.io_failure("open raft snapshot temp file", temp_path, error));
            }
        };
        let mut envelope_crc = RunningCrc32::new();
        let mut payload_crc = RunningCrc32::new();
        self.write_snapshot_temp_bytes(&mut file, temp_path, header)?;
        envelope_crc.update(header);

        let mut offset = 0u64;
        while offset < payload_len {
            let len = stream_chunk_len(payload_len - offset, SNAPSHOT_STREAM_CHUNK_BYTES);
            let bytes = match read_chunk(offset, len) {
                Ok(bytes) => bytes,
                Err(error) => return Err(self.poison_if_io(error)),
            };
            self.write_snapshot_temp_bytes(&mut file, temp_path, &bytes)?;
            payload_crc.update(&bytes);
            envelope_crc.update(&bytes);
            offset += u64::from(len);
        }

        if let Some(expected) = expected_payload_crc {
            if payload_crc.value() != expected {
                return Err(
                    RaftSnapshotStoreWriteError::SnapshotPayloadChecksumMismatch {
                        expected,
                        actual: payload_crc.value(),
                    },
                );
            }
        }

        let payload_crc_bytes = payload_crc.value().to_be_bytes();
        self.write_snapshot_temp_bytes(&mut file, temp_path, &payload_crc_bytes)?;
        envelope_crc.update(&payload_crc_bytes);
        self.write_snapshot_temp_bytes(&mut file, temp_path, &envelope_crc.value().to_be_bytes())?;
        file.sync_data()
            .map_err(|error| self.io_failure("write raft snapshot temp file", temp_path, error))?;
        #[cfg(test)]
        crate::storage_failpoint_test::check(
            crate::storage_failpoint_test::DurabilityPoint::SnapshotAfterTempSync,
        )
        .map_err(|error| self.io_failure("write raft snapshot temp file", temp_path, error))?;
        Ok(())
    }

    fn write_snapshot_temp_bytes(
        &mut self,
        file: &mut File,
        temp_path: &Path,
        bytes: &[u8],
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        file.write_all(bytes)
            .map_err(|error| self.io_failure("write raft snapshot temp file", temp_path, error))
    }

    fn publish_snapshot_file(
        &mut self,
        temp_path: &Path,
        snapshot_path: &Path,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        fs::rename(temp_path, snapshot_path)
            .map_err(|error| self.io_failure("replace raft snapshot", snapshot_path, error))?;
        #[cfg(test)]
        crate::storage_failpoint_test::check(
            crate::storage_failpoint_test::DurabilityPoint::SnapshotAfterFileRename,
        )
        .map_err(|error| self.io_failure("replace raft snapshot", snapshot_path, error))?;
        sync_parent_directory(snapshot_path).map_err(|error| {
            self.io_failure("sync raft snapshot directory", snapshot_path, error)
        })?;
        #[cfg(test)]
        crate::storage_failpoint_test::check(
            crate::storage_failpoint_test::DurabilityPoint::SnapshotAfterFileDirectorySync,
        )
        .map_err(|error| self.io_failure("sync raft snapshot directory", snapshot_path, error))?;
        Ok(())
    }

    fn publish_current_manifest(
        &mut self,
        sequence: u64,
        file_name: &str,
    ) -> Result<(), RaftSnapshotStoreWriteError> {
        let manifest = SnapshotManifest {
            sequence,
            file_name: file_name.to_string(),
        };
        let encoded =
            encode_manifest(&manifest).map_err(RaftSnapshotStoreWriteError::EncodeManifest)?;
        match write_temp_and_rename(
            &self.temp_manifest_path(),
            &manifest_path(&self.directory),
            &encoded,
            "open raft snapshot manifest temp file",
            "write raft snapshot manifest temp file",
            "replace raft snapshot manifest",
            "sync raft snapshot manifest directory",
        ) {
            Ok(()) => Ok(()),
            Err(error) => Err(self.poison_if_io(error)),
        }
    }
}

fn write_temp_and_rename(
    temp_path: &Path,
    final_path: &Path,
    bytes: &[u8],
    open_operation: &'static str,
    write_operation: &'static str,
    rename_operation: &'static str,
    sync_operation: &'static str,
) -> Result<(), RaftSnapshotStoreWriteError> {
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(temp_path)
            .map_err(|error| RaftSnapshotStoreWriteError::Io {
                operation: open_operation,
                path: temp_path.to_path_buf(),
                source: error.into(),
            })?;
        file.write_all(bytes)
            .and_then(|()| file.sync_data())
            .map_err(|error| RaftSnapshotStoreWriteError::Io {
                operation: write_operation,
                path: temp_path.to_path_buf(),
                source: error.into(),
            })?;
        #[cfg(test)]
        crate::storage_failpoint_test::check(
            crate::storage_failpoint_test::DurabilityPoint::SnapshotAfterManifestTempSync,
        )
        .map_err(|error| RaftSnapshotStoreWriteError::Io {
            operation: write_operation,
            path: temp_path.to_path_buf(),
            source: error.into(),
        })?;
    }

    fs::rename(temp_path, final_path).map_err(|error| RaftSnapshotStoreWriteError::Io {
        operation: rename_operation,
        path: final_path.to_path_buf(),
        source: error.into(),
    })?;
    #[cfg(test)]
    crate::storage_failpoint_test::check(
        crate::storage_failpoint_test::DurabilityPoint::SnapshotAfterManifestRename,
    )
    .map_err(|error| RaftSnapshotStoreWriteError::Io {
        operation: rename_operation,
        path: final_path.to_path_buf(),
        source: error.into(),
    })?;
    sync_parent_directory(final_path).map_err(|error| RaftSnapshotStoreWriteError::Io {
        operation: sync_operation,
        path: final_path.to_path_buf(),
        source: error.into(),
    })?;
    #[cfg(test)]
    crate::storage_failpoint_test::check(
        crate::storage_failpoint_test::DurabilityPoint::SnapshotAfterManifestDirectorySync,
    )
    .map_err(|error| RaftSnapshotStoreWriteError::Io {
        operation: sync_operation,
        path: final_path.to_path_buf(),
        source: error.into(),
    })?;
    Ok(())
}
