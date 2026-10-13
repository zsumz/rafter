//! Admit pending-transfer metadata before considering body recovery or cleanup.
use super::{
    super::OpenRaftSnapshotStoreError, error::DecodePendingSnapshotTransferError,
    manifest::PendingTransferManifest,
};
use crate::format::{
    read_checksummed_prefix,
    v1::pending_transfer::{decode_pending_manifest_body, MAX_PENDING_SNAPSHOT_MANIFEST_BODY_LEN},
    StreamChecksumError,
};
use std::{
    fs::File,
    io::{self, Read},
    path::Path,
};

pub(super) fn read_pending_snapshot_transfer_manifest(
    path: &Path,
) -> Result<Option<PendingTransferManifest>, OpenRaftSnapshotStoreError> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(OpenRaftSnapshotStoreError::Io {
                operation: "open pending snapshot transfer manifest",
                path: path.to_path_buf(),
                source: error.into(),
            });
        }
    };
    read_manifest_from(file)
        .map(Some)
        .map_err(|error| match error {
            ReadManifestError::Io(error) => OpenRaftSnapshotStoreError::Io {
                operation: "read pending snapshot transfer manifest",
                path: path.to_path_buf(),
                source: error.into(),
            },
            ReadManifestError::Decode(error) => OpenRaftSnapshotStoreError::PendingTransfer(error),
        })
}

#[derive(Debug)]
enum ReadManifestError {
    Io(io::Error),
    Decode(DecodePendingSnapshotTransferError),
}

fn read_manifest_from(reader: impl Read) -> Result<PendingTransferManifest, ReadManifestError> {
    let envelope = read_checksummed_prefix(reader, MAX_PENDING_SNAPSHOT_MANIFEST_BODY_LEN)
        .map_err(|error| match error {
            StreamChecksumError::Io(error) => ReadManifestError::Io(error),
            StreamChecksumError::Checksum(error) => ReadManifestError::Decode(error.into()),
        })?;
    let omitted =
        usize::try_from(envelope.body_len - envelope.prefix.len() as u64).unwrap_or(usize::MAX);
    decode_pending_manifest_body(&envelope.prefix, omitted).map_err(ReadManifestError::Decode)
}

#[cfg(test)]
#[path = "manifest_read_test.rs"]
mod tests;
