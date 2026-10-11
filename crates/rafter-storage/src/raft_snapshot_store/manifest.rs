//! Current-snapshot manifest file I/O and path selection.
//!
//! The RFSM byte grammar lives in `format::v1::snapshot_manifest`. This module
//! owns reading the manifest file and selecting its fixed store path.

use std::{
    fs::File,
    io::{self, Read},
    path::{Path, PathBuf},
};

pub(super) use crate::format::v1::snapshot_manifest::{encode_manifest, SnapshotManifest};
pub use crate::format::v1::snapshot_manifest::{
    RaftSnapshotManifestDecodeError, RaftSnapshotManifestEncodeError,
};
use crate::format::{
    read_checksummed_prefix,
    v1::snapshot_manifest::{decode_manifest_body, MAX_SNAPSHOT_MANIFEST_BODY_LEN},
    StreamChecksumError,
};

use super::OpenRaftSnapshotStoreError;

pub(super) fn read_manifest(
    manifest_path: &Path,
) -> Result<Option<SnapshotManifest>, OpenRaftSnapshotStoreError> {
    let file = match File::open(manifest_path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(OpenRaftSnapshotStoreError::Io {
                operation: "open raft snapshot manifest",
                path: manifest_path.to_path_buf(),
                source: error.into(),
            });
        }
    };
    read_manifest_from(file)
        .map(Some)
        .map_err(|error| match error {
            ReadManifestError::Io(error) => OpenRaftSnapshotStoreError::Io {
                operation: "read raft snapshot manifest",
                path: manifest_path.to_path_buf(),
                source: error.into(),
            },
            ReadManifestError::Decode(error) => OpenRaftSnapshotStoreError::Manifest(error),
        })
}

#[derive(Debug)]
enum ReadManifestError {
    Io(io::Error),
    Decode(RaftSnapshotManifestDecodeError),
}

fn read_manifest_from(reader: impl Read) -> Result<SnapshotManifest, ReadManifestError> {
    let envelope = read_checksummed_prefix(reader, MAX_SNAPSHOT_MANIFEST_BODY_LEN).map_err(
        |error| match error {
            StreamChecksumError::Io(error) => ReadManifestError::Io(error),
            StreamChecksumError::Checksum(error) => ReadManifestError::Decode(error.into()),
        },
    )?;
    let omitted =
        usize::try_from(envelope.body_len - envelope.prefix.len() as u64).unwrap_or(usize::MAX);
    // The retained prefix covers every possible v1 field. Only trailing bytes
    // can be omitted, and they must not hide an earlier schema/filename error.
    match decode_manifest_body(&envelope.prefix) {
        Ok(manifest) if omitted == 0 => Ok(manifest),
        Ok(_) => Err(ReadManifestError::Decode(
            RaftSnapshotManifestDecodeError::TrailingBytes(omitted),
        )),
        Err(RaftSnapshotManifestDecodeError::TrailingBytes(remaining)) => {
            Err(ReadManifestError::Decode(
                RaftSnapshotManifestDecodeError::TrailingBytes(remaining.saturating_add(omitted)),
            ))
        }
        Err(error) => Err(ReadManifestError::Decode(error)),
    }
}

pub(super) fn manifest_path(directory: &Path) -> PathBuf {
    directory.join("current.snapshot")
}

#[cfg(test)]
#[path = "manifest_read_test.rs"]
mod read_tests;
