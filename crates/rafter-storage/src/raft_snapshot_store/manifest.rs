//! Current-snapshot manifest file I/O and path selection.
//!
//! The RFSM byte grammar lives in `format::v1::snapshot_manifest`. This module
//! owns reading the manifest file and selecting its fixed store path.

use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
};

use crate::format::v1::snapshot_manifest::decode_manifest;
pub(super) use crate::format::v1::snapshot_manifest::{encode_manifest, SnapshotManifest};
pub use crate::format::v1::snapshot_manifest::{
    RaftSnapshotManifestDecodeError, RaftSnapshotManifestEncodeError,
};

use super::OpenRaftSnapshotStoreError;

pub(super) fn read_manifest(
    manifest_path: &Path,
) -> Result<Option<SnapshotManifest>, OpenRaftSnapshotStoreError> {
    if !manifest_path.exists() {
        return Ok(None);
    }
    let mut file = File::open(manifest_path).map_err(|error| OpenRaftSnapshotStoreError::Io {
        operation: "open raft snapshot manifest",
        path: manifest_path.to_path_buf(),
        source: error.into(),
    })?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|error| OpenRaftSnapshotStoreError::Io {
            operation: "read raft snapshot manifest",
            path: manifest_path.to_path_buf(),
            source: error.into(),
        })?;
    decode_manifest(&bytes)
        .map(Some)
        .map_err(OpenRaftSnapshotStoreError::Manifest)
}

pub(super) fn manifest_path(directory: &Path) -> PathBuf {
    directory.join("current.snapshot")
}
