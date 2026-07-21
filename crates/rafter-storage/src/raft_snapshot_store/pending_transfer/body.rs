//! Pending-transfer body replacement, append, truncation, and promotion reads.
//!
//! The manifest remains the authoritative staged length; this module owns only
//! the raw payload-prefix file and its crash-residue reconciliation.

use std::{
    fs::{File, OpenOptions},
    io::{Seek, SeekFrom, Write},
    path::Path,
};

use rafter::StagedSnapshotChunk;

use super::{
    super::RaftSnapshotStoreWriteError, filesystem::write_temp_and_rename,
    paths::pending_snapshot_transfer_body_path,
};

/// Writes `chunk`'s bytes at its offset in the staged body file: a chunk at
/// offset zero replaces the body, a continuation chunk appends to it.
///
/// The caller has already validated that a continuation chunk's offset equals
/// the staged length, so the body file is expected to hold exactly
/// `chunk.offset` bytes. A longer file is the harmless leftover of a crash
/// between a body append and its manifest write and is truncated back; a
/// shorter file lost staged bytes and fails loudly.
pub(super) fn write_pending_snapshot_body_chunk(
    directory: &Path,
    chunk: &StagedSnapshotChunk,
) -> Result<(), RaftSnapshotStoreWriteError> {
    if chunk.offset == 0 {
        rewrite_pending_snapshot_body(directory, &chunk.bytes)
    } else {
        append_pending_snapshot_body(directory, chunk)
    }
}

/// Opens the staged body for promotion, positioned at its start. The file
/// must hold at least `received_len` bytes (a longer file is the harmless
/// leftover of a crash between a body append and its manifest write; the
/// promotion reads only the staged prefix). Content integrity is enforced by
/// the promotion stream itself, which checks the assembled payload checksum
/// against the staged running checksum before the snapshot becomes visible.
pub(in crate::raft_snapshot_store) fn open_staged_body(
    directory: &Path,
    received_len: u64,
) -> Result<File, RaftSnapshotStoreWriteError> {
    let body_path = pending_snapshot_transfer_body_path(directory);
    let file = File::open(&body_path).map_err(|error| RaftSnapshotStoreWriteError::Io {
        operation: "open pending snapshot transfer body for promotion",
        path: body_path.clone(),
        source: error.into(),
    })?;
    let body_len = file
        .metadata()
        .map_err(|error| RaftSnapshotStoreWriteError::Io {
            operation: "stat pending snapshot transfer body for promotion",
            path: body_path.clone(),
            source: error.into(),
        })?
        .len();
    if body_len < received_len {
        return Err(RaftSnapshotStoreWriteError::Io {
            operation: "open pending snapshot transfer body for promotion",
            path: body_path,
            source: std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("body file holds {body_len} bytes but {received_len} bytes are staged"),
            )
            .into(),
        });
    }
    Ok(file)
}

fn append_pending_snapshot_body(
    directory: &Path,
    chunk: &StagedSnapshotChunk,
) -> Result<(), RaftSnapshotStoreWriteError> {
    let body_path = pending_snapshot_transfer_body_path(directory);
    let staged_len = chunk.offset;
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(&body_path)
        .map_err(|error| RaftSnapshotStoreWriteError::Io {
            operation: "open pending snapshot transfer body",
            path: body_path.clone(),
            source: error.into(),
        })?;
    let body_len = file
        .metadata()
        .map_err(|error| RaftSnapshotStoreWriteError::Io {
            operation: "stat pending snapshot transfer body",
            path: body_path.clone(),
            source: error.into(),
        })?
        .len();
    if body_len < staged_len {
        return Err(RaftSnapshotStoreWriteError::Io {
            operation: "append pending snapshot transfer body",
            path: body_path,
            source: std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("body file holds {body_len} bytes but {staged_len} bytes are staged"),
            )
            .into(),
        });
    }
    if body_len > staged_len {
        file.set_len(staged_len)
            .and_then(|()| file.seek(SeekFrom::End(0)).map(|_| ()))
            .map_err(|error| RaftSnapshotStoreWriteError::Io {
                operation: "truncate pending snapshot transfer body to staged length",
                path: body_path.clone(),
                source: error.into(),
            })?;
    }
    file.write_all(&chunk.bytes)
        .and_then(|()| file.sync_data())
        .map_err(|error| RaftSnapshotStoreWriteError::Io {
            operation: "append pending snapshot transfer body",
            path: body_path,
            source: error.into(),
        })
}

fn rewrite_pending_snapshot_body(
    directory: &Path,
    bytes: &[u8],
) -> Result<(), RaftSnapshotStoreWriteError> {
    let temp_path = directory.join(format!(
        ".pending.snapshot-transfer.body-{}.tmp",
        std::process::id()
    ));
    write_temp_and_rename(
        &temp_path,
        &pending_snapshot_transfer_body_path(directory),
        bytes,
        "open pending snapshot transfer body temp file",
        "write pending snapshot transfer body temp file",
        "replace pending snapshot transfer body",
        "sync pending snapshot transfer body directory",
    )
}
