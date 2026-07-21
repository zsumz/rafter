//! File-backed log opening, streaming replay, and commit-floor-bounded tail repair.
//!
//! Replay keeps only decoded entries resident: raw segment bytes are consumed
//! incrementally, and malformed-tail offsets remain exact for safe repair.

use std::{
    fs::{File, OpenOptions},
    io::{BufReader, Read},
    path::Path,
};

use rafter::LogIndex;

use super::{
    compaction_marker_path, frames::RaftLogFrameScan, read_raft_log_frames, ContiguousLogEntries,
    FileRaftLogSegment, NonContiguousRaftEntry, OpenRaftLogSegmentError,
};
use crate::{
    durable_fs::{sync_parent_directory, ParentDirectorySyncBatch},
    file_store_health::FileStoreHealth,
    raft_log_compaction::decode_raft_log_compaction_marker,
};

impl FileRaftLogSegment {
    /// Opens a Raft log segment at `path` and replays existing entries.
    ///
    /// The raw file is consumed incrementally; startup memory is the decoded
    /// physical entries plus at most one encoded frame and a bounded read buffer.
    ///
    /// # Errors
    ///
    /// Returns [`OpenRaftLogSegmentError::Replay`] when existing bytes are
    /// corrupt or end in a partial frame, [`OpenRaftLogSegmentError::NonContiguous`]
    /// when replayed entries skip an index, and [`OpenRaftLogSegmentError::Io`]
    /// when the file cannot be opened or read.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, OpenRaftLogSegmentError> {
        Self::open_with_creation_sync(path.as_ref(), CreationSync::Immediate, OpenMode::Strict)
    }

    /// Opens a Raft log segment, truncating only a corrupt, partial, or
    /// non-contiguous uncommitted tail when the durable `commit_index` proves
    /// the retained prefix is sufficient to recover safely.
    ///
    /// This is intentionally separate from [`Self::open`], which remains
    /// fail-loud. Repair is allowed only when the log's compacted prefix plus
    /// its valid contiguous frame prefix already covers `durable_commit_index`.
    ///
    /// # Errors
    ///
    /// Returns [`OpenRaftLogSegmentError::Replay`] or
    /// [`OpenRaftLogSegmentError::NonContiguous`] instead of truncating when
    /// the first bad frame or gap may contain committed state.
    pub fn open_repairing_uncommitted_tail(
        path: impl AsRef<Path>,
        durable_commit_index: LogIndex,
    ) -> Result<Self, OpenRaftLogSegmentError> {
        Self::open_with_creation_sync(
            path.as_ref(),
            CreationSync::Immediate,
            OpenMode::RepairUncommittedTail {
                durable_commit_index,
            },
        )
    }

    pub(crate) fn open_with_parent_sync_batch(
        path: impl AsRef<Path>,
        batch: &mut ParentDirectorySyncBatch,
    ) -> Result<Self, OpenRaftLogSegmentError> {
        Self::open_with_creation_sync(
            path.as_ref(),
            CreationSync::Batched(batch),
            OpenMode::Strict,
        )
    }

    pub(crate) fn open_with_parent_sync_batch_repairing_uncommitted_tail(
        path: impl AsRef<Path>,
        batch: &mut ParentDirectorySyncBatch,
        durable_commit_index: LogIndex,
    ) -> Result<Self, OpenRaftLogSegmentError> {
        Self::open_with_creation_sync(
            path.as_ref(),
            CreationSync::Batched(batch),
            OpenMode::RepairUncommittedTail {
                durable_commit_index,
            },
        )
    }

    fn open_with_creation_sync(
        path: &Path,
        creation_sync: CreationSync<'_>,
        mode: OpenMode,
    ) -> Result<Self, OpenRaftLogSegmentError> {
        let existed = path.exists();
        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)
            .map_err(|error| OpenRaftLogSegmentError::Io {
                operation: "open raft log segment",
                path: path.to_path_buf(),
                source: error.into(),
            })?;
        if !existed {
            match creation_sync {
                CreationSync::Immediate => {
                    sync_parent_directory(path).map_err(|error| OpenRaftLogSegmentError::Io {
                        operation: "sync raft log segment directory",
                        path: path.to_path_buf(),
                        source: error.into(),
                    })?;
                }
                CreationSync::Batched(batch) => batch.record_parent_of(path),
            }
        }

        let file_len = file
            .metadata()
            .map_err(|error| OpenRaftLogSegmentError::Io {
                operation: "stat raft log segment",
                path: path.to_path_buf(),
                source: error.into(),
            })?
            .len();
        let scan = {
            let reader = BufReader::new(&mut file);
            read_raft_log_frames(reader, file_len).map_err(|error| OpenRaftLogSegmentError::Io {
                operation: "read raft log segment",
                path: path.to_path_buf(),
                source: error.into(),
            })?
        };

        let compacted_through = read_compaction_marker(path)?;
        let (entries, repair_truncate_offset) =
            replay_entries_for_open(scan, compacted_through, mode)?;
        if let Some(offset) = repair_truncate_offset {
            file.set_len(offset)
                .and_then(|()| file.sync_all())
                .map_err(|error| OpenRaftLogSegmentError::Io {
                    operation: "truncate corrupt uncommitted raft log tail",
                    path: path.to_path_buf(),
                    source: error.into(),
                })?;
        }
        Ok(Self {
            file,
            path: path.to_path_buf(),
            compacted_through,
            entries,
            health: FileStoreHealth::Healthy,
            ownership: None,
        })
    }
}

enum CreationSync<'a> {
    Immediate,
    Batched(&'a mut ParentDirectorySyncBatch),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OpenMode {
    Strict,
    RepairUncommittedTail { durable_commit_index: LogIndex },
}

fn replay_entries_for_open(
    scan: RaftLogFrameScan,
    compacted_through: LogIndex,
    mode: OpenMode,
) -> Result<(ContiguousLogEntries, Option<u64>), OpenRaftLogSegmentError> {
    match mode {
        OpenMode::Strict => {
            replay_entries_strict(scan, compacted_through).map(|entries| (entries, None))
        }
        OpenMode::RepairUncommittedTail {
            durable_commit_index,
        } => {
            replay_entries_repairing_uncommitted_tail(scan, compacted_through, durable_commit_index)
        }
    }
}

fn replay_entries_strict(
    scan: RaftLogFrameScan,
    compacted_through: LogIndex,
) -> Result<ContiguousLogEntries, OpenRaftLogSegmentError> {
    let RaftLogFrameScan {
        frames,
        replay_error,
    } = scan;
    if let Some((_, error)) = replay_error {
        return Err(OpenRaftLogSegmentError::Replay(error));
    }
    let entries = frames
        .into_iter()
        .map(|frame| frame.entry)
        .filter(|entry| entry.index > compacted_through)
        .collect::<Vec<_>>();
    ContiguousLogEntries::from_entries(compacted_through.next(), entries).map_err(
        |NonContiguousRaftEntry { expected, actual }| OpenRaftLogSegmentError::NonContiguous {
            expected,
            actual,
        },
    )
}

fn replay_entries_repairing_uncommitted_tail(
    scan: RaftLogFrameScan,
    compacted_through: LogIndex,
    durable_commit_index: LogIndex,
) -> Result<(ContiguousLogEntries, Option<u64>), OpenRaftLogSegmentError> {
    let RaftLogFrameScan {
        frames,
        replay_error,
    } = scan;
    let mut expected = compacted_through.next();
    let mut entries = Vec::new();
    let mut truncate_at = None;

    for frame in frames {
        if frame.entry.index <= compacted_through {
            continue;
        }
        if frame.entry.index != expected {
            if expected <= durable_commit_index {
                return Err(OpenRaftLogSegmentError::NonContiguous {
                    expected,
                    actual: frame.entry.index,
                });
            }
            truncate_at = Some(frame.offset);
            break;
        }
        expected = expected.next();
        entries.push(frame.entry);
    }

    if truncate_at.is_none() {
        if let Some((error_offset, error)) = replay_error {
            if expected <= durable_commit_index {
                return Err(OpenRaftLogSegmentError::Replay(error));
            }
            truncate_at = Some(error_offset);
        }
    }

    let entries = ContiguousLogEntries::from_entries(compacted_through.next(), entries).map_err(
        |NonContiguousRaftEntry { expected, actual }| OpenRaftLogSegmentError::NonContiguous {
            expected,
            actual,
        },
    )?;
    Ok((entries, truncate_at))
}

fn read_compaction_marker(path: &Path) -> Result<LogIndex, OpenRaftLogSegmentError> {
    let marker_path = compaction_marker_path(path);
    if !marker_path.exists() {
        return Ok(LogIndex::ZERO);
    }
    let mut file = File::open(&marker_path).map_err(|error| OpenRaftLogSegmentError::Io {
        operation: "open raft log compaction marker",
        path: marker_path.clone(),
        source: error.into(),
    })?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|error| OpenRaftLogSegmentError::Io {
            operation: "read raft log compaction marker",
            path: marker_path,
            source: error.into(),
        })?;
    decode_raft_log_compaction_marker(&bytes).map_err(OpenRaftLogSegmentError::CompactionMarker)
}
