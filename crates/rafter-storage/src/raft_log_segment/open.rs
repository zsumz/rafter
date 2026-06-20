use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::Read,
    path::Path,
};

use rafter::LogIndex;

use super::{
    compaction_marker_path, entries_by_index, frames::RaftLogFrameScan, scan_raft_log_frames,
    FileRaftLogSegment, NonContiguousRaftEntry, OpenRaftLogSegmentError,
};
use crate::{
    durable_fs::{sync_parent_directory, ParentDirectorySyncBatch},
    raft_log_compaction::decode_raft_log_compaction_marker,
    PersistedRaftLogEntry,
};

impl FileRaftLogSegment {
    /// Opens a Raft log segment at `path` and replays existing entries.
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
                message: error.to_string(),
            })?;
        if !existed {
            match creation_sync {
                CreationSync::Immediate => {
                    sync_parent_directory(path).map_err(|error| OpenRaftLogSegmentError::Io {
                        operation: "sync raft log segment directory",
                        path: path.to_path_buf(),
                        message: error.to_string(),
                    })?;
                }
                CreationSync::Batched(batch) => batch.record_parent_of(path),
            }
        }

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|error| OpenRaftLogSegmentError::Io {
                operation: "read raft log segment",
                path: path.to_path_buf(),
                message: error.to_string(),
            })?;

        let compacted_through = read_compaction_marker(path)?;
        let (entries, repair_truncate_offset) =
            replay_entries_for_open(scan_raft_log_frames(&bytes), compacted_through, mode)?;
        if let Some(offset) = repair_truncate_offset {
            file.set_len(offset as u64)
                .and_then(|()| file.sync_all())
                .map_err(|error| OpenRaftLogSegmentError::Io {
                    operation: "truncate corrupt uncommitted raft log tail",
                    path: path.to_path_buf(),
                    message: error.to_string(),
                })?;
        }
        Ok(Self {
            file,
            path: path.to_path_buf(),
            compacted_through,
            entries,
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
) -> Result<(BTreeMap<LogIndex, PersistedRaftLogEntry>, Option<usize>), OpenRaftLogSegmentError> {
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
) -> Result<BTreeMap<LogIndex, PersistedRaftLogEntry>, OpenRaftLogSegmentError> {
    if let Some(error) = scan.error {
        return Err(OpenRaftLogSegmentError::Replay(error));
    }
    let entries = scan
        .frames
        .into_iter()
        .map(|frame| frame.entry)
        .filter(|entry| entry.index > compacted_through)
        .collect::<Vec<_>>();
    entries_by_index(&entries, compacted_through.next()).map_err(
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
) -> Result<(BTreeMap<LogIndex, PersistedRaftLogEntry>, Option<usize>), OpenRaftLogSegmentError> {
    let mut expected = compacted_through.next();
    let mut entries = Vec::new();
    let mut truncate_at = None;

    for frame in scan.frames {
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
        if let Some(error) = scan.error {
            if expected <= durable_commit_index {
                return Err(OpenRaftLogSegmentError::Replay(error));
            }
            truncate_at = Some(error.offset());
        }
    }

    let entries = entries_by_index(&entries, compacted_through.next()).map_err(
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
        message: error.to_string(),
    })?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .map_err(|error| OpenRaftLogSegmentError::Io {
            operation: "read raft log compaction marker",
            path: marker_path,
            message: error.to_string(),
        })?;
    decode_raft_log_compaction_marker(&bytes).map_err(OpenRaftLogSegmentError::CompactionMarker)
}
