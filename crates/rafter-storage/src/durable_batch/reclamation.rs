//! Generation checkpoints bound WAL replay and reclaim obsolete operation history.

use super::{codec, state::State};
use crate::{
    durable_fs::sync_parent_directory, FileRaftSnapshotStore, PersistedRaftLogEntry, RaftHardState,
    RaftSnapshotStore,
};
use rafter::{LogIndex, RaftSnapshot, Term};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

mod cleanup;
mod format;

pub(super) use cleanup::cleanup;
pub(super) use format::{
    manifest_path, open_segment, read_checkpoint, read_manifest, segment_path, SEGMENT_HEADER,
};
const MAX_RECORD_BODY: usize = codec::MAX_BODY;

fn invalid(message: impl Into<String>) -> io::Error {
    codec::invalid(message)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Authority {
    Legacy,
    Generation(u64),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct SnapshotReference {
    last_included_index: LogIndex,
    last_included_term: Term,
    transfer_id: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Manifest {
    pub generation: u64,
    pub operation: u64,
    pub compacted: LogIndex,
}

#[derive(Debug)]
pub(super) struct Checkpoint {
    pub hard: RaftHardState,
    pub entries: Vec<PersistedRaftLogEntry>,
    pub compacted: LogIndex,
    pub operation: u64,
    pub snapshot: SnapshotReference,
}

#[derive(Debug)]
pub(super) struct Prepared {
    pub manifest: Manifest,
    pub segment: File,
    pub segment_path: PathBuf,
}

#[derive(Debug)]
pub(super) struct Failure {
    pub operation: &'static str,
    pub source: io::Error,
}

pub(super) fn snapshot_reference(
    directory: &Path,
    compacted: LogIndex,
) -> io::Result<SnapshotReference> {
    let snapshots = FileRaftSnapshotStore::open(directory).map_err(io::Error::other)?;
    let snapshot = snapshots.current_snapshot().ok_or_else(|| {
        codec::invalid("WAL compaction requires a durably published current snapshot")
    })?;
    if snapshot.metadata.last_included_index < compacted {
        return Err(codec::invalid(
            "current snapshot does not cover the requested WAL compaction boundary",
        ));
    }
    Ok(reference_from_snapshot(&snapshot))
}

pub(super) fn validate_snapshot(
    reference: SnapshotReference,
    compacted: LogIndex,
    current: Option<&RaftSnapshot>,
) -> io::Result<()> {
    if reference.last_included_index < compacted {
        return Err(codec::invalid(
            "WAL checkpoint snapshot does not cover its compacted boundary",
        ));
    }
    let current = current
        .ok_or_else(|| codec::invalid("WAL checkpoint references a missing current snapshot"))?;
    if current.metadata.last_included_index < compacted {
        return Err(codec::invalid(
            "current snapshot does not cover the WAL checkpoint boundary",
        ));
    }
    if current.metadata.last_included_index == reference.last_included_index
        && (current.metadata.last_included_term != reference.last_included_term
            || current.transfer_id().0 != reference.transfer_id)
    {
        return Err(codec::invalid(
            "current snapshot conflicts with the WAL checkpoint snapshot identity",
        ));
    }
    Ok(())
}

fn reference_from_snapshot(snapshot: &RaftSnapshot) -> SnapshotReference {
    SnapshotReference {
        last_included_index: snapshot.metadata.last_included_index,
        last_included_term: snapshot.metadata.last_included_term,
        transfer_id: snapshot.transfer_id().0,
    }
}

pub(super) fn prepare(state: &State, snapshot: SnapshotReference) -> Result<Prepared, Failure> {
    let generation = match state.authority {
        Authority::Legacy => 1,
        Authority::Generation(current) => current.checked_add(1).ok_or_else(|| Failure {
            operation: "allocate WAL checkpoint generation",
            source: codec::invalid("WAL checkpoint generation exhausted"),
        })?,
    };
    let directory = state.path.parent().ok_or_else(|| Failure {
        operation: "resolve WAL checkpoint directory",
        source: invalid("opened WAL path has no parent"),
    })?;
    let checkpoint_path = format::checkpoint_path(directory, generation);
    let segment_path = format::segment_path(directory, generation);

    format::write_checkpoint(&checkpoint_path, generation, state, snapshot).map_err(|source| {
        Failure {
            operation: "write WAL checkpoint",
            source,
        }
    })?;
    let segment =
        format::create_segment(&segment_path, generation, state.operation).map_err(|source| {
            Failure {
                operation: "create WAL generation segment",
                source,
            }
        })?;
    sync_parent_directory(&checkpoint_path).map_err(|source| Failure {
        operation: "sync WAL checkpoint directory",
        source,
    })?;
    #[cfg(test)]
    crate::storage_failpoint_test::check(
        crate::storage_failpoint_test::DurabilityPoint::WalCheckpointAfterDirectorySync,
    )
    .map_err(|source| Failure {
        operation: "sync WAL checkpoint directory",
        source,
    })?;

    Ok(Prepared {
        manifest: Manifest {
            generation,
            operation: state.operation,
            compacted: state.compacted,
        },
        segment,
        segment_path,
    })
}

pub(super) fn publish_manifest(path: &Path, manifest: &Manifest) -> Result<(), Failure> {
    let directory = path.parent().ok_or_else(|| Failure {
        operation: "resolve WAL manifest directory",
        source: invalid("opened WAL path has no parent"),
    })?;
    let target = manifest_path(directory);
    let temp = directory.join(format!(".raft-wal-current-{}.tmp", std::process::id()));
    let bytes = format::encode_manifest(manifest);
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp)
            .map_err(|source| Failure {
                operation: "open WAL manifest temp file",
                source,
            })?;
        file.write_all(&bytes)
            .and_then(|()| file.sync_data())
            .map_err(|source| Failure {
                operation: "write WAL manifest temp file",
                source,
            })?;
    }
    fs::rename(&temp, &target).map_err(|source| Failure {
        operation: "replace WAL manifest",
        source,
    })?;
    #[cfg(test)]
    crate::storage_failpoint_test::check(
        crate::storage_failpoint_test::DurabilityPoint::WalManifestAfterRename,
    )
    .map_err(|source| Failure {
        operation: "replace WAL manifest",
        source,
    })?;
    sync_parent_directory(&target).map_err(|source| Failure {
        operation: "sync WAL manifest directory",
        source,
    })?;
    #[cfg(test)]
    crate::storage_failpoint_test::check(
        crate::storage_failpoint_test::DurabilityPoint::WalManifestAfterDirectorySync,
    )
    .map_err(|source| Failure {
        operation: "sync WAL manifest directory",
        source,
    })?;
    Ok(())
}
