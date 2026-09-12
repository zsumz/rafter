//! Manifest-selected replay rejects authoritative corruption and trims only a partial final batch.
use super::{
    codec::{self, HEADER, TRAILER},
    reclamation::{self, Authority},
    state::State,
    PersistenceDomain,
};
use crate::{file_store_ownership::SharedFileStoreOwnership, RaftHardState};
use rafter::{LogIndex, RaftSnapshot};
use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};

pub(super) fn open(
    path: &Path,
    ownership: SharedFileStoreOwnership,
    current_snapshot: Option<&RaftSnapshot>,
) -> io::Result<State> {
    let directory = path
        .parent()
        .ok_or_else(|| codec::invalid("WAL parent missing"))?;
    let mut state = if let Some(manifest) = reclamation::read_manifest(directory)? {
        let checkpoint = reclamation::read_checkpoint(directory, manifest)?;
        reclamation::validate_snapshot(
            checkpoint.snapshot,
            checkpoint.compacted,
            current_snapshot,
        )?;
        let file = reclamation::open_segment(directory, manifest)?;
        State {
            file,
            path: reclamation::segment_path(directory, manifest.generation),
            authority: Authority::Generation(manifest.generation),
            snapshot_directory: directory.join("snapshots"),
            domain: PersistenceDomain::new(),
            hard: checkpoint.hard,
            entries: checkpoint.entries,
            compacted: checkpoint.compacted,
            operation: checkpoint.operation,
            poisoned: false,
            syncs: 0,
            _ownership: ownership,
            #[cfg(test)]
            fail_after_write: false,
        }
    } else {
        State {
            file: open_legacy(path)?,
            path: path.to_path_buf(),
            authority: Authority::Legacy,
            snapshot_directory: directory.join("snapshots"),
            domain: PersistenceDomain::new(),
            hard: RaftHardState::default(),
            entries: Vec::new(),
            compacted: LogIndex::ZERO,
            operation: 0,
            poisoned: false,
            syncs: 0,
            _ownership: ownership,
            #[cfg(test)]
            fail_after_write: false,
        }
    };
    let start = match state.authority {
        Authority::Legacy => codec::MAGIC.len() as u64,
        Authority::Generation(_) => reclamation::SEGMENT_HEADER as u64,
    };
    replay_suffix(&mut state, start)?;
    File::open(directory)?.sync_all()?;
    reclamation::cleanup(directory, &state.authority, false).map_err(|failure| failure.source)?;
    state.file.seek(SeekFrom::End(0))?;
    Ok(state)
}

fn open_legacy(path: &Path) -> io::Result<File> {
    let mut file = match OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)
    {
        Ok(mut file) => {
            file.write_all(codec::MAGIC)?;
            file.sync_data()?;
            File::open(
                path.parent()
                    .ok_or_else(|| codec::invalid("WAL parent missing"))?,
            )?
            .sync_all()?;
            file
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            OpenOptions::new().read(true).write(true).open(path)?
        }
        Err(error) => return Err(error),
    };
    file.seek(SeekFrom::Start(0))?;
    let mut magic = [0; 8];
    file.read_exact(&mut magic)?;
    if &magic != codec::MAGIC {
        return Err(codec::invalid(
            "unsupported Raft WAL format; no implicit conversion",
        ));
    }
    Ok(file)
}

fn replay_suffix(state: &mut State, start: u64) -> io::Result<()> {
    let length = state.file.metadata()?.len();
    state.file.seek(SeekFrom::Start(start))?;
    let mut offset = start;
    while offset < length {
        if length - offset < HEADER as u64 {
            break;
        }
        let mut header = [0; HEADER];
        state.file.read_exact(&mut header)?;
        let size = codec::body_size(&header)?;
        if length - offset - (HEADER as u64) < (size + TRAILER) as u64 {
            break;
        }
        let mut body = vec![0; size];
        state.file.read_exact(&mut body)?;
        let mut trailer = [0; TRAILER];
        state.file.read_exact(&mut trailer)?;
        let record = codec::decode(&body, trailer)?;
        if record.operation
            != state
                .operation
                .checked_add(1)
                .ok_or_else(|| codec::invalid("WAL operation overflow"))?
        {
            return Err(codec::invalid("WAL publication sequence is not contiguous"));
        }
        state
            .validate(&record)
            .map_err(|e| codec::invalid(e.to_string()))?;
        state.apply(record);
        offset += (HEADER + size + TRAILER) as u64;
    }
    if offset != length {
        state.file.set_len(offset)?;
    }
    state.file.sync_data()?;
    Ok(())
}
