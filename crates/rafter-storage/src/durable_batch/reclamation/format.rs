//! Checksummed manifest, checkpoint, and generation-segment bytes.

use super::{Checkpoint, Manifest, SnapshotReference};
use crate::{
    checksum::RunningCrc32, crc32, decode_raft_hard_state, decode_raft_log_entry,
    encode_raft_hard_state, encode_raft_log_entry,
};
use rafter::{LogIndex, Term};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const MANIFEST_NAME: &str = "raft-wal-current";
const MANIFEST_MAGIC: &[u8; 8] = b"RFWM\0\0\0\x01";
const MANIFEST_BYTES: usize = 36;
const CHECKPOINT_MAGIC: &[u8; 8] = b"RFWC\0\0\0\x01";
const CHECKPOINT_TRAILER: &[u8; 4] = b"ENDC";
const SEGMENT_MAGIC: &[u8; 8] = b"RFWS\0\0\0\x01";
pub(crate) const SEGMENT_HEADER: usize = 28;
const HARD_STATE_BYTES: usize = 51;

pub(crate) fn manifest_path(directory: &Path) -> PathBuf {
    directory.join(MANIFEST_NAME)
}

pub(crate) fn checkpoint_path(directory: &Path, generation: u64) -> PathBuf {
    directory.join(format!("raft-wal-checkpoint-{generation:020}.rfwc"))
}

pub(crate) fn segment_path(directory: &Path, generation: u64) -> PathBuf {
    directory.join(format!("raft-wal-segment-{generation:020}.rfwb"))
}

pub(crate) fn read_manifest(directory: &Path) -> io::Result<Option<Manifest>> {
    let bytes = match fs::read(manifest_path(directory)) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if bytes.len() != MANIFEST_BYTES
        || bytes.get(..8) != Some(MANIFEST_MAGIC)
        || u32::from_be_bytes(
            bytes[32..36]
                .try_into()
                .map_err(|_| super::invalid("invalid WAL manifest checksum width"))?,
        ) != crc32(&bytes[..32])
    {
        return Err(super::invalid("corrupt or unsupported WAL manifest"));
    }
    let manifest = Manifest {
        generation: read_u64(&bytes[8..16])?,
        operation: read_u64(&bytes[16..24])?,
        compacted: LogIndex(read_u64(&bytes[24..32])?),
    };
    if manifest.generation == 0 || manifest.compacted.0 == u64::MAX {
        return Err(super::invalid("invalid WAL manifest fields"));
    }
    Ok(Some(manifest))
}

pub(crate) fn read_checkpoint(directory: &Path, manifest: Manifest) -> io::Result<Checkpoint> {
    let mut file = File::open(checkpoint_path(directory, manifest.generation))?;
    let mut crc = RunningCrc32::new();
    if &read_tracked::<8>(&mut file, &mut crc)? != CHECKPOINT_MAGIC {
        return Err(super::invalid("unsupported WAL checkpoint format"));
    }
    let generation = read_u64(&read_tracked::<8>(&mut file, &mut crc)?)?;
    let operation = read_u64(&read_tracked::<8>(&mut file, &mut crc)?)?;
    let compacted = LogIndex(read_u64(&read_tracked::<8>(&mut file, &mut crc)?)?);
    if generation != manifest.generation
        || operation != manifest.operation
        || compacted != manifest.compacted
        || compacted.0 == u64::MAX
    {
        return Err(super::invalid("WAL manifest and checkpoint disagree"));
    }
    let hard = decode_raft_hard_state(&read_tracked::<HARD_STATE_BYTES>(&mut file, &mut crc)?)
        .map_err(|error| super::invalid(error.to_string()))?;
    if read_tracked::<1>(&mut file, &mut crc)?[0] != 1 {
        return Err(super::invalid(
            "WAL checkpoint snapshot reference is missing",
        ));
    }
    let snapshot = SnapshotReference {
        last_included_index: LogIndex(read_u64(&read_tracked::<8>(&mut file, &mut crc)?)?),
        last_included_term: Term(read_u64(&read_tracked::<8>(&mut file, &mut crc)?)?),
        transfer_id: read_u64(&read_tracked::<8>(&mut file, &mut crc)?)?,
    };
    let count = read_u64(&read_tracked::<8>(&mut file, &mut crc)?)?;
    let mut entries = Vec::new();
    let mut expected = compacted.next();
    for _ in 0..count {
        let size = u32::from_be_bytes(read_tracked::<4>(&mut file, &mut crc)?) as usize;
        if size == 0 || size > super::MAX_RECORD_BODY {
            return Err(super::invalid("invalid WAL checkpoint entry size"));
        }
        let entry = decode_raft_log_entry(&read_tracked_vec(&mut file, &mut crc, size)?)
            .map_err(|error| super::invalid(error.to_string()))?;
        if entry.index != expected || entry.index.0 == u64::MAX {
            return Err(super::invalid("WAL checkpoint entries are not contiguous"));
        }
        expected = expected.next();
        entries.push(entry);
    }
    validate_checkpoint_trailer(&mut file, crc)?;
    if hard.commit_index >= expected {
        return Err(super::invalid("WAL checkpoint commit exceeds durable log"));
    }
    if snapshot.last_included_index < compacted {
        return Err(super::invalid(
            "WAL checkpoint snapshot does not cover compacted state",
        ));
    }
    Ok(Checkpoint {
        hard,
        entries,
        compacted,
        operation,
        snapshot,
    })
}

fn validate_checkpoint_trailer(file: &mut File, crc: RunningCrc32) -> io::Result<()> {
    let mut trailer = [0; 8];
    file.read_exact(&mut trailer)?;
    if u32::from_be_bytes(
        trailer[..4]
            .try_into()
            .map_err(|_| super::invalid("invalid WAL checkpoint checksum width"))?,
    ) != crc.value()
        || &trailer[4..] != CHECKPOINT_TRAILER
    {
        return Err(super::invalid("corrupt WAL checkpoint"));
    }
    let mut extra = [0; 1];
    if file.read(&mut extra)? != 0 {
        return Err(super::invalid("extra bytes after WAL checkpoint"));
    }
    Ok(())
}

pub(crate) fn open_segment(directory: &Path, manifest: Manifest) -> io::Result<File> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(segment_path(directory, manifest.generation))?;
    let mut header = [0; SEGMENT_HEADER];
    file.read_exact(&mut header)?;
    if &header[..8] != SEGMENT_MAGIC
        || read_u64(&header[8..16])? != manifest.generation
        || read_u64(&header[16..24])? != manifest.operation
        || u32::from_be_bytes(
            header[24..]
                .try_into()
                .map_err(|_| super::invalid("invalid WAL segment checksum width"))?,
        ) != crc32(&header[..24])
    {
        return Err(super::invalid("corrupt or mismatched WAL segment header"));
    }
    file.seek(SeekFrom::Start(SEGMENT_HEADER as u64))?;
    Ok(file)
}

pub(crate) fn write_checkpoint(
    path: &Path,
    generation: u64,
    state: &super::State,
    snapshot: SnapshotReference,
) -> io::Result<()> {
    let mut file = OpenOptions::new().create_new(true).write(true).open(path)?;
    let mut crc = RunningCrc32::new();
    for bytes in [
        CHECKPOINT_MAGIC.as_slice(),
        &generation.to_be_bytes(),
        &state.operation.to_be_bytes(),
        &state.compacted.0.to_be_bytes(),
        &encode_raft_hard_state(&state.hard),
        &[1],
        &snapshot.last_included_index.0.to_be_bytes(),
        &snapshot.last_included_term.0.to_be_bytes(),
        &snapshot.transfer_id.to_be_bytes(),
        &u64::try_from(state.entries.len())
            .map_err(|_| super::invalid("too many entries for WAL checkpoint"))?
            .to_be_bytes(),
    ] {
        write_tracked(&mut file, &mut crc, bytes)?;
    }
    for entry in &state.entries {
        let bytes =
            encode_raft_log_entry(entry).map_err(|error| super::invalid(error.to_string()))?;
        let size = u32::try_from(bytes.len())
            .map_err(|_| super::invalid("WAL checkpoint entry too large"))?;
        write_tracked(&mut file, &mut crc, &size.to_be_bytes())?;
        write_tracked(&mut file, &mut crc, &bytes)?;
    }
    file.write_all(&crc.value().to_be_bytes())?;
    file.write_all(CHECKPOINT_TRAILER)?;
    #[cfg(test)]
    crate::storage_failpoint_test::check(
        crate::storage_failpoint_test::DurabilityPoint::WalCheckpointAfterWrite,
    )?;
    file.sync_data()?;
    #[cfg(test)]
    crate::storage_failpoint_test::check(
        crate::storage_failpoint_test::DurabilityPoint::WalCheckpointAfterSync,
    )?;
    Ok(())
}

pub(crate) fn create_segment(path: &Path, generation: u64, operation: u64) -> io::Result<File> {
    let mut header = Vec::with_capacity(SEGMENT_HEADER);
    header.extend_from_slice(SEGMENT_MAGIC);
    header.extend_from_slice(&generation.to_be_bytes());
    header.extend_from_slice(&operation.to_be_bytes());
    header.extend_from_slice(&crc32(&header).to_be_bytes());
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(path)?;
    file.write_all(&header)?;
    file.sync_data()?;
    Ok(file)
}

pub(crate) fn encode_manifest(manifest: &Manifest) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(MANIFEST_BYTES);
    bytes.extend_from_slice(MANIFEST_MAGIC);
    bytes.extend_from_slice(&manifest.generation.to_be_bytes());
    bytes.extend_from_slice(&manifest.operation.to_be_bytes());
    bytes.extend_from_slice(&manifest.compacted.0.to_be_bytes());
    bytes.extend_from_slice(&crc32(&bytes).to_be_bytes());
    bytes
}

fn write_tracked(file: &mut File, crc: &mut RunningCrc32, bytes: &[u8]) -> io::Result<()> {
    file.write_all(bytes)?;
    crc.update(bytes);
    Ok(())
}

fn read_tracked<const N: usize>(file: &mut File, crc: &mut RunningCrc32) -> io::Result<[u8; N]> {
    let mut bytes = [0; N];
    file.read_exact(&mut bytes)?;
    crc.update(&bytes);
    Ok(bytes)
}

fn read_tracked_vec(file: &mut File, crc: &mut RunningCrc32, size: usize) -> io::Result<Vec<u8>> {
    let mut bytes = vec![0; size];
    file.read_exact(&mut bytes)?;
    crc.update(&bytes);
    Ok(bytes)
}

fn read_u64(bytes: &[u8]) -> io::Result<u64> {
    Ok(u64::from_be_bytes(bytes.try_into().map_err(|_| {
        super::invalid("invalid WAL integer width")
    })?))
}
