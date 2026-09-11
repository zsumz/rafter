//! Exclusive open, streaming validation, and durable torn-tail repair.
//!
//! No existing bytes are changed until every complete record validates. Open
//! resyncs the recovered file and its directory before exposing recovered state,
//! including after an earlier process failed during creation or publication.

use super::journal_error::OpenJournalRaftHardStateStoreError as OpenError;
use crate::{
    durable_fs::sync_parent_directory,
    format::v1::hard_state_journal::{self as format, HEADER_LEN, RECORD_LEN},
    RaftHardState,
};
use std::{
    fs::{File, OpenOptions},
    io::{self, Read, Write},
    path::Path,
};

pub(super) fn open(path: &Path) -> Result<(File, RaftHardState, u64), OpenError> {
    let (mut file, created) = match OpenOptions::new()
        .read(true)
        .append(true)
        .create_new(true)
        .open(path)
    {
        Ok(file) => (file, true),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => (
            OpenOptions::new()
                .read(true)
                .append(true)
                .open(path)
                .map_err(|error| io_error(path, "open hard-state journal", error))?,
            false,
        ),
        Err(error) => return Err(io_error(path, "create hard-state journal", error)),
    };
    let current = if created {
        file.write_all(&format::header())
            .map_err(|error| io_error(path, "write journal header", error))?;
        RaftHardState::default()
    } else {
        recover(&mut file, path)?
    };
    file.sync_data()
        .map_err(|error| io_error(path, "sync recovered journal", error))?;
    #[cfg(test)]
    crate::storage_failpoint_test::check(
        crate::storage_failpoint_test::DurabilityPoint::JournalOpenAfterSync,
    )
    .map_err(|error| io_error(path, "sync recovered journal", error))?;
    sync_parent_directory(path).map_err(|error| io_error(path, "sync journal directory", error))?;
    let records = (file
        .metadata()
        .map_err(|error| io_error(path, "inspect recovered journal", error))?
        .len()
        - HEADER_LEN as u64)
        / RECORD_LEN as u64;
    Ok((file, current, records))
}

fn recover(file: &mut File, path: &Path) -> Result<RaftHardState, OpenError> {
    let mut header = [0; HEADER_LEN];
    if read_up_to(file, &mut header)
        .map_err(|error| io_error(path, "read journal header", error))?
        != HEADER_LEN
        || header != format::header()
    {
        return Err(OpenError::InvalidHeader);
    }
    let mut current = RaftHardState::default();
    let mut offset = HEADER_LEN as u64;
    loop {
        let mut record = [0; RECORD_LEN];
        let count = read_up_to(file, &mut record)
            .map_err(|error| io_error(path, "read journal record", error))?;
        if count == 0 {
            return Ok(current);
        }
        if count < RECORD_LEN {
            file.set_len(offset)
                .map_err(|error| io_error(path, "truncate incomplete journal record", error))?;
            return Ok(current);
        }
        current = format::decode_record(&record)
            .map_err(|source| OpenError::Record { offset, source })?;
        offset += RECORD_LEN as u64;
    }
}

fn read_up_to(file: &mut File, mut bytes: &mut [u8]) -> io::Result<usize> {
    let mut count = 0;
    while !bytes.is_empty() {
        match file.read(bytes) {
            Ok(0) => break,
            Ok(n) => {
                count += n;
                bytes = &mut bytes[n..];
            }
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(count)
}

fn io_error(path: &Path, operation: &'static str, error: io::Error) -> OpenError {
    OpenError::Io {
        operation,
        path: path.to_path_buf(),
        source: error.into(),
    }
}
