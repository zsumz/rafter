//! Replace a bounded journal with one fully durable state in the same v1 format.
use crate::{
    durable_fs::sync_parent_directory,
    format::v1::hard_state_journal,
    telemetry::{measure, Stage},
};
use std::{
    fs::{self, File, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

pub(super) const RECORD_LIMIT: u64 = 4_096;

pub(super) fn temp_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".checkpoint.tmp");
    PathBuf::from(name)
}

pub(super) fn publish(path: &Path, record: &[u8]) -> io::Result<File> {
    let temporary = temp_path(path);
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temporary)?;
    let mut bytes = Vec::with_capacity(hard_state_journal::HEADER_LEN + record.len());
    bytes.extend_from_slice(&hard_state_journal::header());
    bytes.extend_from_slice(record);
    measure(Stage::HardStateWrite, || file.write_all(&bytes))?;
    #[cfg(test)]
    crate::storage_failpoint_test::check(
        crate::storage_failpoint_test::DurabilityPoint::JournalCheckpointAfterWrite,
    )?;
    measure(Stage::HardStateSync, || file.sync_data())?;
    #[cfg(test)]
    crate::storage_failpoint_test::check(
        crate::storage_failpoint_test::DurabilityPoint::JournalCheckpointAfterSync,
    )?;
    fs::rename(&temporary, path)?;
    #[cfg(test)]
    crate::storage_failpoint_test::check(
        crate::storage_failpoint_test::DurabilityPoint::JournalCheckpointAfterRename,
    )?;
    measure(Stage::HardStateDirectorySync, || {
        sync_parent_directory(path)
    })?;
    #[cfg(test)]
    crate::storage_failpoint_test::check(
        crate::storage_failpoint_test::DurabilityPoint::JournalCheckpointAfterDirectorySync,
    )?;
    Ok(file)
}
