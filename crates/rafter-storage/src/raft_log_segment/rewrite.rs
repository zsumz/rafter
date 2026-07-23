//! Streamed log replacement and compacted-prefix marker publication.
//!
//! This module prepares replacement files one RFLE frame at a time, syncs them
//! before a logical commit can be published, replaces stable paths, reopens the
//! append handle, and derives filesystem paths. It does not decide which entries
//! belong in the logical retained suffix.

use std::{
    fs::{self, OpenOptions},
    io::{self, Write},
    path::{Path, PathBuf},
};

use rafter::LogIndex;

use crate::{
    durable_fs::sync_parent_directory, raft_log_compaction::encode_raft_log_compaction_marker,
    EncodeRaftLogEntryError, PersistedRaftLogEntry,
};

use super::{write_raft_log_frames, FileRaftLogSegment, LogIoFailure, WriteRaftLogFramesError};

#[derive(Debug)]
pub(super) enum PrepareLogRewriteError {
    Encode(EncodeRaftLogEntryError),
    Io {
        operation: &'static str,
        source: io::Error,
    },
}

#[derive(Debug)]
pub(super) struct PreparedLogRewrite {
    temp_path: PathBuf,
}

#[cfg(test)]
static FAIL_NEXT_LOG_REWRITE_PUBLICATION: std::sync::Mutex<Vec<std::thread::ThreadId>> =
    std::sync::Mutex::new(Vec::new());

#[cfg(test)]
pub(super) fn inject_log_rewrite_publication_failure() {
    FAIL_NEXT_LOG_REWRITE_PUBLICATION
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push(std::thread::current().id());
}

#[cfg(test)]
fn take_log_rewrite_publication_failure() -> bool {
    let current = std::thread::current().id();
    let mut failures = FAIL_NEXT_LOG_REWRITE_PUBLICATION
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(index) = failures.iter().position(|thread| *thread == current) else {
        return false;
    };
    failures.swap_remove(index);
    true
}

#[cfg(not(test))]
const fn take_log_rewrite_publication_failure() -> bool {
    false
}

/// Streams a complete replacement segment to a synced temporary file.
///
/// No stable artifact changes before this returns successfully.
pub(super) fn prepare_log_rewrite(
    temp_path: PathBuf,
    entries: &[PersistedRaftLogEntry],
) -> Result<PreparedLogRewrite, PrepareLogRewriteError> {
    let mut temp = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp_path)
        .map_err(|source| PrepareLogRewriteError::Io {
            operation: "open rewritten raft log segment",
            source,
        })?;

    match write_raft_log_frames(&mut temp, entries) {
        Ok(()) => {}
        Err(WriteRaftLogFramesError::Encode(error)) => {
            drop(temp);
            let _ = fs::remove_file(&temp_path);
            return Err(PrepareLogRewriteError::Encode(error));
        }
        Err(WriteRaftLogFramesError::Io(source)) => {
            return Err(PrepareLogRewriteError::Io {
                operation: "write rewritten raft log segment",
                source,
            });
        }
    }
    temp.sync_data()
        .map_err(|source| PrepareLogRewriteError::Io {
            operation: "write rewritten raft log segment",
            source,
        })?;
    #[cfg(test)]
    crate::storage_failpoint_test::check(
        crate::storage_failpoint_test::DurabilityPoint::LogRewriteAfterTempSync,
    )
    .map_err(|source| PrepareLogRewriteError::Io {
        operation: "write rewritten raft log segment",
        source,
    })?;
    drop(temp);

    Ok(PreparedLogRewrite { temp_path })
}

impl FileRaftLogSegment {
    /// Atomically selects a prepared replacement and restores the append handle.
    pub(super) fn publish_log_rewrite(
        &mut self,
        prepared: &PreparedLogRewrite,
    ) -> Result<(), LogIoFailure> {
        if take_log_rewrite_publication_failure() {
            return Err(self.record_io_failure(
                "replace raft log segment",
                io::Error::other("injected log rewrite publication failure"),
            ));
        }
        #[cfg(test)]
        if let Err(error) = crate::storage_failpoint_test::check(
            crate::storage_failpoint_test::DurabilityPoint::LogRewriteBeforeRename,
        ) {
            return Err(self.record_io_failure("replace raft log segment", error));
        }

        let path = self.path.clone();
        if let Err(error) = fs::rename(&prepared.temp_path, &path) {
            return Err(self.record_io_failure("replace raft log segment", error));
        }
        #[cfg(test)]
        if let Err(error) = crate::storage_failpoint_test::check(
            crate::storage_failpoint_test::DurabilityPoint::LogRewriteAfterRename,
        ) {
            return Err(self.record_io_failure("replace raft log segment", error));
        }
        if let Err(error) = sync_parent_directory(&path) {
            return Err(self.record_io_failure("sync raft log segment directory", error));
        }
        #[cfg(test)]
        if let Err(error) = crate::storage_failpoint_test::check(
            crate::storage_failpoint_test::DurabilityPoint::LogRewriteAfterDirectorySync,
        ) {
            return Err(self.record_io_failure("sync raft log segment directory", error));
        }

        let reopened = match OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
        {
            Ok(file) => file,
            Err(error) => {
                return Err(self.record_io_failure("reopen raft log segment", error));
            }
        };
        if let Err(error) = reopened.sync_data() {
            return Err(self.record_io_failure("sync rewritten raft log segment", error));
        }
        self.file = reopened;
        Ok(())
    }

    pub(super) fn temp_rewrite_path(&self) -> PathBuf {
        self.path
            .with_extension(format!("rewrite-{}.tmp", std::process::id()))
    }

    pub(super) fn publish_compaction_marker(
        &mut self,
        compacted_through: LogIndex,
    ) -> Result<(), LogIoFailure> {
        let bytes = encode_raft_log_compaction_marker(compacted_through);
        let temp_path = self.temp_compaction_marker_path();
        let marker_path = self.compaction_marker_path();
        let mut temp = match OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp_path)
        {
            Ok(temp) => temp,
            Err(error) => {
                return Err(self.record_io_failure("open raft log compaction marker", error));
            }
        };
        if let Err(error) = temp.write_all(&bytes).and_then(|()| temp.sync_data()) {
            return Err(self.record_io_failure("write raft log compaction marker", error));
        }
        #[cfg(test)]
        if let Err(error) = crate::storage_failpoint_test::check(
            crate::storage_failpoint_test::DurabilityPoint::LogMarkerAfterTempSync,
        ) {
            return Err(self.record_io_failure("write raft log compaction marker", error));
        }
        drop(temp);

        if let Err(error) = fs::rename(&temp_path, &marker_path) {
            return Err(self.record_io_failure("replace raft log compaction marker", error));
        }
        #[cfg(test)]
        if let Err(error) = crate::storage_failpoint_test::check(
            crate::storage_failpoint_test::DurabilityPoint::LogMarkerAfterRename,
        ) {
            return Err(self.record_io_failure("replace raft log compaction marker", error));
        }
        if let Err(error) = sync_parent_directory(&marker_path) {
            return Err(self.record_io_failure("sync raft log compaction marker directory", error));
        }
        #[cfg(test)]
        if let Err(error) = crate::storage_failpoint_test::check(
            crate::storage_failpoint_test::DurabilityPoint::LogMarkerAfterDirectorySync,
        ) {
            return Err(self.record_io_failure("sync raft log compaction marker directory", error));
        }
        Ok(())
    }

    pub(super) fn compaction_marker_path(&self) -> PathBuf {
        compaction_marker_path(&self.path)
    }

    pub(super) fn temp_compaction_marker_path(&self) -> PathBuf {
        let mut temp = self.compaction_marker_path().into_os_string();
        temp.push(format!(".{}.tmp", std::process::id()));
        PathBuf::from(temp)
    }
}

pub(super) fn compaction_marker_path(path: &Path) -> PathBuf {
    let mut marker = path.as_os_str().to_os_string();
    marker.push(".compact");
    PathBuf::from(marker)
}
