//! Exclusive cooperating-process ownership of one session-state path.

use std::{
    fs::{File, OpenOptions},
    io,
    path::{Path, PathBuf},
};

use fs4::{FileExt, TryLockError};

#[derive(Debug)]
pub(super) struct SessionStoreOwnership {
    _file: File,
}

#[derive(Debug)]
pub(super) enum AcquireOwnershipError {
    AlreadyHeld,
    Io {
        operation: &'static str,
        path: PathBuf,
        source: io::Error,
    },
}

pub(super) fn acquire(path: &Path) -> Result<SessionStoreOwnership, AcquireOwnershipError> {
    let lock_path = lock_path(path);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|source| AcquireOwnershipError::Io {
            operation: "open transport session state ownership lock",
            path: lock_path.clone(),
            source,
        })?;
    match FileExt::try_lock(&file) {
        Ok(()) => Ok(SessionStoreOwnership { _file: file }),
        Err(TryLockError::WouldBlock) => Err(AcquireOwnershipError::AlreadyHeld),
        Err(TryLockError::Error(source)) => Err(AcquireOwnershipError::Io {
            operation: "acquire transport session state ownership lock",
            path: lock_path,
            source,
        }),
    }
}

pub(super) fn temp_path(path: &Path) -> PathBuf {
    sibling_with_suffix(path, ".tmp")
}

fn lock_path(path: &Path) -> PathBuf {
    sibling_with_suffix(path, ".lock")
}

fn sibling_with_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    PathBuf::from(value)
}
