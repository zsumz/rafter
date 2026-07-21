//! Exclusive ownership of the standard file-backed replica directory.
//!
//! This module owns the advisory/mandatory operating-system lock used by
//! [`crate::FileRaftNodeStores`]. The persistent lock file is coordination
//! metadata, not Raft state; the lock itself lives only as long as at least one
//! split store retains the shared guard.

use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
};

use fs4::{FileExt, TryLockError};

use crate::StorageIoError;

pub(crate) const FILE_STORE_OWNERSHIP_LOCK_NAME: &str = ".rafter-storage.lock";

pub(crate) type SharedFileStoreOwnership = Arc<FileStoreOwnership>;

/// Held operating-system lock for one standard replica directory.
#[derive(Debug)]
pub(crate) struct FileStoreOwnership {
    _file: File,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum AcquireFileStoreOwnershipError {
    AlreadyHeld {
        directory: PathBuf,
    },
    Io {
        operation: &'static str,
        path: PathBuf,
        source: StorageIoError,
    },
}

pub(crate) fn acquire_file_store_ownership(
    directory: &Path,
) -> Result<SharedFileStoreOwnership, AcquireFileStoreOwnershipError> {
    let lock_path = directory.join(FILE_STORE_OWNERSHIP_LOCK_NAME);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(|source| AcquireFileStoreOwnershipError::Io {
            operation: "open raft node store ownership lock",
            path: lock_path.clone(),
            source: source.into(),
        })?;

    match FileExt::try_lock(&file) {
        Ok(()) => Ok(Arc::new(FileStoreOwnership { _file: file })),
        Err(TryLockError::WouldBlock) => Err(AcquireFileStoreOwnershipError::AlreadyHeld {
            directory: directory.to_path_buf(),
        }),
        Err(TryLockError::Error(source)) => Err(AcquireFileStoreOwnershipError::Io {
            operation: "acquire raft node store ownership lock",
            path: lock_path,
            source: source.into(),
        }),
    }
}
