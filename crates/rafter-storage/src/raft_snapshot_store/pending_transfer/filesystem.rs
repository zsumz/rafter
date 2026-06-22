use std::{
    fs::{self, OpenOptions},
    io::Write,
    path::Path,
};

use crate::durable_fs::sync_parent_directory;

use super::super::RaftSnapshotStoreWriteError;

pub(super) fn write_temp_and_rename(
    temp_path: &Path,
    final_path: &Path,
    bytes: &[u8],
    open_operation: &'static str,
    write_operation: &'static str,
    rename_operation: &'static str,
    sync_operation: &'static str,
) -> Result<(), RaftSnapshotStoreWriteError> {
    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(temp_path)
            .map_err(|error| RaftSnapshotStoreWriteError::Io {
                operation: open_operation,
                path: temp_path.to_path_buf(),
                message: error.to_string(),
            })?;
        file.write_all(bytes)
            .and_then(|()| file.sync_data())
            .map_err(|error| RaftSnapshotStoreWriteError::Io {
                operation: write_operation,
                path: temp_path.to_path_buf(),
                message: error.to_string(),
            })?;
    }

    fs::rename(temp_path, final_path).map_err(|error| RaftSnapshotStoreWriteError::Io {
        operation: rename_operation,
        path: final_path.to_path_buf(),
        message: error.to_string(),
    })?;
    sync_parent_directory(final_path).map_err(|error| RaftSnapshotStoreWriteError::Io {
        operation: sync_operation,
        path: final_path.to_path_buf(),
        message: error.to_string(),
    })
}

pub(super) fn remove_file_if_exists(
    path: &Path,
    operation: &'static str,
) -> Result<bool, RaftSnapshotStoreWriteError> {
    match fs::remove_file(path) {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(RaftSnapshotStoreWriteError::Io {
            operation,
            path: path.to_path_buf(),
            message: error.to_string(),
        }),
    }
}
