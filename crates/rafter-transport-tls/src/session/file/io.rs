//! Durable file replacement and bounded state-file reads.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, ErrorKind, Read, Write},
    path::{Path, PathBuf},
};

use super::ownership::temp_path;

#[derive(Debug)]
pub(super) struct IoFailure {
    pub(super) operation: &'static str,
    pub(super) path: PathBuf,
    pub(super) source: io::Error,
}

pub(super) fn create_state_file(path: &Path, bytes: &[u8]) -> Result<(), IoFailure> {
    require_absent(path)?;
    let temp = temp_path(path);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp)
        .map_err(|source| failure("open transport session state temp file", &temp, source))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|source| failure("write transport session state temp file", &temp, source))?;
    #[cfg(test)]
    super::failpoint::check(super::failpoint::DurabilityPoint::CreateAfterTempSync)
        .map_err(|source| failure("write transport session state temp file", &temp, source))?;
    drop(file);

    require_absent(path)?;
    fs::rename(&temp, path)
        .map_err(|source| failure("publish transport session state", path, source))?;
    #[cfg(test)]
    super::failpoint::check(super::failpoint::DurabilityPoint::CreateAfterRename)
        .map_err(|source| failure("publish transport session state", path, source))?;

    sync_parent_directory(path)
        .map_err(|source| failure("sync transport session state directory", path, source))?;
    #[cfg(test)]
    super::failpoint::check(super::failpoint::DurabilityPoint::CreateAfterDirectorySync)
        .map_err(|source| failure("sync transport session state directory", path, source))?;
    Ok(())
}

pub(super) fn replace_state_file(path: &Path, bytes: &[u8]) -> Result<(), IoFailure> {
    let temp = temp_path(path);
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&temp)
        .map_err(|source| failure("open transport session state temp file", &temp, source))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|source| failure("write transport session state temp file", &temp, source))?;
    #[cfg(test)]
    super::failpoint::check(super::failpoint::DurabilityPoint::ReplaceAfterTempSync)
        .map_err(|source| failure("write transport session state temp file", &temp, source))?;
    drop(file);

    fs::rename(&temp, path)
        .map_err(|source| failure("replace transport session state", path, source))?;
    #[cfg(test)]
    super::failpoint::check(super::failpoint::DurabilityPoint::ReplaceAfterRename)
        .map_err(|source| failure("replace transport session state", path, source))?;

    sync_parent_directory(path)
        .map_err(|source| failure("sync transport session state directory", path, source))?;
    #[cfg(test)]
    super::failpoint::check(super::failpoint::DurabilityPoint::ReplaceAfterDirectorySync)
        .map_err(|source| failure("sync transport session state directory", path, source))?;
    Ok(())
}

pub(super) fn read_bounded(path: &Path, maximum: usize) -> Result<Vec<u8>, IoFailure> {
    let file =
        File::open(path).map_err(|source| failure("open transport session state", path, source))?;
    let read_limit = match u64::try_from(maximum) {
        Ok(maximum) => maximum.saturating_add(1),
        Err(_) => u64::MAX,
    };
    let mut bytes = Vec::new();
    file.take(read_limit)
        .read_to_end(&mut bytes)
        .map_err(|source| failure("read transport session state", path, source))?;
    Ok(bytes)
}

fn require_absent(path: &Path) -> Result<(), IoFailure> {
    match fs::symlink_metadata(path) {
        Ok(_) => Err(failure(
            "create transport session state",
            path,
            io::Error::from(ErrorKind::AlreadyExists),
        )),
        Err(source) if source.kind() == ErrorKind::NotFound => Ok(()),
        Err(source) => Err(failure("inspect transport session state", path, source)),
    }
}

fn sync_parent_directory(path: &Path) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    File::open(parent)?.sync_all()
}

fn failure(operation: &'static str, path: &Path, source: io::Error) -> IoFailure {
    IoFailure {
        operation,
        path: path.to_path_buf(),
        source,
    }
}
