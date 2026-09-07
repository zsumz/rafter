//! No-follow reads of one cache candidate against its locked checksum.
//!
//! The archive is opened without following links, read under the deadline, and
//! reinspected for the same file identity and length, so only bytes that match
//! the authenticated lock checksum are returned.

use std::{
    fs::{self, File, OpenOptions},
    io::Read,
    path::Path,
    time::Instant,
};

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

use sha2::{Digest, Sha256};

use super::{require_time, AuthenticatedArchive, LockedPackage, MAX_ARCHIVE_BYTES};

pub(super) fn authenticate_candidate(
    path: &Path,
    package: &LockedPackage,
    deadline: Instant,
) -> Result<AuthenticatedArchive, String> {
    let name = package.archive_name();
    let mut file = open_nofollow(path)?;
    let before = file
        .metadata()
        .map_err(|error| format!("inspect registry archive {}: {error}", path.display()))?;
    if !before.is_file() || before.len() > MAX_ARCHIVE_BYTES {
        return Err(format!(
            "registry archive {} is not a bounded regular file",
            path.display()
        ));
    }
    let mut bytes = Vec::with_capacity(
        usize::try_from(before.len()).map_err(|_| "registry archive length overflow")?,
    );
    let mut buffer = vec![0_u8; 64 * 1024].into_boxed_slice();
    while u64::try_from(bytes.len()).map_err(|_| "registry archive length overflow")? < before.len()
    {
        require_time(deadline)?;
        let remaining = usize::try_from(
            before
                .len()
                .checked_sub(
                    u64::try_from(bytes.len()).map_err(|_| "registry archive length overflow")?,
                )
                .ok_or("registry archive remaining length underflow")?,
        )
        .map_err(|_| "registry archive remaining length overflow")?;
        let chunk = remaining.min(buffer.len());
        let read = file
            .read(&mut buffer[..chunk])
            .map_err(|error| format!("read registry archive {}: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        bytes.extend_from_slice(&buffer[..read]);
    }
    require_time(deadline)?;
    if u64::try_from(bytes.len()).map_err(|_| "registry archive length overflow")? != before.len() {
        return Err(format!(
            "registry archive {} changed length while it was read",
            path.display()
        ));
    }
    let after = file
        .metadata()
        .map_err(|error| format!("reinspect registry archive {}: {error}", path.display()))?;
    if !same_identity(&before, &after) {
        return Err(format!(
            "registry archive {} changed identity while it was read",
            path.display()
        ));
    }
    let digest_value = Sha256::digest(&bytes);
    if format!("{digest_value:x}") != package.checksum {
        return Err(format!(
            "registry archive {name} does not match its authenticated lock checksum"
        ));
    }
    let digest = digest_value.into();
    Ok(AuthenticatedArchive { bytes, digest })
}

#[cfg(unix)]
fn open_nofollow(path: &Path) -> Result<File, String> {
    let mut options = OpenOptions::new();
    options
        .read(true)
        .custom_flags(rustix::fs::OFlags::NOFOLLOW.bits().cast_signed());
    options
        .open(path)
        .map_err(|error| format!("open registry archive without following links: {error}"))
}

#[cfg(not(unix))]
fn open_nofollow(_path: &Path) -> Result<File, String> {
    Err("authenticated registry archive acquisition requires no-follow file opening".to_owned())
}

#[cfg(unix)]
fn same_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.dev() == right.dev()
        && left.ino() == right.ino()
        && left.len() == right.len()
        && left.mode() == right.mode()
}

#[cfg(not(unix))]
fn same_identity(left: &fs::Metadata, right: &fs::Metadata) -> bool {
    left.len() == right.len()
}
