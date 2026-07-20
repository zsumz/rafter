//! Stable platform file identities for authenticated snapshot paths.

use std::path::Path;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct FileIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(windows)]
    volume: u32,
    #[cfg(windows)]
    index: u64,
}

impl FileIdentity {
    #[cfg(unix)]
    pub(super) fn capture(path: &Path, directory: bool) -> Result<Self, String> {
        use std::os::unix::fs::MetadataExt;

        let metadata = checked_metadata(path, directory)?;
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }

    #[cfg(windows)]
    pub(super) fn capture(path: &Path, directory: bool) -> Result<Self, String> {
        use cap_std::fs::MetadataExt;

        checked_metadata(path, directory)?;
        let metadata = if directory {
            cap_std::fs::Dir::open_ambient_dir(path, cap_std::ambient_authority())
                .and_then(|directory| directory.dir_metadata())
        } else {
            cap_std::fs::File::open_ambient(path, cap_std::ambient_authority())
                .and_then(|file| file.metadata())
        }
        .map_err(|error| format!("inspect snapshot identity {}: {error}", path.display()))?;
        Ok(Self {
            volume: metadata.volume_serial_number().ok_or_else(|| {
                format!(
                    "snapshot volume identity is unavailable: {}",
                    path.display()
                )
            })?,
            index: metadata.file_index().ok_or_else(|| {
                format!("snapshot file identity is unavailable: {}", path.display())
            })?,
        })
    }

    #[cfg(not(any(unix, windows)))]
    pub(super) fn capture(path: &Path, _directory: bool) -> Result<Self, String> {
        Err(format!(
            "snapshot file identity is unsupported on this platform: {}",
            path.display()
        ))
    }
}

#[cfg(any(unix, windows))]
fn checked_metadata(path: &Path, directory: bool) -> Result<std::fs::Metadata, String> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| format!("inspect snapshot identity {}: {error}", path.display()))?;
    let matches_kind = if directory {
        metadata.file_type().is_dir()
    } else {
        metadata.file_type().is_file()
    };
    if !matches_kind {
        return Err(format!(
            "snapshot path changed file kind or became an alias: {}",
            path.display()
        ));
    }
    Ok(metadata)
}
