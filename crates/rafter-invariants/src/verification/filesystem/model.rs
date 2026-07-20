//! Stable file capabilities and platform file identities.

use std::{error::Error, path::PathBuf};

use cap_std::fs::{Dir, File, Metadata};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::verification) struct FileIdentity {
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(windows)]
    volume: Option<u32>,
    #[cfg(windows)]
    index: Option<u64>,
    #[cfg(not(any(unix, windows)))]
    file_type: cap_std::fs::FileType,
    #[cfg(not(any(unix, windows)))]
    length: u64,
    #[cfg(not(any(unix, windows)))]
    modified: Option<std::time::SystemTime>,
}

impl FileIdentity {
    pub(super) fn from_metadata(metadata: &Metadata) -> Self {
        #[cfg(unix)]
        {
            use cap_std::fs::MetadataExt;
            Self {
                device: metadata.dev(),
                inode: metadata.ino(),
            }
        }
        #[cfg(windows)]
        {
            use cap_std::fs::MetadataExt;
            Self {
                volume: metadata.volume_serial_number(),
                index: metadata.file_index(),
            }
        }
        #[cfg(not(any(unix, windows)))]
        {
            Self {
                file_type: metadata.file_type(),
                length: metadata.len(),
                modified: metadata.modified().ok(),
            }
        }
    }
}

pub(in crate::verification) struct VerificationRoot {
    pub(super) directory: Dir,
    pub(super) path: PathBuf,
    pub(super) identity: FileIdentity,
}

pub(in crate::verification) struct VerificationFile {
    pub(super) root: Dir,
    pub(super) root_path: PathBuf,
    pub(super) root_identity: FileIdentity,
    pub(super) file: File,
    pub(super) relative: PathBuf,
    pub(super) identity: FileIdentity,
}

impl VerificationFile {
    pub(in crate::verification) fn identity(&self) -> &FileIdentity {
        &self.identity
    }

    pub(in crate::verification) fn try_clone_std(&self) -> Result<std::fs::File, Box<dyn Error>> {
        Ok(self.file.try_clone()?.into_std())
    }
}
