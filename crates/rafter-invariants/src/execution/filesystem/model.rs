//! Capability identities, traversal limits, and operation deadlines.

use std::{error::Error, fmt, path::PathBuf, time::Instant};

use cap_std::fs::{Dir, File, Metadata};

#[cfg(unix)]
use std::os::fd::OwnedFd;

pub(crate) const TREE_LIMITS: TreeLimits = TreeLimits {
    directory_entries: 16 * 1024,
    files: 64 * 1024,
    directories: 16 * 1024,
    nodes: 96 * 1024,
    depth: 64,
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct TreeLimits {
    directory_entries: usize,
    files: usize,
    directories: usize,
    nodes: usize,
    depth: usize,
}

impl TreeLimits {
    pub(crate) const fn directory_entries(self) -> usize {
        self.directory_entries
    }

    pub(crate) const fn files(self) -> usize {
        self.files
    }

    pub(crate) const fn directories(self) -> usize {
        self.directories
    }

    pub(crate) const fn nodes(self) -> usize {
        self.nodes
    }

    pub(crate) const fn depth(self) -> usize {
        self.depth
    }

    #[cfg(test)]
    pub(crate) const fn with_directory_entries(mut self, directory_entries: usize) -> Self {
        self.directory_entries = directory_entries;
        self
    }

    #[cfg(test)]
    pub(crate) const fn with_files(mut self, files: usize) -> Self {
        self.files = files;
        self
    }

    #[cfg(test)]
    pub(crate) const fn with_directories(mut self, directories: usize) -> Self {
        self.directories = directories;
        self
    }

    #[cfg(test)]
    pub(crate) const fn with_nodes(mut self, nodes: usize) -> Self {
        self.nodes = nodes;
        self
    }

    #[cfg(test)]
    pub(crate) const fn with_depth(mut self, depth: usize) -> Self {
        self.depth = depth;
        self
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct OperationDeadline {
    deadline: Option<Instant>,
    operation: &'static str,
}

impl OperationDeadline {
    #[cfg(test)]
    pub(crate) const fn none(operation: &'static str) -> Self {
        Self {
            deadline: None,
            operation,
        }
    }

    pub(crate) const fn at(deadline: Instant, operation: &'static str) -> Self {
        Self {
            deadline: Some(deadline),
            operation,
        }
    }

    pub(crate) fn check(self) -> Result<(), Box<dyn Error>> {
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(Box::new(FilesystemDeadlineError(self.operation)));
        }
        Ok(())
    }
}

#[derive(Debug)]
struct FilesystemDeadlineError(&'static str);

impl fmt::Display for FilesystemDeadlineError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "producer filesystem deadline expired during {}",
            self.0
        )
    }
}

impl Error for FilesystemDeadlineError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum EntryKind {
    Directory,
    File,
    Symlink,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FileIdentity {
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
    len: u64,
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
                len: metadata.len(),
                modified: metadata.modified().ok(),
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct HeldDirectory {
    pub(super) root: Dir,
    pub(super) dir: Dir,
    pub(super) root_path: PathBuf,
    pub(super) relative: PathBuf,
    pub(super) identity: FileIdentity,
}

#[derive(Debug)]
pub(crate) struct HeldFile {
    pub(super) root: Dir,
    pub(super) file: File,
    pub(super) root_path: PathBuf,
    pub(super) relative: PathBuf,
    pub(super) identity: FileIdentity,
}

#[derive(Debug)]
pub(crate) struct ChildDirectory {
    #[cfg(unix)]
    pub(super) descriptor: OwnedFd,
    pub(super) path: PathBuf,
}
