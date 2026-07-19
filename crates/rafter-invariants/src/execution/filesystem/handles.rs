//! Stable directory and file capabilities with path-binding verification.

use std::{
    error::Error,
    path::{Path, PathBuf},
};

#[cfg(unix)]
use std::os::fd::{AsFd, AsRawFd, BorrowedFd};

use super::{paths::open_components, ChildDirectory, FileIdentity, HeldDirectory, HeldFile};

impl ChildDirectory {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    #[cfg(unix)]
    pub(crate) fn descriptor(&self) -> BorrowedFd<'_> {
        self.descriptor.as_fd()
    }
}

impl HeldFile {
    pub(crate) fn try_clone_std(&self) -> Result<std::fs::File, Box<dyn Error>> {
        Ok(self.file.try_clone()?.into_std())
    }

    pub(crate) fn external_path(&self) -> PathBuf {
        if self.relative.as_os_str().is_empty() {
            self.root_path.clone()
        } else {
            self.root_path.join(&self.relative)
        }
    }

    #[cfg(unix)]
    pub(crate) fn descriptor(&self) -> BorrowedFd<'_> {
        self.file.as_fd()
    }

    pub(crate) fn verify_path_binding(&self) -> Result<(), Box<dyn Error>> {
        let workspace = HeldDirectory {
            root: self.root.try_clone()?,
            dir: self.root.try_clone()?,
            root_path: self.root_path.clone(),
            relative: PathBuf::new(),
            identity: FileIdentity::from_metadata(&self.root.dir_metadata()?),
        };
        let (parent, name) = workspace.parent_and_name(&self.relative, false)?;
        let metadata = parent.dir.symlink_metadata(&name)?;
        if !metadata.is_file() || FileIdentity::from_metadata(&metadata) != self.identity {
            return Err(format!(
                "producer file changed after it was opened: {}",
                self.relative.display()
            )
            .into());
        }
        if FileIdentity::from_metadata(&self.file.metadata()?) != self.identity {
            return Err(format!(
                "producer file handle changed unexpectedly: {}",
                self.relative.display()
            )
            .into());
        }
        Ok(())
    }
}

impl HeldDirectory {
    pub(crate) fn path(&self) -> &Path {
        &self.relative
    }

    pub(crate) fn external_path(&self) -> PathBuf {
        if self.relative.as_os_str().is_empty() {
            self.root_path.clone()
        } else {
            self.root_path.join(&self.relative)
        }
    }

    pub(crate) fn identity(&self) -> &FileIdentity {
        &self.identity
    }

    pub(crate) fn file_identity(&self, path: &Path) -> Result<FileIdentity, Box<dyn Error>> {
        Ok(FileIdentity::from_metadata(
            &self.open_file(path)?.metadata()?,
        ))
    }

    pub(crate) fn directory_identity(&self, path: &Path) -> Result<FileIdentity, Box<dyn Error>> {
        Ok(self.open_dir(path)?.identity)
    }

    pub(crate) fn create_dir_all(&self, path: &Path) -> Result<Self, Box<dyn Error>> {
        self.create_all_beneath(path)
    }

    pub(crate) fn open_dir(&self, path: &Path) -> Result<Self, Box<dyn Error>> {
        self.open_beneath(path)
    }

    pub(crate) fn verify_path_binding(&self) -> Result<(), Box<dyn Error>> {
        let reopened = open_components(&self.root, &self.relative)?;
        let observed = FileIdentity::from_metadata(&reopened.dir_metadata()?);
        if observed != self.identity {
            return Err(format!(
                "producer directory changed after it was opened: {}",
                self.relative.display()
            )
            .into());
        }
        Ok(())
    }

    pub(crate) fn bind_for_child(&self) -> Result<ChildDirectory, Box<dyn Error>> {
        self.verify_path_binding()?;
        #[cfg(unix)]
        {
            let descriptor = rustix::io::fcntl_dupfd_cloexec(&self.dir, 3)?;
            #[cfg(target_os = "linux")]
            let path = PathBuf::from(format!("/proc/self/fd/{}", descriptor.as_raw_fd()));
            #[cfg(not(target_os = "linux"))]
            let path = PathBuf::from(format!("/dev/fd/{}", descriptor.as_raw_fd()));
            Ok(ChildDirectory { descriptor, path })
        }
        #[cfg(not(unix))]
        {
            Ok(ChildDirectory {
                path: self.external_path(),
            })
        }
    }
}
