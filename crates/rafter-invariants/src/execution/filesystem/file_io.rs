//! Confined file opening, creation, holding, and deadline-aware reads.

use std::{error::Error, io::Read, path::Path};

#[cfg(not(unix))]
use std::io::{Seek, SeekFrom};
#[cfg(unix)]
use std::os::unix::fs::FileExt;

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt};
#[cfg(unix)]
use cap_std::fs::OpenOptionsExt;
use cap_std::fs::{File, OpenOptions};

use super::{
    paths::{join_relative, workspace_relative},
    FileIdentity, HeldDirectory, HeldFile, OperationDeadline,
};

impl HeldDirectory {
    pub(crate) fn open_file(&self, path: &Path) -> Result<File, Box<dyn Error>> {
        let (parent, name) = self.parent_and_name(path, false)?;
        let mut options = OpenOptions::new();
        options.read(true).follow(FollowSymlinks::No);
        #[cfg(unix)]
        options.custom_flags(rustix::fs::OFlags::NONBLOCK.bits().cast_signed());
        let file = parent.dir.open_with(&name, &options)?;
        if !file.metadata()?.is_file() {
            return Err(format!("producer path is not a regular file: {}", path.display()).into());
        }
        Ok(file)
    }

    pub(crate) fn hold_file(&self, path: &Path) -> Result<HeldFile, Box<dyn Error>> {
        let relative = join_relative(&self.relative, path)?;
        let file = self.open_file(path)?;
        let identity = FileIdentity::from_metadata(&file.metadata()?);
        Ok(HeldFile {
            root: self.root.try_clone()?,
            file,
            root_path: self.root_path.clone(),
            relative,
            identity,
        })
    }

    pub(crate) fn create_new_file(&self, path: &Path) -> Result<std::fs::File, Box<dyn Error>> {
        let (parent, name) = self.parent_and_name(path, true)?;
        let mut options = OpenOptions::new();
        options
            .create_new(true)
            .write(true)
            .follow(FollowSymlinks::No);
        Ok(parent.dir.open_with(&name, &options)?.into_std())
    }

    pub(crate) fn create_new_held_file(&self, path: &Path) -> Result<HeldFile, Box<dyn Error>> {
        let relative = join_relative(&self.relative, path)?;
        let (parent, name) = self.parent_and_name(path, true)?;
        let mut options = OpenOptions::new();
        options
            .create_new(true)
            .read(true)
            .write(true)
            .follow(FollowSymlinks::No);
        let file = parent.dir.open_with(&name, &options)?;
        let identity = FileIdentity::from_metadata(&file.metadata()?);
        Ok(HeldFile {
            root: self.root.try_clone()?,
            file,
            root_path: self.root_path.clone(),
            relative,
            identity,
        })
    }

    pub(crate) fn read(&self, path: &Path) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut file = self.open_file(path)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        Ok(bytes)
    }

    pub(crate) fn read_with_deadline(
        &self,
        path: &Path,
        deadline: OperationDeadline,
    ) -> Result<Vec<u8>, Box<dyn Error>> {
        self.read_bounded(path, deadline, u64::MAX)
    }

    pub(crate) fn read_bounded(
        &self,
        path: &Path,
        deadline: OperationDeadline,
        maximum_bytes: u64,
    ) -> Result<Vec<u8>, Box<dyn Error>> {
        let mut file = self.open_file(path)?;
        let length = file.metadata()?.len();
        if length > maximum_bytes {
            return Err(format!(
                "producer file {} is {length} bytes, exceeding the {maximum_bytes}-byte limit",
                path.display()
            )
            .into());
        }
        let mut bytes = Vec::with_capacity(usize::try_from(length)?);
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            deadline.check()?;
            let read = file.read(&mut buffer)?;
            deadline.check()?;
            if read == 0 {
                break;
            }
            let next_length = u64::try_from(bytes.len())?
                .checked_add(u64::try_from(read)?)
                .ok_or("producer file read length overflowed u64")?;
            if next_length > maximum_bytes {
                return Err(format!(
                    "producer file {} exceeded the {maximum_bytes}-byte limit while reading",
                    path.display()
                )
                .into());
            }
            bytes.extend_from_slice(&buffer[..read]);
        }
        Ok(bytes)
    }

    pub(crate) fn read_to_string_with_deadline(
        &self,
        path: &Path,
        deadline: OperationDeadline,
    ) -> Result<String, Box<dyn Error>> {
        Ok(String::from_utf8(self.read_with_deadline(path, deadline)?)?)
    }
}

impl HeldFile {
    pub(crate) fn read_bounded(
        &self,
        deadline: OperationDeadline,
        maximum_bytes: u64,
    ) -> Result<Vec<u8>, Box<dyn Error>> {
        deadline.check()?;
        let metadata = self.file.metadata()?;
        if !metadata.is_file() || FileIdentity::from_metadata(&metadata) != self.identity {
            return Err(format!(
                "producer file capability changed unexpectedly: {}",
                self.external_path().display()
            )
            .into());
        }
        let length = metadata.len();
        if length > maximum_bytes {
            return Err(format!(
                "producer file {} is {length} bytes, exceeding the {maximum_bytes}-byte limit",
                self.external_path().display()
            )
            .into());
        }

        let mut bytes = Vec::with_capacity(usize::try_from(length)?);
        let mut buffer = vec![0_u8; 64 * 1024];
        let mut offset = 0_u64;
        loop {
            deadline.check()?;
            #[cfg(unix)]
            let read = self
                .file
                .try_clone()?
                .into_std()
                .read_at(&mut buffer, offset)?;
            #[cfg(not(unix))]
            let read = {
                let mut file = self.file.try_clone()?.into_std();
                file.seek(SeekFrom::Start(offset))?;
                file.read(&mut buffer)?
            };
            deadline.check()?;
            if read == 0 {
                break;
            }
            let read = u64::try_from(read)?;
            offset = offset
                .checked_add(read)
                .ok_or("producer file read length overflowed u64")?;
            if offset > maximum_bytes {
                return Err(format!(
                    "producer file {} exceeded the {maximum_bytes}-byte limit while reading",
                    self.external_path().display()
                )
                .into());
            }
            bytes.extend_from_slice(&buffer[..usize::try_from(read)?]);
        }
        Ok(bytes)
    }

    pub(crate) fn read_to_string_bounded(
        &self,
        deadline: OperationDeadline,
        maximum_bytes: u64,
    ) -> Result<String, Box<dyn Error>> {
        Ok(String::from_utf8(
            self.read_bounded(deadline, maximum_bytes)?,
        )?)
    }
}

pub(crate) fn hold_file(path: &Path) -> Result<HeldFile, Box<dyn Error>> {
    let workspace = HeldDirectory::workspace()?;
    let relative = workspace_relative(&workspace, path)?;
    workspace.hold_file(&relative)
}

pub(crate) fn read_file_bounded(
    path: &Path,
    maximum_bytes: u64,
) -> Result<Vec<u8>, Box<dyn Error>> {
    let workspace = HeldDirectory::workspace()?;
    let relative = workspace_relative(&workspace, path)?;
    workspace.read_bounded(
        &relative,
        OperationDeadline::none("bounded workspace file read"),
        maximum_bytes,
    )
}
