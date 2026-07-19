//! Confined file opening, creation, holding, and deadline-aware reads.

use std::{error::Error, io::Read, path::Path};

use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt};
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
        let mut file = self.open_file(path)?;
        let mut bytes = Vec::new();
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            deadline.check()?;
            let read = file.read(&mut buffer)?;
            deadline.check()?;
            if read == 0 {
                break;
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

pub(crate) fn create_new_file(path: &Path) -> Result<std::fs::File, Box<dyn Error>> {
    let workspace = HeldDirectory::workspace()?;
    let relative = workspace_relative(&workspace, path)?;
    workspace.create_new_file(&relative)
}

pub(crate) fn hold_file(path: &Path) -> Result<HeldFile, Box<dyn Error>> {
    let workspace = HeldDirectory::workspace()?;
    let relative = workspace_relative(&workspace, path)?;
    workspace.hold_file(&relative)
}

pub(crate) fn read_file(path: &Path) -> Result<Vec<u8>, Box<dyn Error>> {
    let workspace = HeldDirectory::workspace()?;
    let relative = workspace_relative(&workspace, path)?;
    workspace.read(&relative)
}
