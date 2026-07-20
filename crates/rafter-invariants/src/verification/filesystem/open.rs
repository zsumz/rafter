//! No-follow component traversal, opening, and final path rebinding.

use std::{
    error::Error,
    ffi::OsString,
    path::{Component, Path},
};

use cap_fs_ext::{DirExt, FollowSymlinks, OpenOptionsFollowExt};
#[cfg(unix)]
use cap_std::fs::OpenOptionsExt;
use cap_std::{
    ambient_authority,
    fs::{Dir, OpenOptions},
};

use super::{model::FileIdentity, VerificationFile, VerificationRoot};

impl VerificationRoot {
    pub(in crate::verification) fn open(path: &Path) -> Result<Self, Box<dyn Error>> {
        let canonical = std::fs::canonicalize(path)?;
        let directory = Dir::open_ambient_dir(&canonical, ambient_authority())?;
        let identity = FileIdentity::from_metadata(&directory.dir_metadata()?);
        Ok(Self {
            directory,
            path: canonical,
            identity,
        })
    }

    pub(in crate::verification) fn hold_file(
        &self,
        path: &Path,
    ) -> Result<VerificationFile, Box<dyn Error>> {
        let components = normal_components(path)?;
        let (name, parents) = components
            .split_last()
            .ok_or_else(|| format!("verification path has no file name: {}", path.display()))?;
        let mut parent = self.directory.try_clone()?;
        for component in parents {
            parent = parent.open_dir_nofollow(component)?;
        }
        let mut options = OpenOptions::new();
        options.read(true).follow(FollowSymlinks::No);
        #[cfg(unix)]
        options.custom_flags(rustix::fs::OFlags::NONBLOCK.bits().cast_signed());
        let file = parent.open_with(name, &options)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            return Err(format!(
                "verification path is not a regular file: {}",
                path.display()
            )
            .into());
        }
        Ok(VerificationFile {
            root: self.directory.try_clone()?,
            root_path: self.path.clone(),
            root_identity: self.identity.clone(),
            file,
            relative: path.to_path_buf(),
            identity: FileIdentity::from_metadata(&metadata),
        })
    }
}

impl VerificationFile {
    pub(in crate::verification) fn verify_path_binding(&self) -> Result<(), Box<dyn Error>> {
        verify_root_binding(&self.root, &self.root_path, &self.root_identity)?;
        let components = normal_components(&self.relative)?;
        let (name, parents) = components.split_last().ok_or_else(|| {
            format!(
                "verification path has no file name: {}",
                self.relative.display()
            )
        })?;
        let mut parent = self.root.try_clone()?;
        for component in parents {
            parent = parent.open_dir_nofollow(component)?;
        }
        let path_metadata = parent.symlink_metadata(name)?;
        if !path_metadata.is_file()
            || FileIdentity::from_metadata(&path_metadata) != self.identity
            || FileIdentity::from_metadata(&self.file.metadata()?) != self.identity
        {
            return Err(format!(
                "verification file changed after it was opened: {}",
                self.relative.display()
            )
            .into());
        }
        Ok(())
    }
}

fn verify_root_binding(
    directory: &Dir,
    path: &Path,
    identity: &FileIdentity,
) -> Result<(), Box<dyn Error>> {
    let rebound = Dir::open_ambient_dir(path, ambient_authority())?;
    if std::fs::canonicalize(path)? != path
        || FileIdentity::from_metadata(&rebound.dir_metadata()?) != *identity
        || FileIdentity::from_metadata(&directory.dir_metadata()?) != *identity
    {
        return Err(format!(
            "verification root changed after it was opened: {}",
            path.display()
        )
        .into());
    }
    Ok(())
}

fn normal_components(path: &Path) -> Result<Vec<OsString>, Box<dyn Error>> {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(name) => components.push(name.to_os_string()),
            Component::Prefix(_)
            | Component::RootDir
            | Component::CurDir
            | Component::ParentDir => {
                return Err(format!(
                    "verification path must be clean and relative: {}",
                    path.display()
                )
                .into());
            }
        }
    }
    Ok(components)
}
