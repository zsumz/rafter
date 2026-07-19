//! Repository-relative path validation and descriptor-confined directory opening.

use std::{
    error::Error,
    ffi::{OsStr, OsString},
    io,
    path::{Component, Path, PathBuf},
};

use cap_fs_ext::DirExt;
use cap_std::{ambient_authority, fs::Dir};

use super::{FileIdentity, HeldDirectory};

impl HeldDirectory {
    pub(crate) fn workspace() -> Result<Self, Box<dyn Error>> {
        let root = Dir::open_ambient_dir(".", ambient_authority())?;
        let dir = root.try_clone()?;
        let identity = FileIdentity::from_metadata(&dir.dir_metadata()?);
        Ok(Self {
            root,
            dir,
            root_path: std::env::current_dir()?,
            relative: PathBuf::new(),
            identity,
        })
    }

    pub(crate) fn open(path: &Path) -> Result<Self, Box<dyn Error>> {
        let workspace = Self::workspace()?;
        let relative = workspace_relative(&workspace, path)?;
        workspace.open_beneath(&relative)
    }

    pub(crate) fn create_all(path: &Path) -> Result<Self, Box<dyn Error>> {
        let workspace = Self::workspace()?;
        let relative = workspace_relative(&workspace, path)?;
        workspace.create_all_beneath(&relative)
    }

    pub(super) fn open_beneath(&self, path: &Path) -> Result<Self, Box<dyn Error>> {
        let relative = join_relative(&self.relative, path)?;
        let dir = open_components(&self.dir, path)?;
        let identity = FileIdentity::from_metadata(&dir.dir_metadata()?);
        Ok(Self {
            root: self.root.try_clone()?,
            dir,
            root_path: self.root_path.clone(),
            relative,
            identity,
        })
    }

    pub(super) fn create_all_beneath(&self, path: &Path) -> Result<Self, Box<dyn Error>> {
        let mut dir = self.dir.try_clone()?;
        let mut relative = self.relative.clone();
        for name in normal_components(path)? {
            match dir.open_dir_nofollow(&name) {
                Ok(next) => dir = next,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    match dir.create_dir(&name) {
                        Ok(()) => {}
                        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                        Err(error) => return Err(error.into()),
                    }
                    dir = dir.open_dir_nofollow(&name)?;
                }
                Err(error) => return Err(error.into()),
            }
            relative.push(name);
        }
        let identity = FileIdentity::from_metadata(&dir.dir_metadata()?);
        Ok(Self {
            root: self.root.try_clone()?,
            dir,
            root_path: self.root_path.clone(),
            relative,
            identity,
        })
    }

    pub(super) fn open_child(&self, name: &OsStr) -> Result<Self, Box<dyn Error>> {
        validate_name(name)?;
        let dir = self.dir.open_dir_nofollow(name)?;
        let identity = FileIdentity::from_metadata(&dir.dir_metadata()?);
        Ok(Self {
            root: self.root.try_clone()?,
            dir,
            root_path: self.root_path.clone(),
            relative: self.relative.join(name),
            identity,
        })
    }

    pub(super) fn parent_and_name(
        &self,
        path: &Path,
        create_parent: bool,
    ) -> Result<(Self, OsString), Box<dyn Error>> {
        let (parent, name) = split_parent(path)?;
        let parent = if create_parent {
            self.create_all_beneath(&parent)?
        } else {
            self.open_beneath(&parent)?
        };
        Ok((parent, name))
    }
}

pub(super) fn workspace_relative(
    workspace: &HeldDirectory,
    path: &Path,
) -> Result<PathBuf, Box<dyn Error>> {
    if path.is_absolute() {
        return path
            .strip_prefix(&workspace.root_path)
            .map(Path::to_path_buf)
            .map_err(|_| {
                format!("producer path is outside the workspace: {}", path.display()).into()
            });
    }
    Ok(path.to_path_buf())
}

pub(super) fn open_components(start: &Dir, path: &Path) -> Result<Dir, Box<dyn Error>> {
    let mut dir = start.try_clone()?;
    for name in normal_components(path)? {
        dir = dir.open_dir_nofollow(name)?;
    }
    Ok(dir)
}

pub(super) fn normal_components(path: &Path) -> Result<Vec<OsString>, Box<dyn Error>> {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(name) => components.push(name.to_os_string()),
            Component::CurDir => {}
            Component::Prefix(_) | Component::RootDir | Component::ParentDir => {
                return Err(format!(
                    "producer filesystem path must be repository-relative: {}",
                    path.display()
                )
                .into());
            }
        }
    }
    Ok(components)
}

pub(super) fn join_relative(base: &Path, path: &Path) -> Result<PathBuf, Box<dyn Error>> {
    let mut joined = base.to_path_buf();
    for component in normal_components(path)? {
        joined.push(component);
    }
    Ok(joined)
}

pub(super) fn split_parent(path: &Path) -> Result<(PathBuf, OsString), Box<dyn Error>> {
    let components = normal_components(path)?;
    let (name, parent) = components
        .split_last()
        .ok_or_else(|| format!("producer filesystem path has no leaf: {}", path.display()))?;
    Ok((parent.iter().collect(), name.clone()))
}

pub(super) fn validate_name(name: &OsStr) -> Result<(), Box<dyn Error>> {
    let path = Path::new(name);
    if path.components().count() != 1
        || !matches!(path.components().next(), Some(Component::Normal(_)))
    {
        return Err(format!("producer filesystem rejected entry name {}", path.display()).into());
    }
    Ok(())
}
