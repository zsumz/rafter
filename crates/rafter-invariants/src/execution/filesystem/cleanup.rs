//! Bounded, identity-checked tree replacement and removal.

use std::{
    cmp::Reverse,
    collections::BTreeMap,
    error::Error,
    io,
    path::{Path, PathBuf},
};

use cap_fs_ext::{DirExt, FollowSymlinks, OpenOptionsFollowExt};
use cap_std::fs::OpenOptions;

use super::{
    paths::{split_parent, workspace_relative},
    traversal::TraversalBudget,
    EntryKind, FileIdentity, HeldDirectory, OperationDeadline, TreeLimits,
};

impl HeldDirectory {
    pub(crate) fn replace_tree(
        path: &Path,
        limits: TreeLimits,
        deadline: OperationDeadline,
    ) -> Result<Self, Box<dyn Error>> {
        Self::replace_tree_with(path, limits, deadline, || {})
    }

    #[cfg(test)]
    pub(crate) fn replace_tree_with_hook<F>(
        path: &Path,
        limits: TreeLimits,
        deadline: OperationDeadline,
        hook: F,
    ) -> Result<Self, Box<dyn Error>>
    where
        F: FnOnce(),
    {
        Self::replace_tree_with(path, limits, deadline, hook)
    }

    fn replace_tree_with<F>(
        path: &Path,
        limits: TreeLimits,
        deadline: OperationDeadline,
        hook: F,
    ) -> Result<Self, Box<dyn Error>>
    where
        F: FnOnce(),
    {
        deadline.check()?;
        let workspace = Self::workspace()?;
        let path = workspace_relative(&workspace, path)?;
        let (parent_path, name) = split_parent(&path)?;
        let parent = workspace.create_all_beneath(&parent_path)?;
        deadline.check()?;
        match parent.dir.symlink_metadata(&name) {
            Ok(metadata) if metadata.file_type().is_symlink() => {
                parent.dir.remove_file_or_symlink(&name)?;
            }
            Ok(metadata) if metadata.is_dir() => {
                let existing = parent.open_child(&name)?;
                hook();
                existing.remove_contents(limits, deadline)?;
                existing.verify_path_binding()?;
                existing.dir.remove_open_dir()?;
            }
            Ok(_) => {
                return Err(format!(
                    "producer scratch path is not a directory: {}",
                    path.display()
                )
                .into());
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        deadline.check()?;
        parent.dir.create_dir(&name)?;
        let created = parent.open_child(&name)?;
        created.verify_path_binding()?;
        Ok(created)
    }

    pub(crate) fn remove_file_if_exists(&self, path: &Path) -> Result<(), Box<dyn Error>> {
        let (parent, name) = match self.parent_and_name(path, false) {
            Ok(value) => value,
            Err(error)
                if error
                    .downcast_ref::<io::Error>()
                    .is_some_and(|error| error.kind() == io::ErrorKind::NotFound) =>
            {
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        match parent.dir.remove_file_or_symlink(&name) {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn remove_file_if_identity(
        &self,
        path: &Path,
        expected: &FileIdentity,
    ) -> Result<(), Box<dyn Error>> {
        let (parent, name) = self.parent_and_name(path, false)?;
        let mut options = OpenOptions::new();
        options.read(true).follow(FollowSymlinks::No);
        let file = parent.dir.open_with(&name, &options)?;
        let observed = FileIdentity::from_metadata(&file.metadata()?);
        if &observed != expected {
            return Err(format!("producer file changed before removal: {}", path.display()).into());
        }
        drop(file);
        parent.dir.remove_file(&name)?;
        Ok(())
    }

    pub(crate) fn remove_dir_if_identity(
        &self,
        path: &Path,
        expected: &FileIdentity,
    ) -> Result<(), Box<dyn Error>> {
        let directory = self.open_dir(path)?;
        if directory.identity() != expected {
            return Err(format!(
                "producer directory changed before removal: {}",
                path.display()
            )
            .into());
        }
        directory.verify_path_binding()?;
        directory.dir.remove_open_dir()?;
        Ok(())
    }

    pub(crate) fn remove_symlink(&self, path: &Path) -> Result<(), Box<dyn Error>> {
        let (parent, name) = self.parent_and_name(path, false)?;
        let metadata = parent.dir.symlink_metadata(&name)?;
        if !metadata.file_type().is_symlink() {
            return Err(format!("producer cleanup expected symlink: {}", path.display()).into());
        }
        parent.dir.remove_file_or_symlink(&name)?;
        Ok(())
    }

    pub(crate) fn remove_contents(
        &self,
        limits: TreeLimits,
        deadline: OperationDeadline,
    ) -> Result<(), Box<dyn Error>> {
        let mut budget = TraversalBudget::new(limits);
        let mut nodes = Vec::new();
        scan_cleanup_tree(self, Path::new(""), 0, &mut budget, deadline, &mut nodes)?;
        let mut ordered =
            BTreeMap::<Reverse<usize>, BTreeMap<Reverse<PathBuf>, CleanupNode>>::new();
        for node in nodes {
            deadline.check()?;
            ordered
                .entry(Reverse(node.depth))
                .or_default()
                .insert(Reverse(node.relative.clone()), node);
        }
        for by_path in ordered.into_values() {
            deadline.check()?;
            for node in by_path.into_values() {
                deadline.check()?;
                match node.kind {
                    EntryKind::Directory => self.remove_dir_if_identity(
                        &node.relative,
                        node.identity
                            .as_ref()
                            .ok_or("producer directory omitted cleanup identity")?,
                    )?,
                    EntryKind::File => self.remove_file_if_identity(
                        &node.relative,
                        node.identity
                            .as_ref()
                            .ok_or("producer file omitted cleanup identity")?,
                    )?,
                    EntryKind::Symlink => self.remove_symlink(&node.relative)?,
                }
            }
        }
        Ok(())
    }

    pub(crate) fn remove_self(self) -> Result<(), Box<dyn Error>> {
        self.verify_path_binding()?;
        self.dir.remove_open_dir()?;
        Ok(())
    }
}

pub(crate) fn remove_file(path: &Path) -> Result<(), Box<dyn Error>> {
    let workspace = HeldDirectory::workspace()?;
    let relative = workspace_relative(&workspace, path)?;
    workspace.remove_file_if_exists(&relative)
}

#[derive(Debug)]
struct CleanupNode {
    relative: PathBuf,
    kind: EntryKind,
    identity: Option<FileIdentity>,
    depth: usize,
}

fn scan_cleanup_tree(
    directory: &HeldDirectory,
    relative: &Path,
    depth: usize,
    budget: &mut TraversalBudget,
    deadline: OperationDeadline,
    nodes: &mut Vec<CleanupNode>,
) -> Result<(), Box<dyn Error>> {
    budget.enter_directory(directory.path(), depth)?;
    let entries = directory.entries(deadline)?;
    if entries.len() > budget.limits.directory_entries() {
        return Err(format!(
            "producer directory {} exceeds the entry limit of {}",
            directory.path().display(),
            budget.limits.directory_entries()
        )
        .into());
    }
    for (name, kind) in entries {
        deadline.check()?;
        let node_relative = relative.join(&name);
        let node_path = directory.path().join(&name);
        let child_depth = depth
            .checked_add(1)
            .ok_or("producer filesystem depth overflow")?;
        budget.visit(&node_path, child_depth, kind)?;
        let identity = match kind {
            EntryKind::Directory => {
                let child = directory.open_child(&name)?;
                child.verify_path_binding()?;
                scan_cleanup_tree(&child, &node_relative, child_depth, budget, deadline, nodes)?;
                Some(child.identity().clone())
            }
            EntryKind::File => Some(directory.file_identity(Path::new(&name))?),
            EntryKind::Symlink => None,
        };
        nodes.push(CleanupNode {
            relative: node_relative,
            kind,
            identity,
            depth: child_depth,
        });
    }
    deadline.check()?;
    Ok(())
}
