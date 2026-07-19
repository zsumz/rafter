//! Deterministic directory classification and bounded recursive traversal.

use std::{
    collections::BTreeMap,
    error::Error,
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
};

use cap_std::fs::Metadata;

use super::{paths::validate_name, EntryKind, HeldDirectory, OperationDeadline, TreeLimits};

impl HeldDirectory {
    pub(crate) fn entry_kind(&self, name: &OsStr) -> Result<Option<EntryKind>, Box<dyn Error>> {
        validate_name(name)?;
        match self.dir.symlink_metadata(name) {
            Ok(metadata) => Ok(Some(classify(&metadata)?)),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    pub(crate) fn entries(
        &self,
        deadline: OperationDeadline,
    ) -> Result<Vec<(OsString, EntryKind)>, Box<dyn Error>> {
        let mut entries = BTreeMap::new();
        for entry in self.dir.entries()? {
            deadline.check()?;
            let entry = entry?;
            let name = entry.file_name();
            validate_name(&name)?;
            let kind = classify_type(entry.file_type()?)?;
            entries.insert(name, kind);
        }
        deadline.check()?;
        Ok(entries.into_iter().collect())
    }

    pub(crate) fn files_below(
        &self,
        limits: TreeLimits,
        deadline: OperationDeadline,
    ) -> Result<Vec<PathBuf>, Box<dyn Error>> {
        let mut budget = TraversalBudget::new(limits);
        let mut files = Vec::new();
        collect_files(self, Path::new(""), 0, &mut budget, deadline, &mut files)?;
        deadline.check()?;
        files.sort();
        deadline.check()?;
        Ok(files)
    }
}

fn collect_files(
    directory: &HeldDirectory,
    relative: &Path,
    depth: usize,
    budget: &mut TraversalBudget,
    deadline: OperationDeadline,
    files: &mut Vec<PathBuf>,
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
        match kind {
            EntryKind::Directory => {
                let child = directory.open_child(&name)?;
                collect_files(&child, &node_relative, child_depth, budget, deadline, files)?;
            }
            EntryKind::File => files.push(node_relative),
            EntryKind::Symlink => {
                return Err(
                    format!("producer traversal rejects symlink {}", node_path.display()).into(),
                );
            }
        }
    }
    Ok(())
}

pub(super) struct TraversalBudget {
    pub(super) limits: TreeLimits,
    directories: usize,
    files: usize,
    nodes: usize,
}

impl TraversalBudget {
    pub(super) const fn new(limits: TreeLimits) -> Self {
        Self {
            limits,
            directories: 0,
            files: 0,
            nodes: 0,
        }
    }

    pub(super) fn enter_directory(
        &mut self,
        path: &Path,
        depth: usize,
    ) -> Result<(), Box<dyn Error>> {
        self.check_depth(path, depth)?;
        if self.directories >= self.limits.directories() {
            return Err(format!(
                "producer traversal exceeds the global directory limit of {}",
                self.limits.directories()
            )
            .into());
        }
        self.directories += 1;
        Ok(())
    }

    pub(super) fn visit(
        &mut self,
        path: &Path,
        depth: usize,
        kind: EntryKind,
    ) -> Result<(), Box<dyn Error>> {
        self.check_depth(path, depth)?;
        if self.nodes >= self.limits.nodes() {
            return Err(format!(
                "producer traversal exceeds the global node limit of {}",
                self.limits.nodes()
            )
            .into());
        }
        self.nodes += 1;
        if kind == EntryKind::File {
            if self.files >= self.limits.files() {
                return Err(format!(
                    "producer traversal exceeds the file limit of {}",
                    self.limits.files()
                )
                .into());
            }
            self.files += 1;
        }
        Ok(())
    }

    fn check_depth(&self, path: &Path, depth: usize) -> Result<(), Box<dyn Error>> {
        if depth > self.limits.depth() {
            return Err(format!(
                "producer path {} exceeds the traversal depth limit of {}",
                path.display(),
                self.limits.depth()
            )
            .into());
        }
        Ok(())
    }
}

fn classify(metadata: &Metadata) -> Result<EntryKind, Box<dyn Error>> {
    classify_type(metadata.file_type())
}

fn classify_type(file_type: cap_std::fs::FileType) -> Result<EntryKind, Box<dyn Error>> {
    if file_type.is_symlink() {
        Ok(EntryKind::Symlink)
    } else if file_type.is_dir() {
        Ok(EntryKind::Directory)
    } else if file_type.is_file() {
        Ok(EntryKind::File)
    } else {
        Err("producer filesystem rejects special filesystem nodes".into())
    }
}
