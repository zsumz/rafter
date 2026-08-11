//! Checkpoint traversal vocabulary, global budgets, and cleanup-root indexing.

use std::{
    collections::BTreeMap,
    error::Error,
    ffi::OsString,
    path::{Path, PathBuf},
    time::Instant,
};

use crate::execution::filesystem::{FileIdentity, HeldDirectory, TreeLimits, TREE_LIMITS};

use super::super::finalization::ensure_deadline;

pub(in crate::producer::tla::checkpoint) type TraversalLimits = TreeLimits;
pub(in crate::producer::tla::checkpoint) const TRAVERSAL_LIMITS: TraversalLimits =
    TREE_LIMITS.with_directory_entries(TREE_LIMITS.files());

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::producer::tla::checkpoint) enum CheckpointNodeKind {
    Directory,
    File,
    Symlink,
}

#[derive(Debug)]
pub(in crate::producer::tla::checkpoint) struct CheckpointNode {
    pub(in crate::producer::tla::checkpoint) path: PathBuf,
    pub(in crate::producer::tla::checkpoint) kind: CheckpointNodeKind,
    pub(in crate::producer::tla::checkpoint) identity: Option<FileIdentity>,
    pub(in crate::producer::tla::checkpoint) depth: usize,
}

#[derive(Debug)]
pub(in crate::producer::tla::checkpoint) struct CheckpointTree {
    pub(in crate::producer::tla::checkpoint) nodes: Vec<CheckpointNode>,
    pub(in crate::producer::tla::checkpoint) root: HeldDirectory,
    pub(in crate::producer::tla::checkpoint) root_path: PathBuf,
}

pub(in crate::producer::tla::checkpoint) struct TraversalBudget {
    pub(in crate::producer::tla::checkpoint) limits: TraversalLimits,
    pub(in crate::producer::tla::checkpoint) directories: usize,
    pub(in crate::producer::tla::checkpoint) nodes: usize,
    pub(in crate::producer::tla::checkpoint) files: usize,
}

impl TraversalBudget {
    pub(in crate::producer::tla::checkpoint) const fn new(limits: TraversalLimits) -> Self {
        Self {
            limits,
            directories: 0,
            nodes: 0,
            files: 0,
        }
    }

    pub(in crate::producer::tla::checkpoint) fn enter_directory(
        &mut self,
        path: &Path,
        depth: usize,
    ) -> Result<(), Box<dyn Error>> {
        self.check_depth(path, depth)?;
        if self.directories >= self.limits.directories() {
            return Err(format!(
                "checkpoint traversal exceeds the global directory limit of {}",
                self.limits.directories()
            )
            .into());
        }
        self.directories += 1;
        Ok(())
    }

    pub(in crate::producer::tla::checkpoint) fn visit_node(
        &mut self,
        path: &Path,
        depth: usize,
        kind: CheckpointNodeKind,
    ) -> Result<(), Box<dyn Error>> {
        self.check_depth(path, depth)?;
        if self.nodes >= self.limits.nodes() {
            return Err(format!(
                "checkpoint traversal exceeds the global node limit of {}",
                self.limits.nodes()
            )
            .into());
        }
        self.nodes += 1;
        if kind == CheckpointNodeKind::File {
            if self.files >= self.limits.files() {
                return Err(format!(
                    "checkpoint inventory exceeds the total file limit of {}",
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
                "checkpoint path {} exceeds the traversal depth limit of {}",
                path.display(),
                self.limits.depth()
            )
            .into());
        }
        Ok(())
    }
}

#[derive(Default)]
pub(in crate::producer::tla::checkpoint) struct RootIndex {
    pub(in crate::producer::tla::checkpoint) terminal: bool,
    pub(in crate::producer::tla::checkpoint) children: BTreeMap<OsString, RootIndex>,
}

impl RootIndex {
    pub(in crate::producer::tla::checkpoint) fn new(
        root: &Path,
        selected: &[PathBuf],
        deadline: Instant,
    ) -> Result<Self, Box<dyn Error>> {
        let mut index = Self::default();
        for path in selected {
            ensure_deadline(deadline, "checkpoint cleanup root indexing")?;
            let relative = path.strip_prefix(root)?;
            let mut cursor = &mut index;
            for component in relative.components() {
                ensure_deadline(deadline, "checkpoint cleanup root indexing")?;
                cursor = cursor
                    .children
                    .entry(component.as_os_str().to_os_string())
                    .or_default();
            }
            cursor.terminal = true;
        }
        Ok(index)
    }

    pub(in crate::producer::tla::checkpoint) fn matches(
        &self,
        path: &Path,
        deadline: Instant,
    ) -> Result<bool, Box<dyn Error>> {
        let mut cursor = self;
        if cursor.terminal {
            return Ok(true);
        }
        for component in path.components() {
            ensure_deadline(deadline, "checkpoint cleanup selection")?;
            let Some(next) = cursor.children.get(component.as_os_str()) else {
                return Ok(false);
            };
            cursor = next;
            if cursor.terminal {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
