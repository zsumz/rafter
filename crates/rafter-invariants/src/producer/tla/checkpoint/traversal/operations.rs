//! Descriptor-relative checkpoint tree scanning and selected subtree removal.

use std::{
    cmp::Reverse,
    collections::BTreeMap,
    error::Error,
    path::{Path, PathBuf},
    time::Instant,
};

use crate::execution::filesystem::{EntryKind, HeldDirectory, OperationDeadline};

use super::{
    super::finalization::ensure_deadline,
    model::{
        CheckpointNode, CheckpointNodeKind, CheckpointTree, RootIndex, TraversalBudget,
        TraversalLimits, TRAVERSAL_LIMITS,
    },
};

pub(in crate::producer::tla::checkpoint) fn sanitize_cache_root(
    root: &Path,
    deadline: Instant,
) -> Result<(), Box<dyn Error>> {
    sanitize_cache_root_with_limits(root, deadline, TRAVERSAL_LIMITS)
}

pub(in crate::producer::tla::checkpoint) fn sanitize_cache_root_with_limits(
    root: &Path,
    deadline: Instant,
    limits: TraversalLimits,
) -> Result<(), Box<dyn Error>> {
    ensure_deadline(deadline, "checkpoint cache sanitization")?;
    let workspace = HeldDirectory::workspace()?;
    match entry_kind(root)? {
        None => {}
        Some(CheckpointNodeKind::Symlink) => workspace.remove_symlink(root)?,
        Some(CheckpointNodeKind::Directory) => {
            let existing = HeldDirectory::open(root)?;
            existing.remove_contents(
                limits,
                OperationDeadline::at(deadline, "checkpoint cache sanitization"),
            )?;
            existing.remove_self()?;
        }
        Some(CheckpointNodeKind::File) => {
            return Err(format!("checkpoint cleanup rejects {}", root.display()).into())
        }
    }
    HeldDirectory::create_all(root)?;
    ensure_deadline(deadline, "checkpoint cache replacement")?;
    Ok(())
}

#[cfg(test)]
pub(in crate::producer::tla::checkpoint) fn read_sorted_entries(
    directory: &Path,
    deadline: Instant,
    operation: &str,
) -> Result<Vec<CheckpointNode>, Box<dyn Error>> {
    let root = HeldDirectory::open(directory)?;
    let mut budget = TraversalBudget::new(TRAVERSAL_LIMITS);
    read_sorted_entries_with_budget(
        directory,
        &root,
        Path::new(""),
        0,
        deadline,
        operation,
        &mut budget,
        false,
    )
}

pub(in crate::producer::tla::checkpoint) fn scan_checkpoint_tree(
    root_path: &Path,
    deadline: Instant,
    operation: &str,
    budget: &mut TraversalBudget,
    allow_symlinks: bool,
) -> Result<CheckpointTree, Box<dyn Error>> {
    let root = HeldDirectory::open(root_path)?;
    let mut nodes = Vec::new();
    let mut pending = vec![(PathBuf::new(), 0_usize)];
    while let Some((relative, depth)) = pending.pop() {
        let entries = read_sorted_entries_with_budget(
            root_path,
            &root,
            &relative,
            depth,
            deadline,
            operation,
            budget,
            allow_symlinks,
        )?;
        for entry in entries.iter().rev() {
            if entry.kind == CheckpointNodeKind::Directory {
                pending.push((
                    entry.path.strip_prefix(root_path)?.to_path_buf(),
                    entry.depth,
                ));
            }
        }
        nodes.extend(entries);
    }
    Ok(CheckpointTree {
        nodes,
        root,
        root_path: root_path.to_path_buf(),
    })
}

#[allow(clippy::too_many_arguments)]
fn read_sorted_entries_with_budget(
    root_path: &Path,
    root: &HeldDirectory,
    relative: &Path,
    depth: usize,
    deadline: Instant,
    operation: &str,
    budget: &mut TraversalBudget,
    allow_symlinks: bool,
) -> Result<Vec<CheckpointNode>, Box<dyn Error>> {
    let directory = root.open_dir(relative)?;
    let display_path = root_path.join(relative);
    budget.enter_directory(&display_path, depth)?;
    let deadline_guard = OperationDeadline::at(deadline, "checkpoint directory traversal");
    let raw_entries = directory.entries(deadline_guard)?;
    if raw_entries.len() > budget.limits.directory_entries() {
        return Err(format!(
            "checkpoint directory {} exceeds the entry limit of {}",
            display_path.display(),
            budget.limits.directory_entries()
        )
        .into());
    }
    let mut entries = Vec::with_capacity(raw_entries.len());
    for (name, entry_kind) in raw_entries {
        ensure_deadline(deadline, operation)?;
        let node_relative = relative.join(&name);
        let path = root_path.join(&node_relative);
        let kind = checkpoint_kind(entry_kind);
        if kind == CheckpointNodeKind::Symlink && !allow_symlinks {
            return Err(format!("checkpoint traversal rejects symlink {}", path.display()).into());
        }
        let node_depth = depth.checked_add(1).ok_or("checkpoint depth overflow")?;
        budget.visit_node(&path, node_depth, kind)?;
        let identity = match kind {
            CheckpointNodeKind::Directory => Some(root.directory_identity(&node_relative)?),
            CheckpointNodeKind::File => Some(root.file_identity(&node_relative)?),
            CheckpointNodeKind::Symlink => None,
        };
        entries.push(CheckpointNode {
            path,
            kind,
            identity,
            depth: node_depth,
        });
    }
    ensure_deadline(deadline, operation)?;
    Ok(entries)
}

pub(in crate::producer::tla::checkpoint) fn remove_scanned_subtrees(
    tree: &CheckpointTree,
    roots: &[PathBuf],
    deadline: Instant,
) -> Result<(), Box<dyn Error>> {
    let index = RootIndex::new(&tree.root_path, roots, deadline)?;
    let mut ordered = BTreeMap::<Reverse<usize>, BTreeMap<Reverse<PathBuf>, usize>>::new();
    for (node_index, node) in tree.nodes.iter().enumerate() {
        ensure_deadline(deadline, "checkpoint cleanup selection")?;
        let relative = node.path.strip_prefix(&tree.root_path)?;
        if index.matches(relative, deadline)? {
            ordered
                .entry(Reverse(node.depth))
                .or_default()
                .insert(Reverse(node.path.clone()), node_index);
        }
    }
    for by_path in ordered.into_values() {
        ensure_deadline(deadline, "checkpoint cleanup ordering")?;
        for node_index in by_path.into_values() {
            ensure_deadline(deadline, "checkpoint cleanup ordering")?;
            remove_scanned_node(tree, &tree.nodes[node_index], deadline)?;
        }
    }
    Ok(())
}

fn remove_scanned_node(
    tree: &CheckpointTree,
    node: &CheckpointNode,
    deadline: Instant,
) -> Result<(), Box<dyn Error>> {
    ensure_deadline(deadline, "checkpoint cleanup")?;
    let relative = node.path.strip_prefix(&tree.root_path)?;
    match node.kind {
        CheckpointNodeKind::Directory => tree.root.remove_dir_if_identity(
            relative,
            node.identity
                .as_ref()
                .ok_or("checkpoint directory omitted identity")?,
        )?,
        CheckpointNodeKind::File => tree.root.remove_file_if_identity(
            relative,
            node.identity
                .as_ref()
                .ok_or("checkpoint file omitted identity")?,
        )?,
        CheckpointNodeKind::Symlink => tree.root.remove_symlink(relative)?,
    }
    ensure_deadline(deadline, "checkpoint cleanup")?;
    Ok(())
}

pub(in crate::producer::tla::checkpoint) fn directory_has_entries(
    path: &Path,
    deadline: Instant,
) -> Result<bool, Box<dyn Error>> {
    ensure_deadline(deadline, "checkpoint candidate discovery")?;
    match entry_kind(path)? {
        None => Ok(false),
        Some(CheckpointNodeKind::Directory) => Ok(!HeldDirectory::open(path)?
            .entries(OperationDeadline::at(
                deadline,
                "checkpoint candidate discovery",
            ))?
            .is_empty()),
        Some(_) => Err(std::io::Error::new(
            std::io::ErrorKind::NotADirectory,
            format!(
                "checkpoint directory path is not a directory: {}",
                path.display()
            ),
        )
        .into()),
    }
}

pub(in crate::producer::tla::checkpoint) fn path_entry_exists(
    path: &Path,
) -> Result<bool, Box<dyn Error>> {
    Ok(entry_kind(path)?.is_some())
}

pub(in crate::producer::tla::checkpoint) fn entry_kind(
    path: &Path,
) -> Result<Option<CheckpointNodeKind>, Box<dyn Error>> {
    let workspace = HeldDirectory::workspace()?;
    let parent = path.parent().unwrap_or_else(|| Path::new(""));
    let Some(name) = path.file_name() else {
        return Ok(Some(CheckpointNodeKind::Directory));
    };
    let parent = match workspace.open_dir(parent) {
        Ok(parent) => parent,
        Err(error)
            if error
                .downcast_ref::<std::io::Error>()
                .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound) =>
        {
            return Ok(None)
        }
        Err(error) => return Err(error),
    };
    Ok(parent.entry_kind(name)?.map(checkpoint_kind))
}

fn checkpoint_kind(kind: EntryKind) -> CheckpointNodeKind {
    match kind {
        EntryKind::Directory => CheckpointNodeKind::Directory,
        EntryKind::File => CheckpointNodeKind::File,
        EntryKind::Symlink => CheckpointNodeKind::Symlink,
    }
}
