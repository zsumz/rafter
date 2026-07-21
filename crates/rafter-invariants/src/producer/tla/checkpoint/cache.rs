//! Checkpoint cache initialization and complete-run retention policy.

use std::{
    collections::BTreeMap,
    error::Error,
    path::{Path, PathBuf},
    time::Instant,
};

use crate::execution::filesystem::HeldDirectory;

use super::{
    finalization::ensure_deadline,
    model::CACHE_VALID_FILE,
    traversal::{
        entry_kind, path_entry_exists, remove_scanned_subtrees, scan_checkpoint_tree,
        CheckpointNodeKind, CheckpointTree, TraversalBudget, TRAVERSAL_LIMITS,
    },
};

pub(super) fn initialize_cache_root(
    root: &Path,
    deadline: Instant,
) -> Result<bool, Box<dyn Error>> {
    let root_is_symlink = entry_kind(root)? == Some(CheckpointNodeKind::Symlink);
    if !root_is_symlink {
        let root_handle = HeldDirectory::create_all(root)?;
        root_handle.remove_file_if_exists(Path::new(CACHE_VALID_FILE))?;
    }
    ensure_deadline(deadline, "checkpoint cache initialization")?;
    Ok(root_is_symlink)
}

pub(super) fn write_cache_valid_marker(
    root: &Path,
    state: &str,
    contract_sha256: &str,
) -> Result<(), Box<dyn Error>> {
    HeldDirectory::open(root)?.write_atomic(
        Path::new(CACHE_VALID_FILE),
        format!("schema_version=1\nstate={state}\ncontract_sha256={contract_sha256}\n").as_bytes(),
    )
}

pub(super) fn prune_to_latest(state_dir: &Path, deadline: Instant) -> Result<(), Box<dyn Error>> {
    ensure_deadline(deadline, "checkpoint pruning")?;
    if !path_entry_exists(state_dir)? {
        return Ok(());
    }
    let mut budget = TraversalBudget::new(TRAVERSAL_LIMITS);
    let tree = scan_checkpoint_tree(
        state_dir,
        deadline,
        "checkpoint pruning",
        &mut budget,
        false,
    )?;
    let runs = checkpoint_runs(state_dir, &tree)?;
    let mut complete = runs
        .iter()
        .filter_map(|(path, markers)| markers.complete().then_some(path.clone()))
        .collect::<Vec<_>>();
    let mut remove = runs
        .iter()
        .filter_map(|(path, markers)| (!markers.complete()).then_some(path.clone()))
        .collect::<Vec<_>>();
    complete.sort();
    if let Some(latest) = complete.pop() {
        for directory in complete {
            if directory != latest {
                remove.push(directory);
            }
        }
    }
    remove.sort();
    remove.dedup();
    if !remove.is_empty() {
        remove_scanned_subtrees(&tree, &remove, deadline)?;
    }
    Ok(())
}

#[derive(Default)]
pub(super) struct CheckpointMarkers {
    committed: bool,
    temporary: bool,
}

impl CheckpointMarkers {
    pub(super) fn complete(&self) -> bool {
        self.committed && !self.temporary
    }
}

pub(super) fn checkpoint_runs(
    state_dir: &Path,
    tree: &CheckpointTree,
) -> Result<BTreeMap<PathBuf, CheckpointMarkers>, Box<dyn Error>> {
    let mut runs = BTreeMap::<PathBuf, CheckpointMarkers>::new();
    for node in &tree.nodes {
        if node.kind != CheckpointNodeKind::File {
            continue;
        }
        let relative = node.path.strip_prefix(state_dir)?;
        let mut components = relative.components();
        let Some(run_name) = components.next() else {
            return Err("checkpoint state file has no run directory".into());
        };
        if components.next().is_none() {
            return Err("checkpoint state file is not inside a TLC run directory".into());
        }
        let markers = runs
            .entry(state_dir.join(run_name.as_os_str()))
            .or_default();
        let name = node
            .path
            .file_name()
            .ok_or("checkpoint state file has no file name")?
            .to_string_lossy();
        markers.temporary |= has_tlc_extension(&name, "tmp");
        markers.committed |= has_tlc_extension(&name, "chkpt");
    }
    Ok(runs)
}

fn has_tlc_extension(name: &str, expected: &str) -> bool {
    let path = Path::new(name);
    path.extension()
        .is_some_and(|extension| extension.eq_ignore_ascii_case(expected))
        || path
            .file_stem()
            .and_then(|stem| Path::new(stem).extension())
            .is_some_and(|extension| extension.eq_ignore_ascii_case(expected))
}
