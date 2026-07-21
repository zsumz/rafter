//! Bounded checkpoint traversal facade.

mod model;
mod operations;

pub(super) use model::{
    CheckpointNodeKind, CheckpointTree, TraversalBudget, TraversalLimits, TRAVERSAL_LIMITS,
};
pub(super) use operations::{
    directory_has_entries, entry_kind, path_entry_exists, remove_scanned_subtrees,
    sanitize_cache_root, scan_checkpoint_tree,
};
#[cfg(test)]
pub(super) use operations::{read_sorted_entries, sanitize_cache_root_with_limits};
