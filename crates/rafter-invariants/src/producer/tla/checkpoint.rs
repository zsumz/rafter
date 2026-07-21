//! TLA+ checkpoint compatibility, recovery, and retained-cache facade.

mod cache;
mod finalization;
mod inventory;
mod model;
mod prepare;
mod traversal;

pub(crate) use crate::evidence::format::tla::checkpoint::{
    CheckpointContract, CheckpointFile, CheckpointInventory, RecoveryReport, RecoveryStatus,
    CONTRACT_KIND, INVENTORY_KIND, RECOVERED_CONTRACT_KIND, RECOVERED_INVENTORY_KIND,
    RECOVERY_REPORT_KIND,
};

pub(in crate::producer::tla) use model::enabled;
pub(in crate::producer) use model::Preparation;
pub(in crate::producer) use prepare::prepare;

#[cfg(test)]
use cache::prune_to_latest;
#[cfg(test)]
use inventory::{
    hash_reader, inventory, inventory_with_limits, read_candidate_json, read_file_with_deadline,
    validate_candidate,
};
#[cfg(test)]
use model::{
    expected_contract, CACHE_VALID_FILE, HASH_BUFFER_BYTES, INPUT_KINDS,
    MAX_CHECKPOINT_METADATA_BYTES,
};
#[cfg(test)]
use traversal::{
    read_sorted_entries, sanitize_cache_root_with_limits, TraversalLimits, TRAVERSAL_LIMITS,
};

#[cfg(test)]
#[path = "../tla_checkpoint_tests.rs"]
mod tests;
