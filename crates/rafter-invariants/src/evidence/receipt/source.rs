//! Source checkout, materialization, and Rust toolchain receipts.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::ToolReceipt;

/// Immutable source and toolchain identity used to produce a bundle.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceReceipt {
    pub commit: String,
    pub tree: String,
    pub materialization: SourceMaterializationReceipt,
    pub cargo_lock_sha256: String,
    pub cargo: String,
    pub cargo_sha256: String,
    pub cargo_config_sha256: String,
    pub rustc: String,
    pub rustc_sha256: String,
    pub target: String,
    pub build_profile: String,
    pub features: Vec<String>,
    pub tools: BTreeMap<String, ToolReceipt>,
    pub environment_sha256: String,
    pub clean: bool,
}

/// Exact raw worktree materialization proven against the recorded Git tree.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SourceMaterializationReceipt {
    pub contract: String,
    pub sha256: String,
    pub tracked_entries: u64,
    pub submodules: u64,
}
