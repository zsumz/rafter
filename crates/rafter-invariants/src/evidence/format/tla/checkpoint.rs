//! Serialized TLA+ checkpoint metadata shared across the trust boundary.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub(crate) const CONTRACT_KIND: &str = "tla-checkpoint-contract";
pub(crate) const INVENTORY_KIND: &str = "tla-checkpoint-inventory";
pub(crate) const RECOVERED_CONTRACT_KIND: &str = "tla-checkpoint-recovered-contract";
pub(crate) const RECOVERED_INVENTORY_KIND: &str = "tla-checkpoint-recovered-inventory";
pub(crate) const RECOVERY_REPORT_KIND: &str = "tla-checkpoint-recovery-report";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CheckpointContract {
    pub schema_version: u32,
    pub profile: String,
    pub config: String,
    pub runner_contract_sha256: String,
    pub input_sha256: BTreeMap<String, String>,
}

impl CheckpointContract {
    pub(crate) fn sha256(&self) -> Result<String, serde_json::Error> {
        Ok(format!("{:x}", Sha256::digest(serde_json::to_vec(self)?)))
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CheckpointInventory {
    pub schema_version: u32,
    pub contract_sha256: String,
    pub latest_checkpoint: Option<String>,
    pub files: Vec<CheckpointFile>,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CheckpointFile {
    pub path: String,
    pub sha256: String,
    pub size_bytes: u64,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum RecoveryStatus {
    Fresh,
    Compatible,
    Incompatible,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RecoveryReport {
    pub schema_version: u32,
    pub status: RecoveryStatus,
    pub contract_sha256: String,
    pub candidate_present: bool,
    pub recovery_attempted: bool,
    pub recovered_checkpoint: Option<String>,
    pub error: Option<String>,
}

#[cfg(test)]
#[path = "checkpoint_tests.rs"]
mod tests;
