//! One runner's complete source-bound evidence bundle.

use serde::{Deserialize, Serialize};

use super::{EvidenceResult, ExecutionReceipt};

/// Current version of the machine-readable receipt and report contract.
pub const RESULT_SCHEMA_VERSION: u32 = 14;

/// Source-bound evidence receipts emitted by one deterministic runner.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ResultBundle {
    pub schema_version: u32,
    pub runner: String,
    pub profile: String,
    pub source_ref: String,
    pub execution: ExecutionReceipt,
    pub results: Vec<EvidenceResult>,
}
