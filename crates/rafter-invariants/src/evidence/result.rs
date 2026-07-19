//! Evidence outcomes and exhaustive red-result classification.

use serde::{Deserialize, Serialize};

use super::ArtifactRef;

/// One runner result for one registry evidence declaration.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct EvidenceResult {
    pub invariant_id: String,
    pub evidence_id: String,
    pub execution_id: String,
    pub status: EvidenceStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classification: Option<FailureClassification>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<ArtifactRef>,
}

/// Exhaustive evidence execution outcomes.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceStatus {
    Pass,
    Fail,
    Incomplete,
    Error,
}

/// Exhaustive red-result classifications used by every runner.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FailureClassification {
    InvariantViolation,
    CoverageNotReached,
    HarnessError,
}
