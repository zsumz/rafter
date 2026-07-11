use serde::{Deserialize, Serialize};

/// Current version of the machine-readable receipt and report contract.
pub const RESULT_SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Source-bound evidence receipts emitted by one deterministic runner.
pub struct ResultBundle {
    pub schema_version: u32,
    pub runner: String,
    pub profile: String,
    pub source_ref: String,
    pub results: Vec<EvidenceResult>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// One runner result for one registry evidence declaration.
pub struct EvidenceResult {
    pub invariant_id: String,
    pub evidence_id: String,
    pub status: EvidenceStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub classification: Option<FailureClassification>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<ArtifactRef>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Exhaustive evidence execution outcomes.
pub enum EvidenceStatus {
    Pass,
    Fail,
    Incomplete,
    Error,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Exhaustive red-result classifications used by every runner.
pub enum FailureClassification {
    InvariantViolation,
    CoverageNotReached,
    HarnessError,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Replayable log, trace, counterexample, or related evidence artifact.
pub struct ArtifactRef {
    pub kind: String,
    pub path: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
/// Aggregate report containing exactly one verdict per reviewed invariant.
pub struct VerdictReport {
    pub schema_version: u32,
    pub profile: String,
    pub source_ref: String,
    pub summary: VerdictSummary,
    pub invariants: Vec<InvariantVerdict>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
/// Aggregate green/red counts.
pub struct VerdictSummary {
    pub total: usize,
    pub green: usize,
    pub red: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
/// Final verdict and supporting issues for one invariant ID.
pub struct InvariantVerdict {
    pub invariant_id: String,
    pub status: VerdictStatus,
    pub required_evidence: usize,
    pub passed_evidence: usize,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub issues: Vec<VerdictIssue>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Exhaustive final verdict states.
pub enum VerdictStatus {
    Green,
    Red,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
/// One missing, failed, incomplete, stale, or malformed evidence issue.
pub struct VerdictIssue {
    pub evidence_id: String,
    pub status: EvidenceStatus,
    pub classification: FailureClassification,
    pub message: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<ArtifactRef>,
}
