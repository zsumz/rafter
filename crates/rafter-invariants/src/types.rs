use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Current version of the machine-readable receipt and report contract.
pub const RESULT_SCHEMA_VERSION: u32 = 6;

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Source-bound evidence receipts emitted by one deterministic runner.
pub struct ResultBundle {
    pub schema_version: u32,
    pub runner: String,
    pub profile: String,
    pub source_ref: String,
    pub execution: ExecutionReceipt,
    pub results: Vec<EvidenceResult>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Deterministic execution provenance shared by every result in a bundle.
pub struct ExecutionReceipt {
    pub producer: String,
    pub command: Vec<String>,
    pub configuration: BTreeMap<String, String>,
    pub source: SourceReceipt,
    pub checks: Vec<CheckReceipt>,
    pub duration_ms: u64,
    pub peak_rss_kib: u64,
    pub artifacts: Vec<ArtifactRef>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Immutable source and toolchain identity used to produce a bundle.
pub struct SourceReceipt {
    pub commit: String,
    pub tree: String,
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Version and executable digest for a non-Rust evidence tool.
pub struct ToolReceipt {
    pub version: String,
    pub sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// One actually invoked deterministic check and its observed completion state.
pub struct CheckReceipt {
    pub execution_id: String,
    pub check_id: String,
    pub evidence_ids: Vec<String>,
    pub completion: CheckCompletion,
    pub observations: BTreeMap<String, u64>,
    pub duration_ms: u64,
    pub peak_rss_kib: u64,
    pub artifacts: Vec<ArtifactRef>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
/// Exhaustive reasons why a deterministic check stopped.
pub enum CheckCompletion {
    Completed,
    FrontierExhausted,
    Counterexample,
    CoverageNotReached,
    BudgetExhausted,
    Timeout,
    HarnessError,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// One runner result for one registry evidence declaration.
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
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

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
/// Replayable log, trace, counterexample, or related evidence artifact.
pub struct ArtifactRef {
    pub kind: String,
    pub path: String,
    pub sha256: String,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
/// Aggregate report containing exactly one verdict per reviewed invariant.
pub struct VerdictReport {
    pub schema_version: u32,
    pub profile: String,
    pub source_ref: String,
    pub summary: VerdictSummary,
    pub artifacts: Vec<ArtifactRef>,
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
    pub required_clauses: usize,
    pub passed_clauses: usize,
    pub required_evidence: usize,
    pub passed_evidence: usize,
    pub clauses: Vec<ClauseVerdict>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub issues: Vec<VerdictIssue>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
/// Final verdict for one stable normative clause within a parent invariant.
pub struct ClauseVerdict {
    pub clause_id: String,
    pub statement: String,
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
