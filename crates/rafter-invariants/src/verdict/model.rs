//! Final aggregate verdict and issue vocabulary.

use serde::{Deserialize, Serialize};

use crate::evidence::{ArtifactRef, EvidenceStatus, FailureClassification};

/// Current version of the final aggregate verdict report contract.
pub const VERDICT_SCHEMA_VERSION: u32 = 2;

/// Aggregate report containing exactly one verdict per reviewed invariant.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VerdictReport {
    pub schema_version: u32,
    pub profile: String,
    pub source_ref: String,
    pub summary: VerdictSummary,
    pub artifacts: Vec<ArtifactRef>,
    pub invariants: Vec<InvariantVerdict>,
}

/// Aggregate green/red counts.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VerdictSummary {
    pub total: usize,
    pub green: usize,
    pub red: usize,
}

/// Final verdict and supporting issues for one invariant ID.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InvariantVerdict {
    pub invariant_id: String,
    pub status: VerdictStatus,
    pub required_clauses: usize,
    pub passed_clauses: usize,
    pub required_evidence: usize,
    pub passed_evidence: usize,
    pub clauses: Vec<ClauseVerdict>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub issues: Vec<VerdictIssue>,
}

/// Final verdict for one stable normative clause within a parent invariant.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClauseVerdict {
    pub clause_id: String,
    pub statement: String,
    pub status: VerdictStatus,
    pub required_evidence: usize,
    pub passed_evidence: usize,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub issues: Vec<VerdictIssue>,
}

/// Exhaustive final verdict states.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VerdictStatus {
    Green,
    Red,
}

/// One missing, failed, incomplete, stale, or malformed evidence issue.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VerdictIssue {
    pub evidence_id: String,
    pub status: EvidenceStatus,
    pub classification: FailureClassification,
    pub message: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<ArtifactRef>,
}
