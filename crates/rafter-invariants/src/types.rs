use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize};

/// Current version of the machine-readable receipt and report contract.
pub const RESULT_SCHEMA_VERSION: u32 = 13;

/// Current version of the final aggregate verdict report contract.
pub const VERDICT_SCHEMA_VERSION: u32 = 2;

/// Current version of the hashed execution-plan contract.
pub const PLAN_SCHEMA_VERSION: u32 = 3;

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
    pub plan: ExecutionPlanReceipt,
    pub invocation: InvocationReceipt,
    pub producer: ProducerBindingReceipt,
    pub source: SourceReceipt,
    pub checks: Vec<CheckReceipt>,
    pub duration_ms: u64,
    pub peak_rss_kib: u64,
    pub artifacts: Vec<ArtifactRef>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Exact registry, manifest, and selected profile contract used by a producer.
pub struct ExecutionPlanReceipt {
    pub schema_version: u32,
    pub profile: String,
    pub registry: PlanInput,
    pub manifest: PlanInput,
    pub result_schema: PlanInput,
    pub verdict_schema: PlanInput,
    pub contract: crate::ProfileContract,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// One repository-relative input whose exact bytes are bound into a plan.
pub struct PlanInput {
    pub path: String,
    pub sha256: String,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Actual invariant CLI process invocation, separate from reproduction hints.
pub struct InvocationReceipt {
    pub program: String,
    pub program_sha256: String,
    pub arguments: Vec<String>,
    pub current_dir: String,
    pub environment: BTreeMap<String, String>,
    pub environment_sha256: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Immutable executable artifact bound to the producer invocation.
pub struct ProducerBindingReceipt {
    pub binding: String,
    pub executable: ArtifactRef,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Immutable source and toolchain identity used to produce a bundle.
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Exact raw worktree materialization proven against the recorded Git tree.
pub struct SourceMaterializationReceipt {
    pub contract: String,
    pub sha256: String,
    pub tracked_entries: u64,
    pub submodules: u64,
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
    #[serde(deserialize_with = "deserialize_present_option")]
    pub simulator_liveness: Option<SimulatorLivenessBinding>,
    pub duration_ms: u64,
    pub peak_rss_kib: u64,
    pub artifacts: Vec<ArtifactRef>,
}

fn deserialize_present_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
/// Registry-owned semantics for one structured bounded-liveness report family.
pub struct SimulatorLivenessContract {
    pub invariant_id: String,
    pub clause_ids: Vec<String>,
    pub feature_id: String,
    pub scenario_id: String,
    pub observation_id: String,
    pub fault_requirement: String,
    pub stable_leader_retained: Option<bool>,
    pub stable_leader_rounds_minimum: Option<u64>,
    pub stable_leader_rounds_exact: Option<u64>,
    pub stable_leader_rounds_relation: String,
    pub proposal_outcome: String,
    pub authority_loss_required: bool,
    pub fault_cycle_required: bool,
    pub fairness_policy_id: String,
    pub fairness_tick_bound_rounds: u64,
    pub fairness_delivery_bound_rounds: u64,
    pub fairness_max_delivery_waves_per_tick: u64,
    pub round_budget_provenance: String,
    pub minimum_rounds: u64,
    pub rounds_per_node: u64,
    pub rounds_per_queued_message: u64,
    pub rounds_per_proposal: u64,
    pub rounds_per_membership_change: u64,
    pub rounds_per_partition: u64,
    pub snapshot_catchup_rounds: u64,
    pub phase_count: u64,
    pub fixed_rounds: u64,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
/// Exact profile and check configuration independently expected for a soak run.
pub struct SimulatorExecutionContract {
    pub contract_id: String,
    pub profile_id: String,
    pub check_id: String,
    pub check_kind: String,
    pub node_config_id: String,
    pub node_count: u64,
    pub steps: u64,
    pub max_proposals: u64,
    pub max_restarts: u64,
    pub max_read_indexes: u64,
    pub max_membership_changes: u64,
    pub max_transfers: u64,
    pub max_partitions: u64,
    pub max_lossy_restarts: u64,
    pub snapshot_catchup_probe: bool,
    pub tick_skew_node_id: Option<u64>,
    pub tick_skew_weight: Option<u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Exact typed contract and canonical report digests for one liveness check.
pub struct SimulatorLivenessBinding {
    pub schema_version: u32,
    pub contract: SimulatorLivenessContract,
    pub contract_sha256: String,
    pub reports_sha256: String,
    pub reports: Vec<SimulatorLivenessReportBinding>,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
/// One validated simulator run bound to its complete structured report bytes.
pub struct SimulatorLivenessReportBinding {
    pub check_id: String,
    pub seed: u64,
    pub execution_contract: SimulatorExecutionContract,
    pub execution_contract_sha256: String,
    pub report_sha256: String,
    pub round_limit: u64,
    pub rounds_used: u64,
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Aggregate report containing exactly one verdict per reviewed invariant.
pub struct VerdictReport {
    pub schema_version: u32,
    pub profile: String,
    pub source_ref: String,
    pub summary: VerdictSummary,
    pub artifacts: Vec<ArtifactRef>,
    pub invariants: Vec<InvariantVerdict>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// Aggregate green/red counts.
pub struct VerdictSummary {
    pub total: usize,
    pub green: usize,
    pub red: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
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

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
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

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
/// Exhaustive final verdict states.
pub enum VerdictStatus {
    Green,
    Red,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
/// One missing, failed, incomplete, stale, or malformed evidence issue.
pub struct VerdictIssue {
    pub evidence_id: String,
    pub status: EvidenceStatus,
    pub classification: FailureClassification,
    pub message: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub artifacts: Vec<ArtifactRef>,
}
