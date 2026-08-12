//! Execution, invocation, plan, and check receipts for one bundle.

use std::collections::BTreeMap;

use serde::{Deserialize, Deserializer, Serialize};

use super::{
    ArtifactRef, ExecutableReceipt, SimulatorLivenessBinding, SourceReceipt, TlaContinuationBinding,
};
use crate::contract::profile::ProfileContract;

/// Current version of the hashed execution-plan contract.
pub const PLAN_SCHEMA_VERSION: u32 = 3;

/// Deterministic execution provenance shared by every result in a bundle.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
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

/// Exact registry, manifest, and selected profile contract used by a producer.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionPlanReceipt {
    pub schema_version: u32,
    pub profile: String,
    pub registry: PlanInput,
    pub manifest: PlanInput,
    pub result_schema: PlanInput,
    pub verdict_schema: PlanInput,
    pub contract: ProfileContract,
}

/// One repository-relative input whose exact bytes are bound into a plan.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PlanInput {
    pub path: String,
    pub sha256: String,
    pub size_bytes: u64,
}

/// Actual invariant CLI process invocation, separate from reproduction hints.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InvocationReceipt {
    pub program: String,
    pub program_sha256: String,
    pub arguments: Vec<String>,
    pub current_dir: String,
    pub environment: BTreeMap<String, String>,
    pub environment_sha256: String,
    pub launchers: Vec<LauncherReceipt>,
}

/// Ordered executable role used to launch or observe one subprocess.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LauncherReceipt {
    pub role: String,
    pub runtime: String,
    #[serde(flatten)]
    pub executable: ExecutableReceipt,
}

/// Immutable executable artifact bound to the producer invocation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProducerBindingReceipt {
    pub binding: String,
    pub executable: ArtifactRef,
}

/// One actually invoked deterministic check and its observed completion state.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CheckReceipt {
    pub execution_id: String,
    pub check_id: String,
    pub evidence_ids: Vec<String>,
    pub completion: CheckCompletion,
    pub observations: BTreeMap<String, u64>,
    #[serde(deserialize_with = "deserialize_present_option")]
    pub simulator_liveness: Option<SimulatorLivenessBinding>,
    /// Primary-continuation policy and outcome, present on TLA+ receipts only.
    ///
    /// Unlike `simulator_liveness` this is additive rather than
    /// present-but-nullable: adding a required key would rewrite every layer's
    /// serialized receipts for a field only one layer uses. The TLA+ receipt
    /// validator requires it, so the layer that needs it still fails closed
    /// when it is absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tla_continuation: Option<TlaContinuationBinding>,
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

/// Exhaustive reasons why a deterministic check stopped.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckCompletion {
    Completed,
    FrontierExhausted,
    Counterexample,
    CoverageNotReached,
    BudgetExhausted,
    Timeout,
    HarnessError,
}
