//! Immutable serialized evidence crossing trust boundaries.

mod artifact;
mod bundle;
pub(crate) mod detector_proof;
mod execution;
pub(crate) mod format;
pub(crate) mod limits;
mod liveness;
mod receipt;
mod result;
mod schema;

pub use artifact::ArtifactRef;
pub(crate) use bundle::ResultBundle;
pub use bundle::RESULT_SCHEMA_VERSION;
pub use execution::{
    CheckCompletion, CheckReceipt, ExecutionPlanReceipt, ExecutionReceipt, InvocationReceipt,
    LauncherReceipt, PlanInput, ProducerBindingReceipt, PLAN_SCHEMA_VERSION,
};
pub(crate) use liveness::{
    bind_liveness_claims, execution_contract_digest, liveness_contract_digest,
    liveness_reports_digest, LivenessBindingClaim, LivenessReportClaim,
};
pub(crate) use receipt::PRIMARY_COMPLETION_KEY;
pub use receipt::{
    ContinuationOutcome, ExecutableReceipt, PrimaryCompletionPolicy, SimulatorLivenessBinding,
    SimulatorLivenessReportBinding, SourceMaterializationReceipt, SourceReceipt,
    TlaContinuationBinding, ToolReceipt,
};
pub use result::{EvidenceResult, EvidenceStatus, FailureClassification};
pub(crate) use schema::{validate_result_bundle, validate_result_value};

#[cfg(test)]
mod tests;
