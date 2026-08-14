//! Shared fixtures and vocabulary for invariant CI contract scenarios.

mod aggregate_report;
mod assertions;
mod contracts;
mod evidence_transport;
mod workflow;

pub(crate) use aggregate_report::AggregateReportFixture;
pub(crate) use assertions::{
    assert_pr_launcher_inventories, assert_separate_aggregate_uploads,
    assert_transport_stage_contract, assert_unique_paths,
};
pub(crate) use contracts::{
    display_layer, scheduled_diagnostics_step, scheduled_upload_step, AggregateWorkflowContract,
    CANONICAL_INVARIANT_IDS, PR_EVIDENCE_PRODUCERS, PR_LAYERS, SCHEDULED_LAYERS,
};
pub(crate) use evidence_transport::EvidenceTransportFixture;
pub(crate) use workflow::{
    assert_failure, assert_success, job_block, read, run_workflow_script, workflow_step,
    workflow_step_paths, workflow_steps, workspace_root,
};
