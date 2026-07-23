//! Gate-owned adapters from binary command inputs to trusted domain operations.

mod check;
mod document;
mod execution;
mod model;
mod output;
mod publication;
mod report_set;

pub use model::{
    CheckOptions, CommandOutput, PlanSelection, ProduceOptions, RenderDocumentOptions,
    RunAllOptions, SealVerifierArtifactsOptions, VerifyLayerOptions, VerifyReportSetOptions,
    VerifyVerifierArchiveOptions,
};

pub use check::execute as check;
pub use document::execute as render_document;
pub use execution::{produce, producer_probe, run_all, verify_layer};
pub use publication::{seal as seal_verifier_artifacts, verify as verify_verifier_archive};
pub use report_set::execute as verify_report_set;
