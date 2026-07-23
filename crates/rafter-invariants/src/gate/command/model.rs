//! Gate-owned command inputs and terminal-output projection.

use std::path::PathBuf;

#[derive(Debug)]
/// Source-controlled contract selection shared by command adapters.
pub struct PlanSelection {
    pub profile: String,
    pub registry: PathBuf,
    pub manifest: PathBuf,
}

impl PlanSelection {
    pub(super) fn plan_options(&self) -> crate::plan::PlanOptions {
        crate::plan::PlanOptions {
            profile: self.profile.clone(),
            registry: self.registry.clone(),
            manifest: self.manifest.clone(),
        }
    }
}

#[derive(Debug)]
/// Inputs for aggregation from existing producer receipts.
pub struct CheckOptions {
    pub plan: PlanSelection,
    pub results: Vec<PathBuf>,
    pub results_dir: PathBuf,
    pub output_dir: PathBuf,
    pub source_ref: Option<String>,
}

#[derive(Debug)]
/// Inputs for one source-bound evidence layer.
pub struct ProduceOptions {
    pub plan: PlanSelection,
    pub layer: String,
    pub output_dir: PathBuf,
}

#[derive(Debug)]
/// Inputs for complete-profile production and aggregation.
pub struct RunAllOptions {
    pub plan: PlanSelection,
    pub results_dir: PathBuf,
    pub output_dir: PathBuf,
}

#[derive(Debug)]
/// Inputs for independent verification of one evidence layer.
pub struct VerifyLayerOptions {
    pub plan: PlanSelection,
    pub layer: String,
    pub result: PathBuf,
}

#[derive(Debug)]
/// Inputs for canonical invariant-catalog rendering or readback.
pub struct RenderDocumentOptions {
    pub registry: PathBuf,
    pub output: PathBuf,
    pub check: bool,
}

#[derive(Debug)]
/// Inputs for verifier-evidence archive publication.
pub struct SealVerifierArtifactsOptions {
    pub profile: String,
    pub profile_manifest: PathBuf,
    pub root: PathBuf,
    pub manifest: PathBuf,
    pub manifest_sha256: String,
    pub archive: PathBuf,
}

#[derive(Debug)]
/// Inputs for verifier-evidence archive readback.
pub struct VerifyVerifierArchiveOptions {
    pub profile: String,
    pub profile_manifest: PathBuf,
    pub archive: PathBuf,
    pub archive_sha256: String,
    pub manifest_sha256: String,
}

#[derive(Debug)]
/// Inputs for semantic readback of one published report set.
pub struct VerifyReportSetOptions {
    pub plan: PlanSelection,
    pub report_dir: PathBuf,
}

#[derive(Debug, Eq, PartialEq)]
/// Exit classification and terminal lines returned to the binary adapter.
pub struct CommandOutput {
    pub success: bool,
    pub lines: Vec<String>,
    pub structural_error: Option<String>,
}

impl CommandOutput {
    pub(super) fn new(success: bool, lines: Vec<String>) -> Self {
        Self {
            success,
            lines,
            structural_error: None,
        }
    }

    pub(super) fn passed(line: String) -> Self {
        Self::new(true, vec![line])
    }

    pub(super) fn structurally_failed(lines: Vec<String>, error: String) -> Self {
        Self {
            success: false,
            lines,
            structural_error: Some(error),
        }
    }
}
