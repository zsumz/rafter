//! Stable machine-readable verifier replay report vocabulary.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    contract::profile::DetectorReplayContract,
    evidence::ArtifactRef,
    verification::source::{AuthenticatedSourceReceipt, RegistryReceipt, ReplayToolchainReceipt},
};

use super::super::{
    model::{ReplayEvidence, ReplayFixture},
    result::FixtureReplayStatus,
    ReplayTarget,
};

pub(super) const REPORT_SCHEMA_VERSION: u32 = 4;
pub(super) const PROCESS_TERMINATION_GRACE_MS: u64 = 30_000;
pub(super) const SUCCESSFUL_PROCESS_LIFECYCLE_ALLOWANCE_MS: u64 = 10_000;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReplayReport {
    pub(super) schema_version: u32,
    pub(super) profile: String,
    pub(super) source_ref: String,
    pub(super) source: AuthenticatedSourceReceipt,
    pub(super) source_sha256: String,
    pub(super) toolchain: ReplayToolchainReceipt,
    pub(super) toolchain_sha256: String,
    pub(super) contract: DetectorReplayContract,
    pub(super) registry: Option<RegistryReceipt>,
    pub(super) inventory: ReplayInventory,
    pub(super) compilation: CompilationReport,
    pub(super) fixtures: Vec<FixtureReport>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReplayInventory {
    pub(super) fixtures: usize,
    pub(super) targets: usize,
    pub(super) evidence_bindings: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) sha256: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CompilationReport {
    pub(super) status: CompilationStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) metadata_sha256: Option<String>,
    pub(super) targets: Vec<TargetReport>,
    pub(super) processes: Vec<ProcessReport>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum CompilationStatus {
    Passed,
    HarnessError,
}

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TargetReport {
    pub(super) package: String,
    pub(super) kind: String,
    pub(super) name: String,
}

impl From<&ReplayTarget> for TargetReport {
    fn from(target: &ReplayTarget) -> Self {
        Self {
            package: target.package.clone(),
            kind: target.kind.clone(),
            name: target.name.clone(),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub(super) enum ProcessReport {
    Completed {
        role: String,
        execution_id: String,
        exit: ProcessExitReport,
        resources: ProcessResourceReport,
        termination: ProcessTerminationReport,
        logs: Vec<ArtifactRef>,
    },
    LifecycleError {
        role: String,
        execution_id: String,
        message: String,
        logs: Vec<ArtifactRef>,
    },
}

impl ProcessReport {
    pub(super) fn logs(&self) -> &[ArtifactRef] {
        match self {
            Self::Completed { logs, .. } | Self::LifecycleError { logs, .. } => logs,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProcessExitReport {
    pub(super) success: bool,
    pub(super) exit_code: Option<i32>,
    pub(super) timed_out: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProcessResourceReport {
    pub(super) duration_ms: u64,
    pub(super) peak_rss_kib: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ProcessTerminationReport {
    pub(super) process_group: bool,
    pub(super) term_signal_sent: bool,
    pub(super) termination_grace_ms: u64,
    pub(super) kill_signal_sent: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FixtureReport {
    pub(super) target: TargetReport,
    pub(super) test_name: String,
    pub(super) source: FixtureSourceReport,
    pub(super) evidence: Vec<ReplayEvidenceReport>,
    pub(super) status: FixtureReplayStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) challenge: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) process: Option<ProcessReport>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct FixtureSourceReport {
    pub(super) fixture_symbol: String,
    pub(super) fixture_path: String,
    pub(super) fixture_sha256: String,
    pub(super) detector_symbol: String,
    pub(super) detector_path: String,
    pub(super) detector_sha256: String,
    pub(super) source_graph_sha256: String,
    pub(super) registered_identity: String,
    pub(super) expected_witnesses: BTreeMap<String, usize>,
}

impl TryFrom<&ReplayFixture> for FixtureSourceReport {
    type Error = String;

    fn try_from(fixture: &ReplayFixture) -> Result<Self, Self::Error> {
        Ok(Self {
            fixture_symbol: fixture.fixture.clone(),
            fixture_path: fixture
                .fixture_path
                .to_str()
                .ok_or_else(|| "detector replay fixture path is not UTF-8".to_owned())?
                .to_owned(),
            fixture_sha256: fixture.fixture_sha256.clone(),
            detector_symbol: fixture.detector.clone(),
            detector_path: fixture
                .detector_path
                .to_str()
                .ok_or_else(|| "detector replay detector path is not UTF-8".to_owned())?
                .to_owned(),
            detector_sha256: fixture.detector_sha256.clone(),
            source_graph_sha256: fixture.source_graph_sha256.clone(),
            registered_identity: fixture.registered_identity.clone(),
            expected_witnesses: fixture.expected_witnesses.clone(),
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ReplayEvidenceReport {
    pub(super) invariant_id: String,
    pub(super) evidence_id: String,
}

impl From<&ReplayEvidence> for ReplayEvidenceReport {
    fn from(evidence: &ReplayEvidence) -> Self {
        Self {
            invariant_id: evidence.invariant_id.clone(),
            evidence_id: evidence.evidence_id.clone(),
        }
    }
}
