//! Typed fresh-replay outcomes before artifact publication and evidence qualification.

use serde::{Deserialize, Serialize};

use super::{
    execution::{CompilationFailure, CompiledReplay},
    model::ReplayEvidence,
    process::{ReplayProcessOutput, RetainedProcessDiagnostics},
    ReplayTarget,
};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum FixtureReplayStatus {
    Passed,
    HarnessError,
}

pub(super) struct FixtureReplayResult {
    pub(super) target: ReplayTarget,
    pub(super) test_name: String,
    pub(super) evidence: Vec<ReplayEvidence>,
    pub(super) status: FixtureReplayStatus,
    pub(super) token: Option<String>,
    pub(super) challenge: Option<String>,
    pub(super) message: Option<String>,
    pub(super) output: Option<ReplayProcessOutput>,
    pub(super) retained_diagnostics: Option<RetainedProcessDiagnostics>,
}

pub(super) struct DetectorReplayRun {
    pub(super) compilation: CompiledReplay,
    pub(super) fixtures: Vec<FixtureReplayResult>,
}

pub(super) enum DetectorReplayAttempt {
    Completed(Box<DetectorReplayRun>),
    CompilationFailed(Box<CompilationFailure>),
}
