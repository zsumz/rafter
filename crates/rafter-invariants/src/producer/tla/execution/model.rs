//! Typed TLA+ execution, probe, and process outcomes.

use std::collections::BTreeMap;

use crate::{
    evidence::ArtifactRef,
    producer::{process, tla_output},
};

use super::super::checkpoint;

pub(in crate::producer) struct TlaExecution {
    pub(in crate::producer) obligations: ObligationOutcome,
    pub(in crate::producer) main: Option<tla_output::TlcSummary>,
    pub(in crate::producer) main_progress: Option<tla_output::TlcProgress>,
    pub(in crate::producer) main_parse_error: Option<String>,
    pub(in crate::producer) main_status: MainStatus,
    pub(in crate::producer) trace_status: ProbeStatus,
    pub(in crate::producer) detector_status: ProbeStatus,
    pub(in crate::producer) detector_qualifications: BTreeMap<String, u64>,
    pub(in crate::producer) peak_rss_kib: u64,
    pub(in crate::producer) duration_ms: u64,
    pub(in crate::producer) artifacts: Vec<ArtifactRef>,
    pub(in crate::producer) checkpoint_report: Option<checkpoint::RecoveryReport>,
    pub(in crate::producer) checkpoint_error: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::producer) enum MainStatus {
    NotRun,
    Succeeded,
    Failed,
    TimedOut,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::producer) enum ProbeStatus {
    NotRun,
    Passed,
    Failed,
}

pub(super) struct TlcRun {
    pub(super) output: process::ProcessOutput,
    pub(super) artifact: ArtifactRef,
}

/// Why the obligation sequence stopped.
///
/// Both variants are red, but they are not the same red and an operator must
/// not have to guess which one they are reading. `Undischarged` is a statement
/// about the model: a theorem the layer claims did not hold, or did not drain
/// its frontier inside the budget its own calibration says is enough.
/// `Underfunded` is a statement about this harness: the obligation was never
/// given the budget it was promised, so it says nothing about the model at
/// all. Collapsing them is how a rebudgeting mistake comes to read as a broken
/// safety invariant.
pub(in crate::producer) enum ObligationFailure {
    Undischarged(String),
    Underfunded(String),
}

/// Aggregate result of the profile's focused proof obligations.
///
/// Obligations are sequential and fail fast, so a `Failed` outcome describes
/// the first obligation that did not discharge and carries observations only
/// for the obligations that actually ran. That prefix shape is deliberate and
/// is what the verifier independently reconstructs.
pub(in crate::producer) struct ObligationOutcome {
    pub(in crate::producer) status: ProbeStatus,
    pub(in crate::producer) failure: Option<ObligationFailure>,
    pub(in crate::producer) observations: BTreeMap<String, u64>,
    pub(super) peak_rss_kib: u64,
    pub(super) duration_ms: u64,
    pub(super) artifacts: Vec<ArtifactRef>,
}

impl Default for ObligationOutcome {
    fn default() -> Self {
        Self {
            status: ProbeStatus::NotRun,
            failure: None,
            observations: BTreeMap::new(),
            peak_rss_kib: 0,
            duration_ms: 0,
            artifacts: Vec::new(),
        }
    }
}

pub(super) struct DetectorRun {
    pub(super) run: TlcRun,
    pub(super) config_artifact: ArtifactRef,
}

pub(super) struct DetectorProbes {
    pub(super) succeeded: bool,
    pub(super) peak_rss_kib: u64,
    pub(super) duration_ms: u64,
    pub(super) qualifications: BTreeMap<String, u64>,
    pub(super) artifacts: Vec<ArtifactRef>,
}

impl Default for DetectorProbes {
    fn default() -> Self {
        Self {
            succeeded: true,
            peak_rss_kib: 0,
            duration_ms: 0,
            qualifications: BTreeMap::new(),
            artifacts: Vec::new(),
        }
    }
}
