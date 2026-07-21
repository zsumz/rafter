//! Typed simulator build, run-plan, and aggregate execution state.

use std::{collections::BTreeMap, ffi::OsString, path::PathBuf};

use serde_json::Value;

use crate::{
    evidence::ArtifactRef,
    execution::filesystem::{HeldDirectory, HeldFile},
};

pub(crate) struct SimulatorExecution {
    pub events: BTreeMap<String, Vec<Value>>,
    pub artifacts: Vec<ArtifactRef>,
    pub runtime_peak_rss_kib: u64,
    pub build_peak_rss_kib: u64,
    pub duration_ms: u64,
    pub build_duration_ms: u64,
    pub processes_succeeded: bool,
    pub harness_errors: Vec<String>,
}

pub(super) struct SimulatorBuild {
    pub(super) binary: PathBuf,
    pub(super) binary_handle: HeldFile,
    pub(super) target_dir: HeldDirectory,
    pub(super) artifacts: Vec<ArtifactRef>,
    pub(super) peak_rss_kib: u64,
    pub(super) duration_ms: u64,
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct ModelRun {
    pub(super) label: String,
    pub(super) arguments: Vec<OsString>,
}
