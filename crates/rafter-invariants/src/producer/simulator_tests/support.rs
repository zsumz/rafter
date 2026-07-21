//! Shared simulator scenario fixtures and passing evidence builders.

use std::collections::BTreeMap;
#[cfg(unix)]
use std::fmt::Write as _;
#[cfg(unix)]
use std::{fs, path::Path};

#[cfg(unix)]
use super::super::test_exec::TestOutcome;
use super::super::DetectorRun;
use crate::EvidenceDescriptor;
#[cfg(unix)]
use crate::{CheckCompletion, EvidenceStatus, SimulatorIdentity};

pub(super) fn safety_descriptor(descriptors: &[EvidenceDescriptor]) -> &EvidenceDescriptor {
    descriptors
        .iter()
        .find(|descriptor| {
            descriptor.layer == "simulator"
                && descriptor
                    .simulator
                    .as_ref()
                    .is_some_and(|identity| identity.liveness_report.is_none())
        })
        .expect("safety simulator descriptor")
}

pub(super) fn standalone_safety_descriptor(
    descriptors: &[EvidenceDescriptor],
) -> EvidenceDescriptor {
    let mut descriptor = safety_descriptor(descriptors).clone();
    descriptor
        .simulator
        .as_mut()
        .expect("simulator identity")
        .negative_test = None;
    descriptor
}

pub(super) fn model_fixture(
    events: BTreeMap<String, Vec<serde_json::Value>>,
) -> crate::producer::simulator::model::SimulatorExecution {
    crate::producer::simulator::model::SimulatorExecution {
        events,
        artifacts: Vec::new(),
        runtime_peak_rss_kib: 0,
        build_peak_rss_kib: 0,
        duration_ms: 0,
        build_duration_ms: 0,
        processes_succeeded: true,
        harness_errors: Vec::new(),
    }
}

pub(super) fn empty_detectors() -> DetectorRun {
    DetectorRun {
        outcomes: BTreeMap::new(),
        artifacts: Vec::new(),
        peak_rss_kib: 0,
        duration_ms: 0,
        harness_error: None,
    }
}

#[cfg(unix)]
pub(super) fn passing_event_stream(identity: &SimulatorIdentity) -> String {
    identity
        .checks
        .iter()
        .fold(String::new(), |mut output, check| {
            writeln!(
                output,
                "RAFTER_EVENT {}",
                serde_json::json!({
                    "event": "exhaustive-check",
                    "check_id": check,
                    "status": "pass",
                    "classification": null,
                    "unique_protocol_states": identity.minimum_protocol_states.unwrap_or_default(),
                    "unique_verifier_states": identity.minimum_verifier_states.unwrap_or_default(),
                    "observations": {
                        identity.required_observation.clone(): identity.minimum_observation,
                    },
                })
            )
            .expect("append passing simulator event");
            output
        })
}

#[cfg(unix)]
pub(super) fn passing_detectors(identity: &SimulatorIdentity) -> DetectorRun {
    DetectorRun {
        outcomes: identity
            .negative_test
            .as_ref()
            .map(|test| (test.check_id(), passing_detector_outcome()))
            .into_iter()
            .collect(),
        artifacts: Vec::new(),
        peak_rss_kib: 0,
        duration_ms: 0,
        harness_error: None,
    }
}

#[cfg(unix)]
pub(super) fn passing_detectors_for_descriptors(descriptors: &[EvidenceDescriptor]) -> DetectorRun {
    DetectorRun {
        outcomes: descriptors
            .iter()
            .filter_map(|descriptor| descriptor.simulator.as_ref())
            .filter_map(|identity| identity.negative_test.as_ref())
            .map(|test| (test.check_id(), passing_detector_outcome()))
            .collect(),
        artifacts: Vec::new(),
        peak_rss_kib: 0,
        duration_ms: 0,
        harness_error: None,
    }
}

#[cfg(unix)]
fn passing_detector_outcome() -> TestOutcome {
    TestOutcome {
        completion: CheckCompletion::Completed,
        status: EvidenceStatus::Pass,
        classification: None,
        message: None,
        observations: BTreeMap::new(),
        duration_ms: 1,
        peak_rss_kib: 1,
        artifacts: Vec::new(),
    }
}

#[cfg(unix)]
pub(super) fn timeout_fixture_output_dir(suffix: &str) -> std::path::PathBuf {
    let path = Path::new("target/rafter-invariants/tests").join(format!(
        "simulator-producer-timeout-{suffix}-{}",
        std::process::id()
    ));
    if path.exists() {
        fs::remove_dir_all(&path).expect("remove stale timeout fixture artifacts");
    }
    path
}
