//! Test-only adapters for adversarial simulator evidence scenarios.

use std::{collections::BTreeMap, path::Path};

use crate::{
    contract::{catalog::EvidenceDescriptor, SimulatorIdentity},
    evidence::{CheckReceipt, ResultBundle},
    verification::{AggregateError, DetectorFixtureAnalysis},
};

use super::detector::{
    verify_negative_detector_evidence_authenticated, DetectorLogVerifier, NegativeDetectorContext,
};

pub(crate) use super::{
    detector::verify_negative_fixture_binding,
    event::{
        index_simulator_event, inspect_machine_events, raw_event_issue,
        verified_passing_simulator_event_contract, verify_nonpassing_event_classification,
        RawEventIssue,
    },
    observation::{
        derive_check_contract_issue, derive_simulator_observation_counts,
        verify_composite_observation, verify_simulator_observations,
    },
};

pub(crate) struct DetectorTestHarness<'a> {
    detector_sources: &'a mut DetectorFixtureAnalysis,
    test_logs: &'a mut BTreeMap<String, String>,
    log_verifier: &'a dyn DetectorLogVerifier,
}

impl<'a> DetectorTestHarness<'a> {
    pub(crate) fn new(
        detector_sources: &'a mut DetectorFixtureAnalysis,
        test_logs: &'a mut BTreeMap<String, String>,
        log_verifier: &'a dyn DetectorLogVerifier,
    ) -> Self {
        Self {
            detector_sources,
            test_logs,
            log_verifier,
        }
    }
}

pub(crate) fn verify_negative_detector_evidence_with(
    bundle: &ResultBundle,
    root: &Path,
    check: &CheckReceipt,
    descriptor: &EvidenceDescriptor,
    identity: &SimulatorIdentity,
    harness: DetectorTestHarness<'_>,
) -> Result<(), AggregateError> {
    let DetectorTestHarness {
        detector_sources,
        test_logs,
        log_verifier,
    } = harness;
    let authenticated = crate::verification::snapshot_available_artifacts(bundle, root)?;
    let mut context = NegativeDetectorContext {
        bundle,
        root,
        source_root: root,
        authenticated: &authenticated,
        detector_sources,
        test_logs,
        log_verifier,
    };
    verify_negative_detector_evidence_authenticated(&mut context, check, descriptor, identity)
}
