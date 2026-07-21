//! End-to-end verification of authenticated simulator evidence.

use std::{collections::BTreeMap, path::Path};

use crate::{
    contract::catalog::Catalog,
    evidence::ResultBundle,
    verification::{AggregateError, AuthenticatedArtifacts},
};

use super::{
    detector::{
        verify_negative_detector_evidence_authenticated, DetectorLogVerifier,
        NegativeDetectorContext,
    },
    event::{inspect_machine_events, simulator_events, verify_nonpassing_event_classification},
    observation::verify_simulator_observations,
};

pub(crate) fn verify_simulator_logs(
    bundle: &ResultBundle,
    root: &Path,
    source_root: &Path,
    catalog: &Catalog,
    authenticated: &AuthenticatedArtifacts,
    log_verifier: &dyn DetectorLogVerifier,
) -> Result<Vec<String>, AggregateError> {
    let schedule =
        super::schedule::verify_simulator_schedule_authenticated(bundle, root, authenticated)?;
    let mut diagnostics = schedule.diagnostics;
    let events = simulator_events(&bundle.profile, schedule.logs)?;
    let profile_descriptors = catalog
        .required_evidence(&bundle.execution.plan.contract)
        .into_values()
        .flatten()
        .filter(|descriptor| descriptor.layer == "simulator")
        .collect::<Vec<_>>();
    let inspection = inspect_machine_events(&bundle.profile, &profile_descriptors, &events);
    diagnostics.extend(inspection.diagnostics);
    let descriptors = profile_descriptors
        .iter()
        .map(|descriptor| (descriptor.evidence_id(), descriptor))
        .collect::<BTreeMap<_, _>>();
    let liveness_contracts = profile_descriptors
        .iter()
        .filter_map(|descriptor| {
            descriptor
                .simulator
                .as_ref()?
                .liveness_report
                .as_ref()
                .map(|contract| (contract.feature_id.clone(), contract.clone()))
        })
        .collect::<BTreeMap<_, _>>()
        .into_values()
        .collect::<Vec<_>>();
    let mut test_logs = BTreeMap::<String, String>::new();
    let mut detector_sources = crate::verification::DetectorFixtureAnalysis::default();
    let mut negative_detector = NegativeDetectorContext {
        bundle,
        root,
        source_root,
        authenticated,
        detector_sources: &mut detector_sources,
        test_logs: &mut test_logs,
        log_verifier,
    };
    for check in &bundle.execution.checks {
        let [evidence_id] = check.evidence_ids.as_slice() else {
            return Err(AggregateError::new(format!(
                "simulator check {} must bind exactly one evidence record",
                check.check_id
            )));
        };
        let descriptor = descriptors.get(evidence_id).ok_or_else(|| {
            AggregateError::new(format!(
                "simulator check {} names unknown evidence {evidence_id}",
                check.check_id
            ))
        })?;
        let identity = descriptor.simulator.as_ref().ok_or_else(|| {
            AggregateError::new(format!(
                "simulator check {} names non-simulator evidence",
                check.check_id
            ))
        })?;
        verify_nonpassing_event_classification(
            bundle,
            check,
            &descriptor.invariant_id,
            identity,
            &events,
            inspection.global_issue,
        )?;
        verify_simulator_observations(bundle, check, identity, &liveness_contracts, &events)?;
        verify_negative_detector_evidence_authenticated(
            &mut negative_detector,
            check,
            descriptor,
            identity,
        )?;
    }
    diagnostics.sort();
    diagnostics.dedup();
    Ok(diagnostics)
}
