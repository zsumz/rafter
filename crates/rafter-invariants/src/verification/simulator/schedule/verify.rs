//! Orchestration over authenticated simulator logs and schedule contracts.

use std::path::Path;

use super::{
    events::{scan_machine_events, ScannedSimulatorLog},
    invocation::verify_simulator_invocations,
    profile::validate_scanned_simulator_schedule,
};
use crate::{
    evidence::ResultBundle,
    verification::{AggregateError, AuthenticatedArtifacts},
};

pub(crate) struct VerifiedSimulatorSchedule<'a> {
    pub(crate) diagnostics: Vec<String>,
    pub(crate) logs: Vec<ScannedSimulatorLog<'a>>,
}

pub(crate) fn verify_simulator_schedule_authenticated<'a>(
    bundle: &ResultBundle,
    root: &Path,
    authenticated: &'a AuthenticatedArtifacts,
) -> Result<VerifiedSimulatorSchedule<'a>, AggregateError> {
    let configuration = bundle
        .execution
        .plan
        .contract
        .runners
        .get("simulator")
        .ok_or_else(|| AggregateError::new("simulator runner contract is missing".to_owned()))?
        .simulator_configuration()
        .map_err(|error| {
            AggregateError::new(format!("parse typed simulator runner contract: {error}"))
        })?;
    configuration
        .validate_profile(&bundle.profile)
        .map_err(|error| AggregateError::new(format!("validate simulator contract: {error}")))?;
    let logs = bundle
        .execution
        .artifacts
        .iter()
        .filter(|artifact| artifact.kind == "simulator-log")
        .collect::<Vec<_>>();
    let sources = logs
        .iter()
        .map(|log| authenticated.text(log))
        .collect::<Result<Vec<_>, _>>()?;
    let invocation =
        verify_simulator_invocations(bundle, root, &configuration, &sources, authenticated)?;
    let mut diagnostics = invocation.diagnostics;
    let mut event_diagnostics = Vec::new();
    let scanned_logs = sources
        .iter()
        .enumerate()
        .map(|(index, source)| {
            let (events, diagnostics) =
                scan_machine_events(source, &format!("simulator log {index}"));
            event_diagnostics.extend(diagnostics);
            ScannedSimulatorLog { source, events }
        })
        .collect::<Vec<_>>();
    if invocation.complete && event_diagnostics.is_empty() {
        validate_scanned_simulator_schedule(
            &bundle.profile,
            &bundle.source_ref,
            &configuration,
            &scanned_logs,
        )?;
    }
    diagnostics.extend(event_diagnostics);
    Ok(VerifiedSimulatorSchedule {
        diagnostics,
        logs: scanned_logs,
    })
}

#[cfg(test)]
pub(crate) fn verify_simulator_schedule(
    bundle: &ResultBundle,
    root: &Path,
) -> Result<Vec<String>, AggregateError> {
    let authenticated = crate::verification::snapshot_available_artifacts(bundle, root)?;
    verify_simulator_schedule_authenticated(bundle, root, &authenticated)
        .map(|verified| verified.diagnostics)
}
