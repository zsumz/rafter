//! Independent lease-isolation artifact interpretation.

use std::{collections::BTreeMap, path::Path};

use crate::{
    evidence::ArtifactRef,
    verification::{AggregateError, AuthenticatedArtifacts},
};

use super::observation::MARKERS;

mod history;
mod marker;
mod sequence;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum LeaseArtifactStatus {
    #[default]
    Missing,
    Complete,
    Incomplete,
    Violation,
    ViolationWithHarnessError,
    HarnessError,
}

pub(super) struct MarkerScan {
    pub(super) values: BTreeMap<&'static str, u64>,
    pub(super) status: LeaseArtifactStatus,
    probe: Option<LeaseProbe>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct LeaseProbe {
    client: String,
    message: u64,
}

pub(super) fn scan_node_logs(
    artifacts: &[&ArtifactRef],
    authenticated: &AuthenticatedArtifacts,
) -> Result<MarkerScan, AggregateError> {
    let mut values = MARKERS.into_iter().map(|name| (name, 0)).collect();
    let mut lease_events = Vec::new();
    let mut parse_errors = 0;
    let logs = artifacts
        .iter()
        .filter(|artifact| artifact.kind == "maelstrom-node-log")
        .collect::<Vec<_>>();
    if logs.is_empty() {
        return Err(error("Maelstrom trial has no captured node logs"));
    }
    for artifact in logs {
        let source_node = Path::new(&artifact.path)
            .file_stem()
            .and_then(|name| name.to_str())
            .ok_or_else(|| error("Maelstrom node log artifact has no UTF-8 file stem"))?;
        marker::scan(
            authenticated.text(artifact)?,
            source_node,
            &mut values,
            &mut lease_events,
            &mut parse_errors,
        )?;
    }
    let status = finalize_scan(&mut values, &lease_events, parse_errors);
    let probe = lease_events
        .iter()
        .find(|event| event.phase == "read-buffered")
        .map(|event| LeaseProbe {
            client: event.client.clone(),
            message: event.message,
        });
    Ok(MarkerScan {
        values,
        status,
        probe,
    })
}

pub(super) fn bind_history(
    scan: &mut MarkerScan,
    artifacts: &[&ArtifactRef],
    authenticated: &AuthenticatedArtifacts,
) -> Result<(), AggregateError> {
    if scan.status != LeaseArtifactStatus::Complete {
        return Ok(());
    }
    let histories = artifacts
        .iter()
        .filter(|artifact| {
            artifact.kind == "maelstrom-store-file"
                && Path::new(&artifact.path).ends_with("store/history.edn")
        })
        .collect::<Vec<_>>();
    let matches = match (histories.as_slice(), scan.probe.as_ref()) {
        ([history], Some(probe)) => {
            history::completion_count(authenticated.text(history)?, &probe.client, probe.message)?
        }
        _ => 0,
    };
    if matches == 1 {
        add(&mut scan.values, "lease_history_probe_matches", 1);
    } else {
        add(&mut scan.values, "lease_history_probe_mismatches", 1);
        scan.values.insert("lease_sequence_complete", 0);
        scan.values.insert("lease_sequence_invalid", 1);
        scan.status = LeaseArtifactStatus::HarnessError;
    }
    Ok(())
}

pub(crate) fn finalize_scan(
    values: &mut BTreeMap<&'static str, u64>,
    events: &[marker::LeaseMarker],
    parse_errors: u64,
) -> LeaseArtifactStatus {
    let derived = match sequence::rederive(events) {
        Ok(LeaseArtifactStatus::Complete) => {
            add(values, "lease_sequence_complete", 1);
            LeaseArtifactStatus::Complete
        }
        Ok(LeaseArtifactStatus::Missing) => LeaseArtifactStatus::Missing,
        Ok(status) => {
            add(values, "lease_sequence_invalid", 1);
            status
        }
        Err(()) => {
            add(values, "lease_sequence_invalid", 1);
            LeaseArtifactStatus::HarnessError
        }
    };
    if parse_errors == 0 {
        return derived;
    }
    values.insert("lease_sequence_complete", 0);
    add(values, "lease_sequence_invalid", parse_errors);
    if matches!(
        derived,
        LeaseArtifactStatus::Violation | LeaseArtifactStatus::ViolationWithHarnessError
    ) {
        LeaseArtifactStatus::ViolationWithHarnessError
    } else {
        LeaseArtifactStatus::HarnessError
    }
}

fn add(values: &mut BTreeMap<&'static str, u64>, name: &'static str, amount: u64) {
    *values.entry(name).or_default() += amount;
}

fn error(message: impl Into<String>) -> AggregateError {
    AggregateError::new(message.into())
}

#[cfg(test)]
pub(crate) use finalize_scan as finalize_lease_scan;
#[cfg(test)]
pub(crate) use history::{
    completion_count as history_completion_count,
    completion_count_with_limits as history_completion_count_with_limits, Limits as HistoryLimits,
};
#[cfg(test)]
pub(crate) use marker::{
    scan as scan_markers, scan_with_limits as scan_markers_with_limits,
    LeaseMarker as ArtifactLeaseMarker, Limits as MarkerLimits,
};
