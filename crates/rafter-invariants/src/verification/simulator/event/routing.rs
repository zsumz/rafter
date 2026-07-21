//! Routing and classification of simulator machine-event failures.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::{
    contract::{catalog::EvidenceDescriptor, SimulatorIdentity},
    evidence::{CheckReceipt, ResultBundle},
    verification::AggregateError,
};

use super::{machine_invariant_id, merge_raw_issue, receipt_issue, RawEventIssue};

pub(crate) struct MachineEventInspection {
    pub(crate) global_issue: Option<RawEventIssue>,
    pub(crate) diagnostics: Vec<String>,
}

pub(crate) fn verify_nonpassing_event_classification(
    bundle: &ResultBundle,
    check: &CheckReceipt,
    invariant_id: &str,
    identity: &SimulatorIdentity,
    events: &BTreeMap<String, Vec<Value>>,
    global_issue: Option<RawEventIssue>,
) -> Result<(), AggregateError> {
    let mut expected = global_issue;
    for event in identity
        .checks
        .iter()
        .flat_map(|name| events.get(name).into_iter().flatten())
    {
        let (candidate, _) = raw_event_issue(
            event
                .get("check_id")
                .and_then(Value::as_str)
                .unwrap_or("<missing>"),
            event,
            Some(invariant_id),
        );
        if candidate.is_some_and(|candidate| {
            expected.is_none_or(|expected| candidate.rank() > expected.rank())
        }) {
            expected = candidate;
        }
    }
    let Some(expected) = expected else {
        return Ok(());
    };
    let outcomes = bundle
        .results
        .iter()
        .filter(|result| result.execution_id == check.execution_id)
        .map(|result| (result.status, result.classification))
        .collect::<Vec<_>>();
    if outcomes.is_empty()
        || outcomes.iter().any(|outcome| {
            receipt_issue(*outcome).is_none_or(|actual| {
                actual.rank() < expected.rank()
                    || (actual == RawEventIssue::InvariantViolation
                        && expected != RawEventIssue::InvariantViolation)
            })
        })
    {
        return Err(AggregateError::new(format!(
            "simulator check {} receipt does not preserve its raw semantic failure classification",
            check.check_id
        )));
    }
    Ok(())
}

pub(crate) fn inspect_machine_events(
    profile: &str,
    descriptors: &[EvidenceDescriptor],
    events: &BTreeMap<String, Vec<Value>>,
) -> MachineEventInspection {
    let mut routes = BTreeMap::<String, BTreeSet<String>>::new();
    for descriptor in descriptors {
        if let Some(identity) = descriptor.simulator.as_ref() {
            for check in &identity.checks {
                routes
                    .entry(check.clone())
                    .or_default()
                    .insert(descriptor.invariant_id.clone());
            }
        }
    }
    let mut unknown = BTreeSet::new();
    let mut diagnostics = BTreeSet::new();
    let mut global_issue = None;
    for (indexed_check_id, indexed_events) in events {
        for event in indexed_events.iter().filter(|event| {
            event.get("check_id").and_then(Value::as_str) == Some(indexed_check_id.as_str())
        }) {
            let check_id = indexed_check_id.as_str();
            let (event_issue, diagnostic) = raw_event_issue(check_id, event, None);
            diagnostics.extend(diagnostic);
            let canonical =
                crate::contract::profile::canonical_simulator_check_id(profile, check_id);
            let route = routes.get(check_id).or_else(|| {
                canonical
                    .as_ref()
                    .and_then(|canonical| routes.get(canonical))
            });
            if let Some(route) = route {
                if event_issue == Some(RawEventIssue::InvariantViolation) {
                    match machine_invariant_id(check_id, event) {
                        Ok(invariant_id) if route.contains(invariant_id) => {}
                        Ok(invariant_id) => {
                            diagnostics.insert(format!(
                                "simulator check `{check_id}` emitted invariant {invariant_id} without a registered failure route"
                            ));
                            merge_raw_issue(&mut global_issue, Some(RawEventIssue::HarnessError));
                        }
                        Err(error) => {
                            diagnostics.insert(error);
                            merge_raw_issue(&mut global_issue, Some(RawEventIssue::HarnessError));
                        }
                    }
                }
                continue;
            }
            if allowed_summary_event(profile, check_id, event) {
                merge_raw_issue(&mut global_issue, event_issue);
            } else {
                unknown.insert(check_id.to_owned());
            }
        }
    }
    if !unknown.is_empty() {
        diagnostics.insert(format!(
            "simulator emitted unclaimed machine event check IDs: {}",
            unknown.into_iter().collect::<Vec<_>>().join(", ")
        ));
        merge_raw_issue(&mut global_issue, Some(RawEventIssue::HarnessError));
    }
    MachineEventInspection {
        global_issue,
        diagnostics: diagnostics.into_iter().collect(),
    }
}

pub(crate) fn raw_event_issue(
    check_id: &str,
    event: &Value,
    expected_invariant_id: Option<&str>,
) -> (Option<RawEventIssue>, Option<String>) {
    let issue = match (
        event.get("status").and_then(Value::as_str),
        event.get("classification"),
    ) {
        (Some("pass"), None | Some(Value::Null)) => {
            if event.get("event").and_then(Value::as_str) == Some("profile-total") {
                return (None, None);
            }
            return match super::verified_passing_simulator_event_contract(check_id, event) {
                Ok(()) => (None, None),
                Err(error) => (Some(RawEventIssue::HarnessError), Some(error)),
            };
        }
        (Some("fail"), Some(Value::String(classification)))
            if classification == "invariant-violation" =>
        {
            match machine_invariant_id(check_id, event) {
                Ok(observed)
                    if expected_invariant_id.is_none_or(|expected| expected == observed) =>
                {
                    RawEventIssue::InvariantViolation
                }
                Ok(_) => RawEventIssue::CoverageNotReached,
                Err(error) => return (Some(RawEventIssue::HarnessError), Some(error)),
            }
        }
        (Some("incomplete"), Some(Value::String(classification)))
            if classification == "coverage-not-reached" =>
        {
            RawEventIssue::CoverageNotReached
        }
        (Some("error"), Some(Value::String(classification)))
            if classification == "harness-error" =>
        {
            RawEventIssue::HarnessError
        }
        _ => {
            return (
                Some(RawEventIssue::HarnessError),
                Some(invalid_event_pair_message(check_id, event)),
            )
        }
    };
    (Some(issue), None)
}

fn allowed_summary_event(profile: &str, check_id: &str, event: &Value) -> bool {
    matches!(profile, "nightly" | "weekly")
        && event.get("event").and_then(Value::as_str) == Some("profile-total")
        && check_id == format!("raft-profile-total-{profile}")
}

fn invalid_event_pair_message(check_id: &str, event: &Value) -> String {
    let field = |name| {
        event
            .get(name)
            .map_or_else(|| "<missing>".to_owned(), Value::to_string)
    };
    format!(
        "simulator check `{check_id}` has invalid status/classification pair: status={}, classification={}",
        field("status"),
        field("classification")
    )
}
