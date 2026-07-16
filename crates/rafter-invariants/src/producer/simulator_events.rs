use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use super::{merge_issue, simulator_model, SimulatorIssue};
use crate::EvidenceDescriptor;

pub(super) fn simulator_event_issue(
    check: &str,
    expected_invariant_id: &str,
    event: &Value,
) -> Option<SimulatorIssue> {
    let message = event.get("message").and_then(Value::as_str).map_or_else(
        || format!("simulator check `{check}` did not pass"),
        str::to_owned,
    );
    match (
        event.get("status").and_then(Value::as_str),
        event.get("classification"),
    ) {
        (Some("pass"), None | Some(Value::Null)) => None,
        (Some("fail"), Some(Value::String(classification)))
            if classification == "invariant-violation" =>
        {
            match machine_invariant_id(check, event) {
                Ok(observed) if observed == expected_invariant_id => {
                    Some(SimulatorIssue::InvariantViolation(message))
                }
                Ok(observed) => Some(SimulatorIssue::CoverageNotReached(format!(
                    "simulator check `{check}` stopped after invariant {observed} failed before proving {expected_invariant_id}"
                ))),
                Err(error) => Some(SimulatorIssue::HarnessError(error)),
            }
        }
        (Some("incomplete"), Some(Value::String(classification)))
            if classification == "coverage-not-reached" =>
        {
            Some(SimulatorIssue::CoverageNotReached(message))
        }
        (Some("error"), Some(Value::String(classification)))
            if classification == "harness-error" =>
        {
            Some(SimulatorIssue::HarnessError(message))
        }
        _ => Some(SimulatorIssue::HarnessError(invalid_event_pair_message(
            check, event,
        ))),
    }
}

pub(super) fn simulator_event_inventory_issue(
    profile: &str,
    descriptors: &[EvidenceDescriptor],
    events: &BTreeMap<String, Vec<Value>>,
) -> Option<SimulatorIssue> {
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
    let mut issue = None;
    for (indexed_check_id, indexed_events) in events {
        for event in indexed_events.iter().filter(|event| {
            event.get("check_id").and_then(Value::as_str) == Some(indexed_check_id.as_str())
        }) {
            let check_id = indexed_check_id.as_str();
            let canonical = simulator_model::canonical_check_id(profile, check_id);
            let route = routes.get(check_id).or_else(|| {
                canonical
                    .as_ref()
                    .and_then(|canonical| routes.get(canonical))
            });
            if let Some(route) = route {
                verify_invariant_route(check_id, event, route, &mut issue);
                continue;
            }
            if allowed_summary_event(profile, check_id, event) {
                merge_issue(&mut issue, summary_event_issue(check_id, event));
            } else {
                unknown.insert(check_id.to_owned());
            }
        }
    }
    if !unknown.is_empty() {
        merge_issue(
            &mut issue,
            Some(SimulatorIssue::HarnessError(format!(
                "simulator emitted unclaimed machine event check IDs: {}",
                unknown.into_iter().collect::<Vec<_>>().join(", ")
            ))),
        );
    }
    issue
}

fn verify_invariant_route(
    check_id: &str,
    event: &Value,
    route: &BTreeSet<String>,
    issue: &mut Option<SimulatorIssue>,
) {
    if event.get("status").and_then(Value::as_str) != Some("fail")
        || event.get("classification").and_then(Value::as_str) != Some("invariant-violation")
    {
        return;
    }
    match machine_invariant_id(check_id, event) {
        Ok(invariant_id) if route.contains(invariant_id) => {}
        Ok(invariant_id) => merge_issue(
            issue,
            Some(SimulatorIssue::HarnessError(format!(
                "simulator check `{check_id}` emitted invariant {invariant_id} without a registered failure route"
            ))),
        ),
        Err(error) => merge_issue(issue, Some(SimulatorIssue::HarnessError(error))),
    }
}

fn summary_event_issue(check: &str, event: &Value) -> Option<SimulatorIssue> {
    let message = event.get("message").and_then(Value::as_str).map_or_else(
        || format!("simulator check `{check}` did not pass"),
        str::to_owned,
    );
    match (
        event.get("status").and_then(Value::as_str),
        event.get("classification"),
    ) {
        (Some("pass"), None | Some(Value::Null)) => None,
        (Some("incomplete"), Some(Value::String(classification)))
            if classification == "coverage-not-reached" =>
        {
            Some(SimulatorIssue::CoverageNotReached(message))
        }
        (Some("error"), Some(Value::String(classification)))
            if classification == "harness-error" =>
        {
            Some(SimulatorIssue::HarnessError(message))
        }
        _ => Some(SimulatorIssue::HarnessError(invalid_event_pair_message(
            check, event,
        ))),
    }
}

fn machine_invariant_id<'a>(check: &str, event: &'a Value) -> Result<&'a str, String> {
    if event.get("event").and_then(Value::as_str) != Some("check-failure")
        || event.get("event_version").and_then(Value::as_u64) != Some(2)
    {
        return Err(format!(
            "simulator check `{check}` invariant violation used an unsupported machine-event contract"
        ));
    }
    let invariant_id = event
        .get("invariant_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            format!("simulator check `{check}` invariant violation omitted invariant_id")
        })?;
    let valid_shape = invariant_id.len() == 5
        && invariant_id.as_bytes()[0..2]
            .iter()
            .all(u8::is_ascii_uppercase)
        && invariant_id.as_bytes()[2] == b'-'
        && invariant_id.as_bytes()[3..5].iter().all(u8::is_ascii_digit);
    let label = event
        .get("invariant")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            format!("simulator check `{check}` invariant violation omitted its invariant label")
        })?;
    if !valid_shape
        || !label
            .strip_prefix(invariant_id)
            .is_some_and(|suffix| suffix.starts_with(' '))
    {
        return Err(format!(
            "simulator check `{check}` has an invalid invariant identity: id={invariant_id:?}, label={label:?}"
        ));
    }
    Ok(invariant_id)
}

fn allowed_summary_event(profile: &str, check_id: &str, event: &Value) -> bool {
    matches!(profile, "nightly" | "weekly")
        && event.get("event").and_then(Value::as_str) == Some("profile-total")
        && check_id == format!("raft-profile-total-{profile}")
}

fn invalid_event_pair_message(check: &str, event: &Value) -> String {
    let field = |name| {
        event
            .get(name)
            .map_or_else(|| "<missing>".to_owned(), Value::to_string)
    };
    format!(
        "simulator check `{check}` has invalid status/classification pair: status={}, classification={}",
        field("status"),
        field("classification")
    )
}
