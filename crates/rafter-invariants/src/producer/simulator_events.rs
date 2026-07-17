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
        (Some("pass"), None | Some(Value::Null)) => {
            passing_simulator_event_contract(check, event)
                .err()
                .map(SimulatorIssue::HarnessError)
        }
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

pub(crate) fn passing_simulator_event_contract(check: &str, event: &Value) -> Result<(), String> {
    let expected_event_kind = expected_passing_event_kind(check);
    let observations_are_counts = event
        .get("observations")
        .and_then(Value::as_object)
        .is_some_and(|observations| observations.values().all(Value::is_u64));
    let common = event.get("check_id").and_then(Value::as_str) == Some(check)
        && event.get("status").and_then(Value::as_str) == Some("pass")
        && matches!(event.get("classification"), None | Some(Value::Null))
        && observations_are_counts;
    let expected_shape = match expected_event_kind {
        "exhaustive-check" => {
            event.get("event").and_then(Value::as_str) == Some(expected_event_kind)
                && event
                    .get("unique_protocol_states")
                    .and_then(Value::as_u64)
                    .is_some()
                && event
                    .get("unique_verifier_states")
                    .and_then(Value::as_u64)
                    .is_some()
        }
        "soak-check" => {
            event.get("event").and_then(Value::as_str) == Some(expected_event_kind)
                && event.get("seed").and_then(Value::as_u64).is_some()
                && event.get("steps").and_then(Value::as_u64).is_some()
                && event.get("duration_ms").and_then(Value::as_u64).is_some()
                && event
                    .get("execution_contract")
                    .is_some_and(Value::is_object)
                && string_array(event.get("observed_actions"))
                && string_array(event.get("liveness_features"))
                && event.get("liveness_reports").is_some_and(Value::is_array)
        }
        _ => unreachable!("simulator passing event kinds are exhaustive or soak"),
    };
    if common && expected_shape {
        return Ok(());
    }
    Err(format!(
        "simulator check `{check}` has a malformed passing machine event: expected {expected_event_kind}, found {}",
        event
            .get("event")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
    ))
}

fn expected_passing_event_kind(check: &str) -> &'static str {
    if check.split('-').any(|segment| segment == "soak") {
        "soak-check"
    } else {
        "exhaustive-check"
    }
}

fn string_array(value: Option<&Value>) -> bool {
    value
        .and_then(Value::as_array)
        .is_some_and(|values| values.iter().all(Value::is_string))
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::passing_simulator_event_contract;

    #[test]
    fn passing_soak_event_requires_its_complete_machine_shape() {
        let event = json!({
            "event": "soak-check",
            "check_id": "raft-soak",
            "status": "pass",
            "classification": null,
            "seed": 1,
            "steps": 320,
            "duration_ms": 4,
            "execution_contract": {},
            "observed_actions": ["tick"],
            "liveness_features": ["proposal-progress"],
            "observations": {"accepted_completed_liveness_proposals": 1},
            "liveness_reports": [],
        });
        passing_simulator_event_contract("raft-soak", &event)
            .expect("complete soak event is a passing machine receipt");

        for missing in [
            "seed",
            "steps",
            "duration_ms",
            "execution_contract",
            "observed_actions",
            "liveness_features",
            "observations",
            "liveness_reports",
        ] {
            let mut malformed = event.clone();
            malformed
                .as_object_mut()
                .expect("soak event object")
                .remove(missing);
            assert!(
                passing_simulator_event_contract("raft-soak", &malformed).is_err(),
                "accepted soak event without {missing}"
            );
        }
    }

    #[test]
    fn passing_event_contract_rejects_cross_kind_substitution() {
        let exhaustive = json!({
            "event": "exhaustive-check",
            "check_id": "raft-election",
            "status": "pass",
            "classification": null,
            "unique_protocol_states": 14_000,
            "unique_verifier_states": 18_000,
            "observations": {"election_certificates": 1},
        });
        let soak = json!({
            "event": "soak-check",
            "check_id": "raft-soak",
            "status": "pass",
            "classification": null,
            "seed": 1,
            "steps": 320,
            "duration_ms": 4,
            "execution_contract": {},
            "observed_actions": ["tick"],
            "liveness_features": ["proposal-progress"],
            "observations": {"accepted_completed_liveness_proposals": 1},
            "liveness_reports": [],
        });
        passing_simulator_event_contract("raft-election", &exhaustive)
            .expect("exhaustive safety check accepts its event kind");
        passing_simulator_event_contract("raft-soak", &soak)
            .expect("soak check accepts its event kind");

        let mut substituted_soak = soak;
        substituted_soak["check_id"] = json!("raft-election");
        let error = passing_simulator_event_contract("raft-election", &substituted_soak)
            .expect_err("exhaustive safety check must reject a passing soak event");
        assert!(error.contains("expected exhaustive-check, found soak-check"));

        let mut substituted_exhaustive = exhaustive;
        substituted_exhaustive["check_id"] = json!("raft-soak");
        let error = passing_simulator_event_contract("raft-soak", &substituted_exhaustive)
            .expect_err("soak check must reject a passing exhaustive event");
        assert!(error.contains("expected soak-check, found exhaustive-check"));
    }
}
