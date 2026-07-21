//! Structural contracts for accepted simulator machine events.

use serde_json::Value;

pub(crate) fn verified_passing_simulator_event_contract(
    check_id: &str,
    event: &Value,
) -> Result<(), String> {
    let expected_event_kind = if check_id.split('-').any(|segment| segment == "soak") {
        "soak-check"
    } else {
        "exhaustive-check"
    };
    let observations_are_counts = event
        .get("observations")
        .and_then(Value::as_object)
        .is_some_and(|observations| observations.values().all(Value::is_u64));
    let common = event.get("check_id").and_then(Value::as_str) == Some(check_id)
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
                && verified_string_array(event.get("observed_actions"))
                && verified_string_array(event.get("liveness_features"))
                && event.get("liveness_reports").is_some_and(Value::is_array)
        }
        _ => unreachable!("simulator passing event kinds are exhaustive or soak"),
    };
    if common && expected_shape {
        return Ok(());
    }
    Err(format!(
        "simulator check `{check_id}` has a malformed passing machine event: expected {expected_event_kind}, found {}",
        event
            .get("event")
            .and_then(Value::as_str)
            .unwrap_or("<missing>")
    ))
}

fn verified_string_array(value: Option<&Value>) -> bool {
    value
        .and_then(Value::as_array)
        .is_some_and(|values| values.iter().all(Value::is_string))
}

pub(crate) fn machine_invariant_id<'a>(
    check_id: &str,
    event: &'a Value,
) -> Result<&'a str, String> {
    if event.get("event").and_then(Value::as_str) != Some("check-failure")
        || event.get("event_version").and_then(Value::as_u64) != Some(2)
    {
        return Err(format!(
            "simulator check `{check_id}` invariant violation used an unsupported machine-event contract"
        ));
    }
    let invariant_id = event
        .get("invariant_id")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            format!("simulator check `{check_id}` invariant violation omitted invariant_id")
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
            format!("simulator check `{check_id}` invariant violation omitted its invariant label")
        })?;
    if !valid_shape
        || !label
            .strip_prefix(invariant_id)
            .is_some_and(|suffix| suffix.starts_with(' '))
    {
        return Err(format!(
            "simulator check `{check_id}` has an invalid invariant identity: id={invariant_id:?}, label={label:?}"
        ));
    }
    Ok(invariant_id)
}
