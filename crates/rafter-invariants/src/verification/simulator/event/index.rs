//! Canonical indexing of authenticated simulator events.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::verification::AggregateError;

pub(crate) fn simulator_events(
    profile: &str,
    logs: Vec<super::super::schedule::ScannedSimulatorLog<'_>>,
) -> Result<BTreeMap<String, Vec<Value>>, AggregateError> {
    if logs.is_empty() {
        return Err(AggregateError::new(
            "simulator execution has no machine-readable logs".to_owned(),
        ));
    }
    let mut events = BTreeMap::<String, Vec<Value>>::new();
    for log in logs {
        for event in log.events {
            index_simulator_event(profile, event, &mut events)
                .map_err(|error| AggregateError::new(error.to_owned()))?;
        }
    }
    Ok(events)
}

pub(crate) fn index_simulator_event(
    profile: &str,
    event: Value,
    events: &mut BTreeMap<String, Vec<Value>>,
) -> Result<(), &'static str> {
    let check_id = event
        .get("check_id")
        .and_then(Value::as_str)
        .ok_or("simulator event scanner returned an event without check_id")?
        .to_owned();
    let canonical = crate::contract::profile::canonical_simulator_check_id(profile, &check_id);
    events.entry(check_id).or_default().push(event.clone());
    if let Some(canonical) = canonical {
        events.entry(canonical).or_default().push(event);
    }
    Ok(())
}
