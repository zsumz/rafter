//! Bounded parsing for authenticated simulator machine events.

use serde_json::Value;

use super::super::EVENT_PREFIX;

const MAX_EVENTS_PER_LOG: usize = 4_096;
const MAX_EVENT_BYTES: usize = 64 * 1024;

pub(in crate::artifact_verify) struct ScannedSimulatorLog<'a> {
    pub(in crate::artifact_verify) source: &'a str,
    pub(in crate::artifact_verify) events: Vec<Value>,
}

pub(in crate::artifact_verify) fn scan_machine_events(
    log: &str,
    context: &str,
) -> (Vec<Value>, Vec<String>) {
    let mut events = Vec::new();
    let mut diagnostics = Vec::new();
    for source in log
        .lines()
        .filter_map(|line| line.strip_prefix(EVENT_PREFIX))
    {
        if events.len() == MAX_EVENTS_PER_LOG {
            diagnostics.push(format!(
                "parse {context}: event count exceeds {MAX_EVENTS_PER_LOG}"
            ));
            break;
        }
        if source.len() > MAX_EVENT_BYTES {
            diagnostics.push(format!(
                "parse {context}: event exceeds {MAX_EVENT_BYTES} bytes"
            ));
            break;
        }
        let event = match serde_json::from_str::<Value>(source) {
            Ok(event) => event,
            Err(error) => {
                diagnostics.push(format!("parse {context}: {error}"));
                break;
            }
        };
        if event["check_id"].as_str().is_none() {
            diagnostics.push(format!("parse {context}: event omitted check_id"));
            break;
        }
        events.push(event);
    }
    (events, diagnostics)
}

#[cfg(test)]
#[path = "events/tests.rs"]
mod tests;
