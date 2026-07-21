//! Bounded decoding of machine events from authenticated simulator logs.

use serde_json::Value;

const EVENT_PREFIX: &str = "RAFTER_EVENT ";

#[cfg(test)]
pub(crate) const MAX_EVENTS_PER_LOG: usize = 4_096;
#[cfg(not(test))]
const MAX_EVENTS_PER_LOG: usize = 4_096;

#[cfg(test)]
pub(crate) const MAX_EVENT_BYTES: usize = 64 * 1024;
#[cfg(not(test))]
const MAX_EVENT_BYTES: usize = 64 * 1024;

pub(crate) struct ScannedSimulatorLog<'a> {
    pub(crate) source: &'a str,
    pub(crate) events: Vec<Value>,
}

pub(crate) fn scan_machine_events(log: &str, context: &str) -> (Vec<Value>, Vec<String>) {
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
