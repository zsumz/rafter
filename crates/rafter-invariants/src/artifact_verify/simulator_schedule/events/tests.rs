//! Adversarial resource-limit scenarios for simulator event parsing.

use super::{scan_machine_events, MAX_EVENTS_PER_LOG, MAX_EVENT_BYTES};
use crate::artifact_verify::EVENT_PREFIX;

#[test]
fn oversized_event_is_rejected_before_json_allocation() {
    let log = format!(
        "{EVENT_PREFIX}{{\"check_id\":\"{}\"}}",
        "x".repeat(MAX_EVENT_BYTES)
    );
    let (events, diagnostics) = scan_machine_events(&log, "fixture");
    assert!(events.is_empty());
    assert_eq!(diagnostics.len(), 1);
    assert!(diagnostics[0].contains("event exceeds"));
}

#[test]
fn event_inventory_is_bounded() {
    let event = format!("{EVENT_PREFIX}{{\"check_id\":\"fixture\"}}");
    let log = std::iter::repeat_n(event, MAX_EVENTS_PER_LOG + 1)
        .collect::<Vec<_>>()
        .join("\n");
    let (events, diagnostics) = scan_machine_events(&log, "fixture");
    assert_eq!(events.len(), MAX_EVENTS_PER_LOG);
    assert_eq!(diagnostics.len(), 1);
    assert!(diagnostics[0].contains("event count exceeds"));
}
