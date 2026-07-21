//! Compatibility facade for domain-owned simulator event decoding.

pub(in crate::artifact_verify) use crate::verification::simulator::schedule::scan_machine_events;

#[cfg(test)]
use crate::verification::simulator::schedule::{MAX_EVENTS_PER_LOG, MAX_EVENT_BYTES};

#[cfg(test)]
#[path = "../../verification/simulator/schedule/tests/events.rs"]
mod tests;
