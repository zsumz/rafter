//! Stable producer Maelstrom execution and lease-test identities.

#[cfg(test)]
pub(super) use super::maelstrom::{
    bind_lease_history, finish_lease_transcript, trial_process_timeout, validate_lease_transcript,
    LeaseMarker, LeaseTranscriptStatus, ScenarioMarkers,
};
#[cfg(test)]
pub(super) use super::maelstrom::{capture_tree, discover_store, reset_state_directory};

#[cfg(test)]
mod lease_history;

#[cfg(test)]
#[path = "maelstrom/lease_transcript_tests.rs"]
mod lease_transcript_tests;
