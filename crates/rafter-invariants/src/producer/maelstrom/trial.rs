//! Bounded Maelstrom trial execution and retained artifact collection facade.

mod artifacts;
mod lease;
mod model;
mod runner;

#[cfg(test)]
pub(in crate::producer) use artifacts::{capture_tree, discover_store, reset_state_directory};
#[cfg(test)]
pub(in crate::producer) use lease::{
    bind_history_for_test as bind_lease_history,
    finish_transcript_for_test as finish_lease_transcript, probe_completion_count,
    validate_transcript_for_test as validate_lease_transcript, TestLeaseMarker as LeaseMarker,
    MAX_LINE_BYTES,
};
pub(in crate::producer) use model::{LeaseTranscriptStatus, ScenarioMarkers};
#[cfg(test)]
pub(in crate::producer) use runner::trial_process_timeout;

pub(in crate::producer) use super::scenario::Scenario;
pub(super) use model::TrialOutcome;
pub(super) use runner::run_trial;
