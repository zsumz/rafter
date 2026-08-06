//! Maelstrom evidence-production facade and stable producer test mount.

mod binding;
mod evaluation;
mod result;
mod runner;
mod scenario;
mod tool;
mod trial;

#[cfg(test)]
pub(super) use binding::bind_counterexamples;
pub(super) use runner::run;

#[cfg(test)]
pub(super) use trial::{
    bind_lease_history, finish_lease_transcript, trial_process_timeout, validate_lease_transcript,
    LeaseMarker, LeaseTranscriptStatus, ScenarioMarkers,
};
#[cfg(test)]
pub(super) use trial::{
    capture_tree, cleanup_state_directory, discover_store, reset_state_directory,
};
#[cfg(test)]
pub(super) use trial::{probe_completion_count, MAX_LINE_BYTES};

pub(super) use super::{artifact, maelstrom_edn, process, source, ProducerContext};

#[cfg(test)]
#[path = "maelstrom/tests.rs"]
mod tests;
