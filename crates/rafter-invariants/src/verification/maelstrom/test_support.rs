//! Compatibility exports that preserve Maelstrom test identities during migration.

pub(crate) use super::lease::{
    finalize_lease_scan, history_completion_count, history_completion_count_with_limits,
    scan_markers, scan_markers_with_limits, ArtifactLeaseMarker, HistoryLimits,
    LeaseArtifactStatus, MarkerLimits,
};
pub(crate) use super::observation::{trial_floors_met, MARKERS};
pub(crate) use super::receipt::valid_counterexample_attribution;
pub(crate) use super::scenario::Scenario;
pub(crate) use super::status::{
    counterexample_statuses, expected_counterexample_invariants, has_harness_error,
    local_counterexample_agrees,
};
pub(crate) use crate::evidence::format::java::major as java_major;

pub(crate) fn empty_observations(trials: u64) -> std::collections::BTreeMap<String, u64> {
    super::observation::ObservationLedger::new(trials).into_values()
}
