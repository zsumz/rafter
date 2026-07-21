//! Test-identity facade for verification-owned Maelstrom lease acceptance.

#[cfg(test)]
pub(super) use crate::verification::maelstrom::test_support::{
    empty_observations, finalize_lease_scan, history_completion_count,
    history_completion_count_with_limits, scan_markers, scan_markers_with_limits, trial_floors_met,
    ArtifactLeaseMarker, HistoryLimits, LeaseArtifactStatus, MarkerLimits, Scenario, MARKERS,
};

#[cfg(test)]
#[path = "verification/maelstrom/tests/lease.rs"]
mod tests;
