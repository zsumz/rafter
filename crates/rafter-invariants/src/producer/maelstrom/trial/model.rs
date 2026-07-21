//! Typed Maelstrom trial outcomes and lease transcript observations.

use crate::evidence::ArtifactRef;

use super::super::maelstrom_edn;

pub(in crate::producer::maelstrom) struct TrialOutcome {
    pub(in crate::producer::maelstrom) summary: Option<maelstrom_edn::MaelstromSummary>,
    pub(in crate::producer::maelstrom) error: Option<String>,
    pub(in crate::producer::maelstrom) process_succeeded: bool,
    pub(in crate::producer::maelstrom) process_timed_out: bool,
    pub(in crate::producer::maelstrom) markers: ScenarioMarkers,
    pub(in crate::producer::maelstrom) duration_ms: u64,
    pub(in crate::producer::maelstrom) peak_rss_kib: u64,
    pub(in crate::producer::maelstrom) artifacts: Vec<ArtifactRef>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::producer) enum LeaseTranscriptStatus {
    #[default]
    Missing,
    Complete,
    Incomplete,
    Violation,
    ViolationWithHarnessError,
    HarnessError,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::producer) struct ScenarioMarkers {
    pub(in crate::producer) membership_enter: u64,
    pub(in crate::producer) membership_leave: u64,
    pub(in crate::producer) membership_complete: u64,
    pub(in crate::producer) restarts: u64,
    pub(in crate::producer) post_restart_progress: u64,
    pub(in crate::producer) crashpoints: u64,
    pub(in crate::producer) post_crash_progress: u64,
    pub(in crate::producer) snapshots_compacted: u64,
    pub(in crate::producer) snapshots_applied: u64,
    pub(in crate::producer) post_restart_snapshots_applied: u64,
    pub(in crate::producer) lease_fast_path_read_ok: u64,
    pub(in crate::producer) lease_read_buffered: u64,
    pub(in crate::producer) lease_expired_while_leader: u64,
    pub(in crate::producer) lease_post_expiry_released: u64,
    pub(in crate::producer) lease_post_expiry_handler: u64,
    pub(in crate::producer) lease_post_expiry_unavailable: u64,
    pub(in crate::producer) lease_post_expiry_read_served: u64,
    pub(in crate::producer) lease_post_expiry_renewed: u64,
    pub(in crate::producer) lease_post_expiry_unexpected_error: u64,
    pub(in crate::producer) lease_duplicate_terminal: u64,
    pub(in crate::producer) lease_coverage_lost: u64,
    pub(in crate::producer) lease_history_probe_matches: u64,
    pub(in crate::producer) lease_history_probe_mismatches: u64,
    pub(in crate::producer) lease_sequence_complete: u64,
    pub(in crate::producer) lease_sequence_invalid: u64,
    pub(in crate::producer) lease_status: LeaseTranscriptStatus,
}
