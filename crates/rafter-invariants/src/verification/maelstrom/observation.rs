//! Exact Maelstrom observation vocabulary and coverage derivation.

use std::collections::BTreeMap;

use crate::evidence::format::maelstrom::{MaelstromSummary, Validity};

use super::scenario::Scenario;

pub(super) const OBSERVATIONS: [&str; 33] = [
    "trials",
    "valid_trials",
    "invalid_trials",
    "operation_count",
    "ok_count",
    "read_ok",
    "write_ok",
    "cas_ok",
    "membership_enter",
    "membership_leave",
    "membership_complete",
    "restarts",
    "post_restart_progress",
    "crashpoints",
    "post_crash_progress",
    "snapshots_compacted",
    "snapshots_applied",
    "post_restart_snapshots_applied",
    "lease_fast_path_read_ok",
    "lease_read_buffered",
    "lease_expired_while_leader",
    "lease_post_expiry_released",
    "lease_post_expiry_handler",
    "lease_post_expiry_unavailable",
    "lease_post_expiry_read_served",
    "lease_post_expiry_renewed",
    "lease_post_expiry_unexpected_error",
    "lease_duplicate_terminal",
    "lease_coverage_lost",
    "lease_history_probe_matches",
    "lease_history_probe_mismatches",
    "lease_sequence_complete",
    "lease_sequence_invalid",
];

pub(crate) const MARKERS: [&str; 25] = [
    "membership_enter",
    "membership_leave",
    "membership_complete",
    "restarts",
    "post_restart_progress",
    "crashpoints",
    "post_crash_progress",
    "snapshots_compacted",
    "snapshots_applied",
    "post_restart_snapshots_applied",
    "lease_fast_path_read_ok",
    "lease_read_buffered",
    "lease_expired_while_leader",
    "lease_post_expiry_released",
    "lease_post_expiry_handler",
    "lease_post_expiry_unavailable",
    "lease_post_expiry_read_served",
    "lease_post_expiry_renewed",
    "lease_post_expiry_unexpected_error",
    "lease_duplicate_terminal",
    "lease_coverage_lost",
    "lease_history_probe_matches",
    "lease_history_probe_mismatches",
    "lease_sequence_complete",
    "lease_sequence_invalid",
];

pub(super) struct ObservationLedger {
    values: BTreeMap<String, u64>,
}

impl ObservationLedger {
    pub(super) fn new(trials: u64) -> Self {
        let mut values = BTreeMap::from([
            ("trials".to_owned(), trials),
            ("valid_trials".to_owned(), 0),
            ("invalid_trials".to_owned(), 0),
            ("operation_count".to_owned(), 0),
            ("ok_count".to_owned(), 0),
            ("read_ok".to_owned(), 0),
            ("write_ok".to_owned(), 0),
            ("cas_ok".to_owned(), 0),
        ]);
        values.extend(MARKERS.into_iter().map(|name| (name.to_owned(), 0)));
        Self { values }
    }

    pub(super) fn add_summary(&mut self, summary: &MaelstromSummary) {
        self.add(
            "valid_trials",
            u64::from(summary.validity == Validity::Valid),
        );
        self.add(
            "invalid_trials",
            u64::from(summary.linearizability == Validity::Invalid),
        );
        self.add("operation_count", summary.operation_count);
        self.add("ok_count", summary.ok_count);
        self.add("read_ok", summary.read_ok);
        self.add("write_ok", summary.write_ok);
        self.add("cas_ok", summary.cas_ok);
    }

    pub(super) fn add_markers(&mut self, markers: &BTreeMap<&str, u64>) {
        for (&name, &value) in markers {
            self.add(name, value);
        }
    }

    pub(super) fn into_values(self) -> BTreeMap<String, u64> {
        self.values
    }

    fn add(&mut self, name: &str, value: u64) {
        *self.values.entry(name.to_owned()).or_default() += value;
    }
}

pub(crate) fn trial_floors_met(
    scenario: Scenario,
    summary: &MaelstromSummary,
    markers: &BTreeMap<&str, u64>,
    durable: bool,
) -> bool {
    let operations = summary.read_ok > 0 && summary.write_ok > 0 && summary.cas_ok > 0;
    let covered = match scenario {
        Scenario::Base => true,
        Scenario::Membership => {
            markers["membership_enter"] > 0
                && markers["membership_leave"] > 0
                && markers["membership_complete"] > 0
        }
        Scenario::Restart => markers["restarts"] >= 3 && markers["post_restart_progress"] > 0,
        Scenario::ApplicationCrash => {
            markers["crashpoints"] > 0 && markers["post_crash_progress"] > 0
        }
        Scenario::Snapshot => {
            markers["restarts"] > 0
                && markers["snapshots_compacted"] > 0
                && markers["snapshots_applied"] > 0
                && markers["post_restart_snapshots_applied"] > 0
        }
        Scenario::LeaseIsolation => lease_floors_met(markers),
    };
    operations && covered && (!scenario.requires_durable_state() || durable)
}

fn lease_floors_met(markers: &BTreeMap<&str, u64>) -> bool {
    markers["lease_sequence_complete"] == 1
        && markers["lease_sequence_invalid"] == 0
        && markers["lease_fast_path_read_ok"] == 1
        && markers["lease_read_buffered"] == 1
        && markers["lease_expired_while_leader"] == 1
        && markers["lease_post_expiry_released"] == 1
        && markers["lease_post_expiry_handler"] == 1
        && markers["lease_post_expiry_unavailable"] == 1
        && markers["lease_post_expiry_read_served"] == 0
        && markers["lease_post_expiry_renewed"] == 0
        && markers["lease_post_expiry_unexpected_error"] == 0
        && markers["lease_duplicate_terminal"] == 0
        && markers["lease_coverage_lost"] == 0
        && markers["lease_history_probe_matches"] == 1
        && markers["lease_history_probe_mismatches"] == 0
}
