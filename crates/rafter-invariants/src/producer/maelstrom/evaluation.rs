//! Scenario completion, counterexample precedence, and observation reduction.

use std::collections::BTreeMap;

use crate::evidence::CheckCompletion;

use super::{
    maelstrom_edn::Validity,
    trial::{LeaseTranscriptStatus, Scenario, ScenarioMarkers, TrialOutcome},
};

pub(super) enum ScenarioVerdict {
    Pass,
    Counterexample {
        rd05: bool,
        rd06: bool,
        harness_error: bool,
    },
    Incomplete(String),
    Error(String),
}

impl ScenarioVerdict {
    pub(super) const fn completion(&self) -> CheckCompletion {
        match self {
            Self::Pass => CheckCompletion::Completed,
            Self::Counterexample { .. } => CheckCompletion::Counterexample,
            Self::Incomplete(_) => CheckCompletion::CoverageNotReached,
            Self::Error(_) => CheckCompletion::HarnessError,
        }
    }

    pub(super) fn targets(&self, invariant_id: &str) -> bool {
        match self {
            Self::Counterexample { rd05, .. } if invariant_id == "RD-05" => *rd05,
            Self::Counterexample { rd06, .. } if invariant_id == "RD-06" => *rd06,
            _ => false,
        }
    }
}

pub(super) fn evaluate(scenario: Scenario, outcomes: &[TrialOutcome]) -> ScenarioVerdict {
    let rd06 = outcomes.iter().any(|outcome| {
        outcome
            .summary
            .as_ref()
            .is_some_and(|summary| summary.linearizability == Validity::Invalid)
    });
    let rd05 = scenario == Scenario::LeaseIsolation
        && outcomes.iter().any(|outcome| {
            matches!(
                outcome.markers.lease_status,
                LeaseTranscriptStatus::Violation | LeaseTranscriptStatus::ViolationWithHarnessError
            )
        });
    let harness_error = outcomes.iter().any(|outcome| {
        outcome.error.is_some()
            || !outcome.process_succeeded
            || matches!(
                outcome.markers.lease_status,
                LeaseTranscriptStatus::HarnessError
                    | LeaseTranscriptStatus::ViolationWithHarnessError
            )
    });
    if rd05 || rd06 {
        return ScenarioVerdict::Counterexample {
            rd05,
            rd06,
            harness_error,
        };
    }
    if outcomes.iter().any(|outcome| outcome.process_timed_out) {
        return ScenarioVerdict::Error("Maelstrom process exceeded its trial timeout".to_owned());
    }
    if let Some(error) = outcomes.iter().find_map(|outcome| outcome.error.as_ref()) {
        return ScenarioVerdict::Error(error.clone());
    }
    if outcomes.iter().any(|outcome| !outcome.process_succeeded) {
        return ScenarioVerdict::Error("Maelstrom process did not exit successfully".to_owned());
    }
    if outcomes.iter().any(|outcome| {
        outcome
            .summary
            .as_ref()
            .is_none_or(|summary| summary.validity != Validity::Valid)
    }) {
        return ScenarioVerdict::Incomplete(
            "Maelstrom did not produce a completed valid checker result".to_owned(),
        );
    }
    if scenario == Scenario::LeaseIsolation {
        if outcomes
            .iter()
            .any(|outcome| outcome.markers.lease_status == LeaseTranscriptStatus::HarnessError)
        {
            return ScenarioVerdict::Error(
                "lease-isolation transcript was malformed or returned an unexpected error"
                    .to_owned(),
            );
        }
        if outcomes
            .iter()
            .any(|outcome| outcome.markers.lease_status != LeaseTranscriptStatus::Complete)
        {
            return ScenarioVerdict::Incomplete(
                "lease-isolation did not complete one ordered real-client transcript".to_owned(),
            );
        }
    }
    if outcomes.iter().any(|outcome| {
        outcome.summary.as_ref().is_none_or(|summary| {
            summary.read_ok == 0 || summary.write_ok == 0 || summary.cas_ok == 0
        }) || !markers_cover(scenario, outcome.markers)
    }) {
        return ScenarioVerdict::Incomplete(format!(
            "Maelstrom scenario {} did not reach its operation or fault marker floor",
            scenario.name()
        ));
    }
    ScenarioVerdict::Pass
}

fn markers_cover(scenario: Scenario, markers: ScenarioMarkers) -> bool {
    match scenario {
        Scenario::Base => true,
        Scenario::Membership => {
            markers.membership_enter > 0
                && markers.membership_leave > 0
                && markers.membership_complete > 0
        }
        Scenario::Restart => markers.restarts >= 3 && markers.post_restart_progress > 0,
        Scenario::AppCrash => markers.crashpoints > 0 && markers.post_crash_progress > 0,
        Scenario::Snapshot => {
            markers.restarts > 0
                && markers.snapshots_compacted > 0
                && markers.snapshots_applied > 0
                && markers.post_restart_snapshots_applied > 0
        }
        Scenario::LeaseIsolation => {
            markers.lease_sequence_complete == 1
                && markers.lease_sequence_invalid == 0
                && markers.lease_fast_path_read_ok == 1
                && markers.lease_read_buffered == 1
                && markers.lease_expired_while_leader == 1
                && markers.lease_post_expiry_released == 1
                && markers.lease_post_expiry_handler == 1
                && markers.lease_post_expiry_unavailable == 1
                && markers.lease_post_expiry_read_served == 0
                && markers.lease_post_expiry_renewed == 0
                && markers.lease_post_expiry_unexpected_error == 0
                && markers.lease_duplicate_terminal == 0
                && markers.lease_coverage_lost == 0
                && markers.lease_history_probe_matches == 1
                && markers.lease_history_probe_mismatches == 0
        }
    }
}

pub(super) fn observations(outcomes: &[TrialOutcome]) -> BTreeMap<String, u64> {
    let mut values = BTreeMap::from([
        ("trials".to_owned(), outcomes.len() as u64),
        ("valid_trials".to_owned(), 0),
        ("invalid_trials".to_owned(), 0),
        ("operation_count".to_owned(), 0),
        ("ok_count".to_owned(), 0),
        ("read_ok".to_owned(), 0),
        ("write_ok".to_owned(), 0),
        ("cas_ok".to_owned(), 0),
        ("membership_enter".to_owned(), 0),
        ("membership_leave".to_owned(), 0),
        ("membership_complete".to_owned(), 0),
        ("restarts".to_owned(), 0),
        ("post_restart_progress".to_owned(), 0),
        ("crashpoints".to_owned(), 0),
        ("post_crash_progress".to_owned(), 0),
        ("snapshots_compacted".to_owned(), 0),
        ("snapshots_applied".to_owned(), 0),
        ("post_restart_snapshots_applied".to_owned(), 0),
        ("lease_fast_path_read_ok".to_owned(), 0),
        ("lease_read_buffered".to_owned(), 0),
        ("lease_expired_while_leader".to_owned(), 0),
        ("lease_post_expiry_released".to_owned(), 0),
        ("lease_post_expiry_handler".to_owned(), 0),
        ("lease_post_expiry_unavailable".to_owned(), 0),
        ("lease_post_expiry_read_served".to_owned(), 0),
        ("lease_post_expiry_renewed".to_owned(), 0),
        ("lease_post_expiry_unexpected_error".to_owned(), 0),
        ("lease_duplicate_terminal".to_owned(), 0),
        ("lease_coverage_lost".to_owned(), 0),
        ("lease_history_probe_matches".to_owned(), 0),
        ("lease_history_probe_mismatches".to_owned(), 0),
        ("lease_sequence_complete".to_owned(), 0),
        ("lease_sequence_invalid".to_owned(), 0),
    ]);
    for outcome in outcomes {
        if let Some(summary) = &outcome.summary {
            add(
                &mut values,
                "valid_trials",
                u64::from(summary.validity == Validity::Valid),
            );
            add(
                &mut values,
                "invalid_trials",
                u64::from(summary.linearizability == Validity::Invalid),
            );
            add(&mut values, "operation_count", summary.operation_count);
            add(&mut values, "ok_count", summary.ok_count);
            add(&mut values, "read_ok", summary.read_ok);
            add(&mut values, "write_ok", summary.write_ok);
            add(&mut values, "cas_ok", summary.cas_ok);
        }
        for (name, value) in marker_values(outcome.markers) {
            add(&mut values, name, value);
        }
    }
    values
}

fn add(values: &mut BTreeMap<String, u64>, name: &str, value: u64) {
    *values.entry(name.to_owned()).or_default() += value;
}

fn marker_values(markers: ScenarioMarkers) -> [(&'static str, u64); 25] {
    [
        ("membership_enter", markers.membership_enter),
        ("membership_leave", markers.membership_leave),
        ("membership_complete", markers.membership_complete),
        ("restarts", markers.restarts),
        ("post_restart_progress", markers.post_restart_progress),
        ("crashpoints", markers.crashpoints),
        ("post_crash_progress", markers.post_crash_progress),
        ("snapshots_compacted", markers.snapshots_compacted),
        ("snapshots_applied", markers.snapshots_applied),
        (
            "post_restart_snapshots_applied",
            markers.post_restart_snapshots_applied,
        ),
        ("lease_fast_path_read_ok", markers.lease_fast_path_read_ok),
        ("lease_read_buffered", markers.lease_read_buffered),
        (
            "lease_expired_while_leader",
            markers.lease_expired_while_leader,
        ),
        (
            "lease_post_expiry_released",
            markers.lease_post_expiry_released,
        ),
        (
            "lease_post_expiry_handler",
            markers.lease_post_expiry_handler,
        ),
        (
            "lease_post_expiry_unavailable",
            markers.lease_post_expiry_unavailable,
        ),
        (
            "lease_post_expiry_read_served",
            markers.lease_post_expiry_read_served,
        ),
        (
            "lease_post_expiry_renewed",
            markers.lease_post_expiry_renewed,
        ),
        (
            "lease_post_expiry_unexpected_error",
            markers.lease_post_expiry_unexpected_error,
        ),
        ("lease_duplicate_terminal", markers.lease_duplicate_terminal),
        ("lease_coverage_lost", markers.lease_coverage_lost),
        (
            "lease_history_probe_matches",
            markers.lease_history_probe_matches,
        ),
        (
            "lease_history_probe_mismatches",
            markers.lease_history_probe_mismatches,
        ),
        ("lease_sequence_complete", markers.lease_sequence_complete),
        ("lease_sequence_invalid", markers.lease_sequence_invalid),
    ]
}
