//! Stable Maelstrom producer reduction scenarios.

use crate::producer::maelstrom_edn::{MaelstromSummary, Validity};

use super::{
    evaluation::{evaluate, ScenarioVerdict},
    trial::{LeaseTranscriptStatus, Scenario, ScenarioMarkers, TrialOutcome},
};

#[test]
fn producer_preserves_simultaneous_rd05_and_rd06_counterexamples() {
    let outcome = TrialOutcome {
        summary: Some(MaelstromSummary {
            validity: Validity::Invalid,
            linearizability: Validity::Invalid,
            operation_count: 3,
            ok_count: 3,
            read_ok: 1,
            write_ok: 1,
            cas_ok: 1,
        }),
        error: None,
        process_succeeded: true,
        process_timed_out: false,
        markers: ScenarioMarkers {
            lease_status: LeaseTranscriptStatus::Violation,
            ..ScenarioMarkers::default()
        },
        duration_ms: 1,
        peak_rss_kib: 1,
        artifacts: Vec::new(),
    };
    assert!(matches!(
        evaluate(Scenario::LeaseIsolation, &[outcome]),
        ScenarioVerdict::Counterexample {
            rd05: true,
            rd06: true,
            harness_error: false
        }
    ));
}

#[test]
fn rd05_violation_survives_later_parse_process_and_transcript_errors() {
    let outcome = TrialOutcome {
        summary: None,
        error: Some("malformed results.edn".to_owned()),
        process_succeeded: false,
        process_timed_out: false,
        markers: ScenarioMarkers {
            lease_status: LeaseTranscriptStatus::ViolationWithHarnessError,
            lease_post_expiry_read_served: 1,
            lease_sequence_invalid: 1,
            ..ScenarioMarkers::default()
        },
        duration_ms: 1,
        peak_rss_kib: 1,
        artifacts: Vec::new(),
    };
    assert!(matches!(
        evaluate(Scenario::LeaseIsolation, &[outcome]),
        ScenarioVerdict::Counterexample {
            rd05: true,
            rd06: false,
            harness_error: true
        }
    ));
}

#[test]
fn trial_timeout_is_a_harness_error_even_with_a_valid_checker_summary() {
    let outcome = TrialOutcome {
        summary: Some(MaelstromSummary {
            validity: Validity::Valid,
            linearizability: Validity::Valid,
            operation_count: 3,
            ok_count: 3,
            read_ok: 1,
            write_ok: 1,
            cas_ok: 1,
        }),
        error: None,
        process_succeeded: false,
        process_timed_out: true,
        markers: ScenarioMarkers::default(),
        duration_ms: 1,
        peak_rss_kib: 1,
        artifacts: Vec::new(),
    };
    assert!(matches!(
        evaluate(Scenario::Base, &[outcome]),
        ScenarioVerdict::Error(message)
            if message == "Maelstrom process exceeded its trial timeout"
    ));
}
