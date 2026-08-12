//! TLA+ producer completion, attribution, and progress scenarios.

use std::{collections::BTreeMap, path::PathBuf};

use super::{
    evaluate, evidence_result, observations, MainStatus, ProbeStatus, TlaExecution, TlaVerdict,
};
use crate::{
    producer::tla_exec::parse_main_summary,
    producer::tla_output::{TlcProgress, TlcSummary, REQUIRED_MODEL_TRANSITIONS},
    Catalog, CheckCompletion, EvidenceStatus, FailureClassification, PrimaryCompletionPolicy,
};

pub(super) fn complete_execution(exit_succeeded: bool) -> TlaExecution {
    TlaExecution {
        obligations: super::ObligationOutcome::default(),
        main: Some(TlcSummary {
            generated_states: 130_000_001,
            distinct_states: 16_284_977,
            states_left: 0,
            search_depth: 19,
            completed_without_error: true,
            process_finished: true,
            violated_invariant: None,
        }),
        main_progress: None,
        main_parse_error: None,
        main_status: if exit_succeeded {
            MainStatus::Succeeded
        } else {
            MainStatus::Failed
        },
        trace_status: ProbeStatus::Passed,
        detector_status: ProbeStatus::Passed,
        detector_qualifications: crate::producer::tla_output::REGISTERED_PREDICATES
            .into_iter()
            .map(|predicate| (format!("detector_qualified:{predicate}"), 1))
            .collect(),
        peak_rss_kib: 1,
        duration_ms: 1,
        artifacts: Vec::new(),
        checkpoint_report: None,
        checkpoint_error: None,
    }
}

#[test]
fn successful_frames_with_nonzero_exit_are_a_harness_error() {
    let execution = complete_execution(false);
    let symbols = ["ElectionSafety".to_owned()].into_iter().collect();
    let configuration = BTreeMap::from([
        (
            "minimum_generated_states".to_owned(),
            "120000000".to_owned(),
        ),
        ("minimum_distinct_states".to_owned(), "16000000".to_owned()),
        (
            "primary_completion".to_owned(),
            "gating-frontier-exhausted".to_owned(),
        ),
    ]);

    assert!(matches!(
        evaluate(&execution, &symbols, &configuration),
        TlaVerdict::Error(_)
    ));
}

#[test]
fn coverage_floor_uses_generated_and_distinct_state_counters() {
    let execution = complete_execution(true);
    let symbols = ["ElectionSafety".to_owned()].into_iter().collect();
    let passing = BTreeMap::from([
        (
            "minimum_generated_states".to_owned(),
            "120000000".to_owned(),
        ),
        ("minimum_distinct_states".to_owned(), "16000000".to_owned()),
        (
            "primary_completion".to_owned(),
            "gating-frontier-exhausted".to_owned(),
        ),
    ]);
    assert!(matches!(
        evaluate(&execution, &symbols, &passing),
        TlaVerdict::Pass
    ));

    let mut too_high = passing;
    too_high.insert(
        "minimum_generated_states".to_owned(),
        "140000000".to_owned(),
    );
    assert!(matches!(
        evaluate(&execution, &symbols, &too_high),
        TlaVerdict::Incomplete(_, _)
    ));
}

#[test]
fn observations_report_each_detector_qualification_independently() {
    let execution = complete_execution(true);
    let symbols = crate::producer::tla_output::REGISTERED_PREDICATES
        .into_iter()
        .map(str::to_owned)
        .collect();
    let observed = observations(
        &execution,
        &symbols,
        9,
        PrimaryCompletionPolicy::GatingFrontierExhausted,
    );
    assert!(!observed.contains_key("detector_negative_passed"));
    for predicate in crate::producer::tla_output::REGISTERED_PREDICATES {
        assert_eq!(observed[&format!("detector_qualified:{predicate}")], 1);
    }
    for transition in REQUIRED_MODEL_TRANSITIONS {
        assert_eq!(observed[&format!("transition_covered:{transition}")], 1);
    }
}

#[test]
fn named_counterexample_fails_only_its_predicate() -> Result<(), Box<dyn std::error::Error>> {
    let registry = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("verification/raft-invariants.yaml");
    let catalog = Catalog::load(&registry)?;
    let results = catalog
        .evidence
        .iter()
        .filter(|descriptor| descriptor.layer == "tla")
        .map(|descriptor| {
            evidence_result(
                descriptor,
                "tla-test",
                &TlaVerdict::Violation("ElectionSafety".to_owned()),
                &[],
            )
        })
        .collect::<Vec<_>>();

    assert_eq!(
        results.len(),
        catalog
            .evidence
            .iter()
            .filter(|descriptor| descriptor.layer == "tla")
            .count()
    );
    assert_eq!(
        results
            .iter()
            .filter(|result| result.status == EvidenceStatus::Fail)
            .count(),
        1
    );
    let failed = results
        .iter()
        .filter(|result| result.status == EvidenceStatus::Fail)
        .collect::<Vec<_>>();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].invariant_id, "EL-05");
    assert_eq!(
        failed[0].classification,
        Some(FailureClassification::InvariantViolation)
    );
    assert!(results
        .iter()
        .filter(|result| result.status != EvidenceStatus::Fail)
        .all(|result| {
            result.status == EvidenceStatus::Incomplete
                && result.classification == Some(FailureClassification::CoverageNotReached)
        }));
    Ok(())
}

#[test]
fn multi_clause_counterexample_fails_every_bound_evidence_row(
) -> Result<(), Box<dyn std::error::Error>> {
    let registry = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join("verification/raft-invariants.yaml");
    let catalog = Catalog::load(&registry)?;
    let results = catalog
        .evidence
        .iter()
        .filter(|descriptor| descriptor.layer == "tla")
        .map(|descriptor| {
            evidence_result(
                descriptor,
                "tla-test",
                &TlaVerdict::Violation("CommittedEntriesHaveQuorum".to_owned()),
                &[],
            )
        })
        .collect::<Vec<_>>();
    let failed = results
        .iter()
        .filter(|result| result.status == EvidenceStatus::Fail)
        .collect::<Vec<_>>();

    assert_eq!(failed.len(), 2);
    assert!(failed.iter().all(|result| result.invariant_id == "CM-02"));
    assert!(failed.iter().all(|result| {
        result.classification == Some(FailureClassification::InvariantViolation)
    }));
    assert!(results
        .iter()
        .filter(|result| result.status != EvidenceStatus::Fail)
        .all(|result| result.status == EvidenceStatus::Incomplete));
    Ok(())
}

#[test]
fn parsed_named_counterexample_outranks_concurrent_timeout() {
    let mut execution = complete_execution(false);
    execution.main_status = MainStatus::TimedOut;
    execution.main.as_mut().expect("summary").violated_invariant =
        Some("ElectionSafety".to_owned());
    let symbols = ["ElectionSafety".to_owned()].into_iter().collect();
    assert!(matches!(
        evaluate(&execution, &symbols, &BTreeMap::new()),
        TlaVerdict::Violation(symbol) if symbol == "ElectionSafety"
    ));
}

#[test]
fn named_counterexample_outranks_a_later_malformed_terminal_frame() {
    let output = b"@!@!@STARTMSG 2110:1 @!@!@\nInvariant ElectionSafety is violated.\n@!@!@ENDMSG 2110 @!@!@\n\
                   @!@!@STARTMSG 2199:0 @!@!@\nmalformed statistics\n@!@!@ENDMSG 2199 @!@!@\n";
    let (summary, parse_error) = parse_main_summary(output);
    assert_eq!(
        parse_error.as_deref(),
        Some("TLC 2199 frame has malformed state statistics")
    );

    let mut execution = complete_execution(false);
    execution.main = summary;
    execution.main_parse_error = parse_error;
    let symbols = ["ElectionSafety".to_owned()].into_iter().collect();
    assert!(matches!(
        evaluate(&execution, &symbols, &BTreeMap::new()),
        TlaVerdict::Violation(symbol) if symbol == "ElectionSafety"
    ));
}

#[test]
fn parsed_named_counterexample_outranks_checkpoint_finalization_failure() {
    let mut execution = complete_execution(false);
    execution.main.as_mut().expect("summary").violated_invariant =
        Some("ElectionSafety".to_owned());
    execution.checkpoint_error = Some("checkpoint deadline expired".to_owned());
    let symbols = ["ElectionSafety".to_owned()].into_iter().collect();

    assert!(matches!(
        evaluate(&execution, &symbols, &BTreeMap::new()),
        TlaVerdict::Violation(symbol) if symbol == "ElectionSafety"
    ));
}

#[test]
fn timeout_reports_progress_without_claiming_terminal_proof() {
    let mut execution = complete_execution(false);
    execution.main_status = MainStatus::TimedOut;
    execution.main = Some(TlcSummary::default());
    execution.main_progress = Some(TlcProgress {
        generated_states: 181_490_601,
        distinct_states: 40_062_465,
        states_left: 19_012_042,
        depth: 23,
    });
    let symbols = ["ElectionSafety".to_owned()].into_iter().collect();
    let verdict = evaluate(
        &execution,
        &symbols,
        &super::policy_tests::gating_configuration(),
    );
    assert!(matches!(
        verdict,
        TlaVerdict::Incomplete(CheckCompletion::Timeout, _)
    ));

    let observed = observations(
        &execution,
        &symbols,
        9,
        PrimaryCompletionPolicy::GatingFrontierExhausted,
    );
    assert_eq!(observed["progress_generated_states"], 181_490_601);
    assert_eq!(observed["progress_distinct_states"], 40_062_465);
    assert_eq!(observed["progress_states_left"], 19_012_042);
    assert_eq!(observed["progress_depth"], 23);
    for terminal in [
        "generated_states",
        "distinct_states",
        "states_left_on_queue",
        "search_depth",
        "checked:ElectionSafety",
    ] {
        assert!(!observed.contains_key(terminal));
    }
}

/// An undischarged obligation is red before the primary configuration's own
/// verdict is even consulted: the main run never happened, so a clean main
/// summary in the same frame must not be able to rescue the layer.
#[test]
fn an_undischarged_obligation_fails_the_layer_before_the_primary_verdict() {
    let symbols = ["ElectionSafety".to_owned()].into_iter().collect();
    let configuration = BTreeMap::from([
        (
            "minimum_generated_states".to_owned(),
            "120000000".to_owned(),
        ),
        ("minimum_distinct_states".to_owned(), "16000000".to_owned()),
        (
            "primary_completion".to_owned(),
            "gating-frontier-exhausted".to_owned(),
        ),
    ]);

    let mut execution = complete_execution(true);
    assert!(matches!(
        evaluate(&execution, &symbols, &configuration),
        TlaVerdict::Pass
    ));

    execution.obligations.status = ProbeStatus::Failed;
    execution.obligations.failure = Some("proof obligation focused refuted LogMatching".to_owned());
    let TlaVerdict::Error(message) = evaluate(&execution, &symbols, &configuration) else {
        panic!("an undischarged obligation must fail the layer");
    };
    assert!(message.contains("refuted LogMatching"), "{message}");
}

/// Observations from executed obligations reach the receipt frame verbatim, so
/// the verifier can rederive them from the same logs.
#[test]
fn obligation_observations_are_framed_in_the_receipt() {
    let mut execution = complete_execution(true);
    let symbols: std::collections::BTreeSet<String> =
        ["ElectionSafety".to_owned()].into_iter().collect();
    assert!(!observations(
        &execution,
        &symbols,
        9,
        PrimaryCompletionPolicy::GatingFrontierExhausted
    )
    .keys()
    .any(|key| key.starts_with("obligation_")));

    execution.obligations.status = ProbeStatus::Passed;
    execution.obligations.observations = BTreeMap::from([
        ("obligation_generated_states:focused".to_owned(), 4_242),
        ("obligation_frontier_exhausted:focused".to_owned(), 1),
    ]);
    let framed = observations(
        &execution,
        &symbols,
        9,
        PrimaryCompletionPolicy::GatingFrontierExhausted,
    );
    assert_eq!(framed["obligation_generated_states:focused"], 4_242);
    assert_eq!(framed["obligation_frontier_exhausted:focused"], 1);
}
