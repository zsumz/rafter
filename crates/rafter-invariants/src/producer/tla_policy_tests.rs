//! Scenarios: the primary-continuation policy decides only what it may decide.

use std::collections::BTreeMap;

use super::{evaluate, observations, MainStatus, ProbeStatus, TlaVerdict};
use crate::{
    producer::tla_output::{TlcProgress, TlcSummary},
    CheckCompletion, PrimaryCompletionPolicy,
};

use super::tests::complete_execution;

fn reporting_configuration() -> BTreeMap<String, String> {
    BTreeMap::from([
        (
            "minimum_generated_states".to_owned(),
            "120000000".to_owned(),
        ),
        ("minimum_distinct_states".to_owned(), "16000000".to_owned()),
        (
            "primary_completion".to_owned(),
            "reporting-continuation".to_owned(),
        ),
    ])
}

pub(super) fn gating_configuration() -> BTreeMap<String, String> {
    let mut configuration = reporting_configuration();
    configuration.insert(
        "primary_completion".to_owned(),
        "gating-frontier-exhausted".to_owned(),
    );
    configuration
}

/// The whole point of the demotion: the same elapsed continuation is incomplete
/// under a gating policy and a pass under a reporting one, where the
/// obligations carried the gate.
#[test]
fn an_elapsed_continuation_gates_only_under_a_gating_policy() {
    let symbols = ["ElectionSafety".to_owned()].into_iter().collect();
    let mut execution = complete_execution(true);
    execution.main_status = MainStatus::TimedOut;
    execution.main = None;
    execution.main_progress = Some(TlcProgress {
        generated_states: 23_784_130,
        distinct_states: 6_246_309,
        states_left: 3_294_097,
        depth: 21,
    });

    assert!(matches!(
        evaluate(&execution, &symbols, &gating_configuration()),
        TlaVerdict::Incomplete(CheckCompletion::Timeout, _)
    ));
    let reporting = evaluate(&execution, &symbols, &reporting_configuration());
    assert!(matches!(reporting, TlaVerdict::PassBudgetElapsed));
    // It passes, and it records what actually happened. Those are separate
    // claims: the obligations carried the gate, and this run left 3,294,097
    // states on its queue. The receipt says both.
    assert_eq!(
        reporting.completion(),
        CheckCompletion::BudgetElapsedFrontierOpen
    );
}

/// Reporting relaxes the budget, never safety or evidence integrity.
#[test]
fn a_reporting_continuation_still_fails_on_violations_and_malformed_progress() {
    let symbols = ["ElectionSafety".to_owned()].into_iter().collect();

    let mut violated = complete_execution(true);
    violated.main = Some(TlcSummary {
        violated_invariant: Some("ElectionSafety".to_owned()),
        ..violated.main.expect("summary")
    });
    assert!(matches!(
        evaluate(&violated, &symbols, &reporting_configuration()),
        TlaVerdict::Violation(_)
    ));

    let mut unreadable = complete_execution(true);
    unreadable.main_status = MainStatus::TimedOut;
    unreadable.main = None;
    unreadable.main_progress = None;
    assert!(matches!(
        evaluate(&unreadable, &symbols, &reporting_configuration()),
        TlaVerdict::Error(_)
    ));

    let mut unpinned = complete_execution(true);
    unpinned.main_status = MainStatus::TimedOut;
    let mut configuration = reporting_configuration();
    configuration.remove("primary_completion");
    assert!(matches!(
        evaluate(&unpinned, &symbols, &configuration),
        TlaVerdict::Error(_)
    ));
}

/// A reporting continuation that falls short of the pinned floors is still a
/// pass: those numbers are the accumulation bar it reports against, not a
/// terminal condition it has to clear.
#[test]
fn reporting_floors_are_context_rather_than_a_gate() {
    let symbols = ["ElectionSafety".to_owned()].into_iter().collect();
    let mut execution = complete_execution(true);
    execution.main = Some(TlcSummary {
        generated_states: 17,
        distinct_states: 9,
        ..execution.main.expect("summary")
    });

    assert!(matches!(
        evaluate(&execution, &symbols, &gating_configuration()),
        TlaVerdict::Incomplete(CheckCompletion::CoverageNotReached, _)
    ));
    assert!(matches!(
        evaluate(&execution, &symbols, &reporting_configuration()),
        TlaVerdict::Pass
    ));
}

/// Under a reporting policy the `checked:` claim is sourced from the
/// obligations, which bind the same predicates and did drain.
#[test]
fn reporting_checked_predicates_come_from_the_obligations() {
    let symbols: std::collections::BTreeSet<String> =
        ["ElectionSafety".to_owned()].into_iter().collect();
    let mut execution = complete_execution(true);
    execution.main_status = MainStatus::TimedOut;
    execution.main = None;
    execution.main_progress = Some(TlcProgress {
        generated_states: 1,
        distinct_states: 1,
        states_left: 1,
        depth: 1,
    });
    execution.obligations.status = ProbeStatus::Passed;

    let framed = observations(
        &execution,
        &symbols,
        9,
        PrimaryCompletionPolicy::ReportingContinuation,
    );
    assert_eq!(framed["checked:ElectionSafety"], 1);
    assert_eq!(framed["progress_states_left"], 1);

    execution.obligations.status = ProbeStatus::NotRun;
    assert!(!observations(
        &execution,
        &symbols,
        9,
        PrimaryCompletionPolicy::ReportingContinuation
    )
    .contains_key("checked:ElectionSafety"));
}
