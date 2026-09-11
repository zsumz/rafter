//! Failure events and the trace timeline printed beside them.
//!
//! A failure must name a reviewed invariant identifier; an unregistered label
//! is downgraded to a harness error rather than reported as a protocol
//! violation, so an unreviewed check can never be mistaken for a real one.
//! Classification comes from the failure itself and is never inferred here.

use rafter_sim::model_check::{reviewed_invariant_id, Failure, FailureKind, SoakFailure};
use serde_json::json;

use super::rules::rule_guide;
use super::EVENT_PREFIX;

pub(crate) fn print_raft_failure(name: &str, failure: &Failure) {
    print_failure_event(name, failure.kind(), failure.invariant(), failure.message());
    eprintln!("model-check {name} failed: {failure}");
    for line in super::failure_timeline_lines(
        name,
        failure.kind(),
        failure.invariant(),
        failure.message(),
        failure
            .trace()
            .iter()
            .enumerate()
            .map(|(index, action)| (index, action.to_string())),
    ) {
        eprintln!("  {line}");
    }
    eprintln!("state={:?}", failure.state());
}

pub(crate) fn print_soak_failure(name: &str, failure: &SoakFailure) {
    print_failure_event(
        name,
        failure.failure().kind(),
        failure.failure().invariant(),
        failure.failure().message(),
    );
    eprintln!("model-check {name} failed: {failure}");
    eprintln!("seed={:#x}", failure.seed().0);
    eprintln!("step={}", failure.step());
    for line in super::failure_timeline_lines(
        name,
        failure.failure().kind(),
        failure.failure().invariant(),
        failure.failure().message(),
        failure
            .trace()
            .iter()
            .enumerate()
            .map(|(index, action)| (index, action.to_string())),
    ) {
        eprintln!("  {line}");
    }
    eprintln!("state={:?}", failure.failure().state());
}

fn print_failure_event(name: &str, kind: FailureKind, invariant: &str, message: &str) {
    println!(
        "{EVENT_PREFIX}{}",
        failure_event(name, kind, invariant, message)
    );
}

pub(super) fn failure_event(
    name: &str,
    kind: FailureKind,
    invariant: &str,
    message: &str,
) -> serde_json::Value {
    let Some(invariant_id) = reviewed_invariant_id(invariant) else {
        return json!({
            "event": "check-failure",
            "event_version": 2,
            "check_id": name,
            "status": "error",
            "classification": FailureKind::HarnessError.as_str(),
            "invariant_id": serde_json::Value::Null,
            "invariant": invariant,
            "message": format!(
                "check failure used an unregistered invariant label {invariant:?}; original {kind}: {message}"
            ),
            "reported_classification": kind.as_str(),
            "reported_message": message,
        });
    };
    let status = match kind {
        FailureKind::InvariantViolation => "fail",
        FailureKind::CoverageNotReached => "incomplete",
        FailureKind::HarnessError => "error",
    };
    json!({
        "event": "check-failure",
        "event_version": 2,
        "check_id": name,
        "status": status,
        "classification": kind.as_str(),
        "invariant_id": invariant_id,
        "invariant": invariant,
        "message": message,
    })
}

pub(crate) fn failure_timeline_lines(
    name: &str,
    failure_kind: FailureKind,
    invariant: &str,
    message: &str,
    trace: impl IntoIterator<Item = (usize, String)>,
) -> Vec<String> {
    let mut lines = vec![format!(
        "ERROR test model failure name={} failure_kind={} invariant={} error_message={}",
        field_value(name),
        failure_kind,
        field_value(invariant),
        field_value(message)
    )];
    if let Some(guide) = rule_guide(invariant) {
        lines.push(format!(
            "INFO test rule reference source={} transition={} guide=docs/protocol-rules.md#{} rule={}",
            guide.source, guide.transition, guide.guide_anchor, field_value(guide.explanation),
        ));
    }
    // The supplied trace is preserved verbatim. No shortest-counterexample
    // claim is justified by formatting or by replay stopping at first failure.
    lines.push("INFO test trace provenance reduction=none replay=replay_raft_trace observations=ReplayReport::states".to_string());
    lines.extend(trace.into_iter().map(|(index, action)| {
        format!(
            "DEBUG test trace step step={index} action={}",
            field_value(&action)
        )
    }));
    lines
}

fn field_value(value: &str) -> String {
    if value.contains(char::is_whitespace) {
        format!("{value:?}")
    } else {
        value.to_string()
    }
}
