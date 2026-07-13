use rafter_sim::model_check::{Failure, FailureKind, SoakFailure};
use serde_json::json;

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
    let status = match kind {
        FailureKind::InvariantViolation => "fail",
        FailureKind::CoverageNotReached => "incomplete",
        FailureKind::HarnessError => "error",
    };
    json!({
        "event": "check-failure",
        "check_id": name,
        "status": status,
        "classification": kind.as_str(),
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
