use std::time::Duration;

use rafter_sim::model_check::{Failure, SoakFailure, SoakSummary, Summary};

pub(crate) fn print_raft_summary(name: &str, summary: Summary, duration: Duration) {
    println!(
        "model-check {name}: unique_states={} explored_states={} explored_actions={} max_depth={} duration_ms={}",
        summary.unique_states(),
        summary.explored_states(),
        summary.explored_actions(),
        summary.max_depth(),
        duration.as_millis()
    );
}

pub(crate) fn print_soak_summary(name: &str, summary: &SoakSummary, duration: Duration) {
    println!(
        "model-check {name}: seed={:#x} steps={} observed_actions={:?} duration_ms={}",
        summary.seed().0,
        summary.steps_executed(),
        summary.observed_actions(),
        duration.as_millis()
    );
}

pub(crate) fn print_raft_failure(name: &str, failure: &Failure) {
    eprintln!("model-check {name} failed: {failure}");
    for line in failure_timeline_lines(
        name,
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
    eprintln!("model-check {name} failed: {failure}");
    eprintln!("seed={:#x}", failure.seed().0);
    eprintln!("step={}", failure.step());
    for line in failure_timeline_lines(
        name,
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

pub(crate) fn failure_timeline_lines(
    name: &str,
    invariant: &str,
    message: &str,
    trace: impl IntoIterator<Item = (usize, String)>,
) -> Vec<String> {
    let mut lines = vec![format!(
        "ERROR test model failure name={} invariant={} error_message={}",
        field_value(name),
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
