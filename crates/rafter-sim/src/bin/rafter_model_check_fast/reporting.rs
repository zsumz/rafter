use std::time::Duration;

use rafter_sim::model_check::{
    ExplorationCompletion, Failure, FailureKind, SoakFailure, SoakSummary, Summary,
};

pub(crate) fn print_raft_summary(name: &str, summary: Summary, duration: Duration) {
    println!("{}", raft_summary_line(name, summary, duration));
}

pub(crate) fn raft_summary_line(name: &str, summary: Summary, duration: Duration) -> String {
    let pruned_states = summary
        .explored_states()
        .saturating_sub(summary.unique_verifier_states());
    format_raft_summary_line(
        name,
        summary.unique_states(),
        summary.unique_protocol_states(),
        summary.unique_verifier_states(),
        summary.explored_states(),
        summary.explored_actions(),
        pruned_states,
        summary.max_depth(),
        summary.reached_depth(),
        summary.completion(),
        duration,
    )
}

#[cfg(test)]
pub(crate) fn raft_summary_line_for_counts(
    name: &str,
    unique_protocol_states: usize,
    unique_verifier_states: usize,
    explored_states: usize,
    explored_actions: usize,
    max_depth: usize,
    duration: Duration,
) -> String {
    let pruned_states = explored_states.saturating_sub(unique_verifier_states);
    format_raft_summary_line(
        name,
        unique_verifier_states,
        unique_protocol_states,
        unique_verifier_states,
        explored_states,
        explored_actions,
        pruned_states,
        max_depth,
        max_depth,
        ExplorationCompletion::FrontierExhausted,
        duration,
    )
}

fn format_raft_summary_line(
    name: &str,
    unique_states: usize,
    unique_protocol_states: usize,
    unique_verifier_states: usize,
    explored_states: usize,
    explored_actions: usize,
    pruned_states: usize,
    configured_depth: usize,
    reached_depth: usize,
    completion: ExplorationCompletion,
    duration: Duration,
) -> String {
    let pruning_rate = if explored_states == 0 {
        0.0
    } else {
        pruned_states as f64 / explored_states as f64
    };
    format!(
        "model-check {name}: unique_states={} unique_protocol_states={} unique_verifier_states={} explored_states={} explored_actions={} pruned_states={} pruning_rate={:.6} configured_depth={} reached_depth={} completion={} duration_ms={}",
        unique_states,
        unique_protocol_states,
        unique_verifier_states,
        explored_states,
        explored_actions,
        pruned_states,
        pruning_rate,
        configured_depth,
        reached_depth,
        completion,
        duration.as_millis()
    )
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
    eprintln!("model-check {name} failed: {failure}");
    eprintln!("seed={:#x}", failure.seed().0);
    eprintln!("step={}", failure.step());
    for line in failure_timeline_lines(
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
