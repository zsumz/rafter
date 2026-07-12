use std::time::Duration;

use rafter_sim::model_check::{
    ExplorationCompletion, Failure, FailureKind, SoakConfig, SoakFailure, SoakSummary, Summary,
};
use serde_json::json;

const EVENT_PREFIX: &str = "RAFTER_EVENT ";

pub(crate) fn print_raft_summary(name: &str, summary: Summary, duration: Duration) {
    println!("{}", raft_summary_line(name, summary, duration));
    println!("{EVENT_PREFIX}{}", raft_event(name, summary, duration));
}

fn raft_event(name: &str, summary: Summary, duration: Duration) -> serde_json::Value {
    let frontier_exhausted = summary.completion() == ExplorationCompletion::FrontierExhausted;
    let observations = summary
        .observation_labels()
        .map(|label| (label.to_owned(), json!(1)))
        .collect::<serde_json::Map<_, _>>();
    json!({
        "event": "exhaustive-check",
        "check_id": name,
        "status": if frontier_exhausted { "pass" } else { "incomplete" },
        "classification": if frontier_exhausted { serde_json::Value::Null } else { json!("coverage-not-reached") },
        "completion": summary.completion().to_string(),
        "configured_depth": summary.max_depth(),
        "reached_depth": summary.reached_depth(),
        "unique_states": summary.unique_states(),
        "unique_protocol_states": summary.unique_protocol_states(),
        "unique_verifier_states": summary.unique_verifier_states(),
        "explored_states": summary.explored_states(),
        "explored_actions": summary.explored_actions(),
        "observations": observations,
        "duration_ms": duration.as_millis(),
    })
}

pub(crate) fn raft_summary_line(name: &str, summary: Summary, duration: Duration) -> String {
    let pruned_states = summary
        .explored_states()
        .saturating_sub(summary.unique_verifier_states());
    format_raft_summary_line(
        name,
        RaftSummaryMetrics {
            unique_states: summary.unique_states(),
            unique_protocol_states: summary.unique_protocol_states(),
            unique_verifier_states: summary.unique_verifier_states(),
            explored_states: summary.explored_states(),
            explored_actions: summary.explored_actions(),
            pruned_states,
            configured_depth: summary.max_depth(),
            reached_depth: summary.reached_depth(),
            completion: summary.completion(),
        },
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
        RaftSummaryMetrics {
            unique_states: unique_verifier_states,
            unique_protocol_states,
            unique_verifier_states,
            explored_states,
            explored_actions,
            pruned_states,
            configured_depth: max_depth,
            reached_depth: max_depth,
            completion: ExplorationCompletion::FrontierExhausted,
        },
        duration,
    )
}

#[derive(Clone, Copy)]
struct RaftSummaryMetrics {
    unique_states: usize,
    unique_protocol_states: usize,
    unique_verifier_states: usize,
    explored_states: usize,
    explored_actions: usize,
    pruned_states: usize,
    configured_depth: usize,
    reached_depth: usize,
    completion: ExplorationCompletion,
}

fn format_raft_summary_line(name: &str, metrics: RaftSummaryMetrics, duration: Duration) -> String {
    let pruning_parts_per_million = metrics
        .pruned_states
        .saturating_mul(1_000_000)
        .checked_div(metrics.explored_states)
        .unwrap_or_default();
    let pruning_whole = pruning_parts_per_million / 1_000_000;
    let pruning_fraction = pruning_parts_per_million % 1_000_000;
    format!(
        "model-check {name}: unique_states={} unique_protocol_states={} unique_verifier_states={} explored_states={} explored_actions={} pruned_states={} pruning_rate={pruning_whole}.{pruning_fraction:06} configured_depth={} reached_depth={} completion={} duration_ms={}",
        metrics.unique_states,
        metrics.unique_protocol_states,
        metrics.unique_verifier_states,
        metrics.explored_states,
        metrics.explored_actions,
        metrics.pruned_states,
        metrics.configured_depth,
        metrics.reached_depth,
        metrics.completion,
        duration.as_millis()
    )
}

pub(crate) fn print_soak_summary(
    name: &str,
    summary: &SoakSummary,
    config: SoakConfig,
    duration: Duration,
) {
    println!(
        "model-check {name}: seed={:#x} steps={} observed_actions={:?} duration_ms={}",
        summary.seed().0,
        summary.steps_executed(),
        summary.observed_actions(),
        duration.as_millis()
    );
    let observed_actions = summary
        .observed_actions()
        .iter()
        .map(|kind| kind.as_str())
        .collect::<Vec<_>>();
    let mut liveness_features = vec![
        "leader-convergence",
        "quorum-only-leader-convergence",
        "proposal-progress",
        "proposal-termination",
    ];
    let mut observations = serde_json::Map::from_iter([
        ("post_heal_quiescent_leaders".to_owned(), json!(1)),
        ("quorum_only_post_fault_leaders".to_owned(), json!(1)),
        ("accepted_completed_liveness_proposals".to_owned(), json!(1)),
        ("terminated_liveness_proposals".to_owned(), json!(1)),
    ]);
    if config.checks_read_progress() {
        liveness_features.push("read-barrier");
        observations.insert("completed_liveness_read_barriers".to_owned(), json!(1));
    }
    if config.checks_membership_progress() {
        liveness_features.push("membership-transition");
        observations.insert(
            "completed_stable_membership_transitions".to_owned(),
            json!(1),
        );
    }
    if config.checks_transfer_progress() {
        liveness_features.push("leadership-transfer");
        observations.insert("completed_target_leadership_transfers".to_owned(), json!(1));
    }
    if config.checks_snapshot_progress() {
        liveness_features.push("snapshot-catch-up");
        observations.insert("completed_expected_snapshot_catchups".to_owned(), json!(1));
    }
    println!(
        "{EVENT_PREFIX}{}",
        json!({
            "event": "soak-check",
            "check_id": name,
            "status": "pass",
            "classification": serde_json::Value::Null,
            "seed": summary.seed().0,
            "steps": summary.steps_executed(),
            "observed_actions": observed_actions,
            "liveness_features": liveness_features,
            "observations": observations,
            "duration_ms": duration.as_millis(),
        })
    );
}

pub(crate) fn print_raft_failure(name: &str, failure: &Failure) {
    print_failure_event(name, failure.kind(), failure.invariant(), failure.message());
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
    print_failure_event(
        name,
        failure.failure().kind(),
        failure.failure().invariant(),
        failure.failure().message(),
    );
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

fn print_failure_event(name: &str, kind: FailureKind, invariant: &str, message: &str) {
    println!(
        "{EVENT_PREFIX}{}",
        failure_event(name, kind, invariant, message)
    );
}

fn failure_event(
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

#[cfg(test)]
mod tests {
    use rafter_sim::model_check::FailureKind;

    use super::failure_event;

    #[test]
    fn machine_failure_event_preserves_classification_and_message() {
        let event = failure_event(
            "raft-commit",
            FailureKind::CoverageNotReached,
            "CM-02",
            "required witness absent",
        );
        assert_eq!(event["event"], "check-failure");
        assert_eq!(event["status"], "incomplete");
        assert_eq!(event["classification"], "coverage-not-reached");
        assert_eq!(event["invariant"], "CM-02");
        assert_eq!(event["message"], "required witness absent");
    }
}
