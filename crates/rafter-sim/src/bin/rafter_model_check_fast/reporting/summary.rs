use std::time::Duration;

use rafter_sim::model_check::{ExplorationCompletion, Summary};
use serde_json::json;

use super::EVENT_PREFIX;

pub(crate) fn print_raft_summary(name: &str, summary: Summary, duration: Duration) {
    println!("{}", super::raft_summary_line(name, summary, duration));
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

pub(crate) fn print_profile_total(
    profile: &str,
    protocol_states: usize,
    verifier_states: usize,
    target_protocol_states: usize,
    target_verifier_states: usize,
) {
    let passed =
        protocol_states >= target_protocol_states && verifier_states >= target_verifier_states;
    println!(
        "{EVENT_PREFIX}{}",
        json!({
            "event": "profile-total",
            "check_id": format!("raft-profile-total-{}", profile.trim_start_matches("raft-")),
            "profile": profile,
            "status": if passed { "pass" } else { "incomplete" },
            "classification": if passed { serde_json::Value::Null } else { json!("coverage-not-reached") },
            "unique_protocol_states": protocol_states,
            "unique_verifier_states": verifier_states,
            "target_protocol_states": target_protocol_states,
            "target_verifier_states": target_verifier_states,
        })
    );
}
