//! JSON measurements with separate proposal and batch-completion samples.

#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "bounded benchmark counts and percentile ranks"
)]

use std::time::Duration;

#[derive(Debug)]
pub(super) struct ProposalMetrics {
    pub(super) batch_size: usize,
    pub(super) proposals: usize,
    pub(super) payload_bytes: usize,
    pub(super) elapsed: Duration,
    pub(super) latencies: Vec<Duration>,
    pub(super) batch_latencies: Vec<Duration>,
    pub(super) leader_steps: Duration,
    pub(super) message_pumps: Duration,
}

#[derive(Debug)]
pub(super) struct SnapshotMetrics {
    pub(super) payload_bytes: usize,
    pub(super) elapsed: Duration,
    pub(super) preparation: Duration,
}

pub(super) fn report_json(workloads: &[String], hard_state: &str) -> String {
    format!(
        "{{\n  \"harness\": \"rafter-bench-cluster\",\n  \"hard_state\": \"{hard_state}\",\n  \"workloads\": [\n{}\n  ]\n}}",
        workloads.join(",\n")
    )
}

pub(super) fn proposal_json(metrics: &ProposalMetrics) -> String {
    format!(
        "    {{\"name\": \"proposals\", \"batch_size\": {}, \"proposals\": {}, \"payload_bytes\": {}, \"elapsed_ms\": {:.3}, \"proposals_per_s\": {:.3}, \"commit_latency_ms\": {}, \"proposal_batches\": {}, \"batch_completion_latency_ms\": {}, \"phase_ms\": {{\"leader_steps\": {:.3}, \"message_pumps\": {:.3}}}}}",
        metrics.batch_size, metrics.proposals, metrics.payload_bytes,
        millis(metrics.elapsed), metrics.proposals as f64 / metrics.elapsed.as_secs_f64(),
        latency_json(&metrics.latencies), metrics.batch_latencies.len(),
        latency_json(&metrics.batch_latencies), millis(metrics.leader_steps),
        millis(metrics.message_pumps),
    )
}

pub(super) fn snapshot_json(metrics: &SnapshotMetrics) -> String {
    format!(
        "    {{\"name\": \"snapshot_transfer\", \"payload_bytes\": {}, \"elapsed_ms\": {:.3}, \"throughput_mib_per_s\": {:.3}, \"preparation_ms\": {:.3}}}",
        metrics.payload_bytes, millis(metrics.elapsed),
        metrics.payload_bytes as f64 / (1024.0 * 1024.0) / metrics.elapsed.as_secs_f64(),
        millis(metrics.preparation),
    )
}

fn latency_json(samples: &[Duration]) -> String {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    format!(
        "{{\"samples\": {}, \"p50\": {:.3}, \"p99\": {:.3}, \"max\": {:.3}}}",
        sorted.len(),
        millis(percentile(&sorted, 0.5)),
        millis(percentile(&sorted, 0.99)),
        millis(percentile(&sorted, 1.0))
    )
}

fn millis(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let rank = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}
