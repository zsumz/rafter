//! Shared workload definitions and reporting for the C5 comparison harness.
//!
//! Every library binary drives the same two workloads over a 3-voter
//! in-process cluster with in-memory storage and reports through
//! [`report_json`], so the emitted schema is identical across libraries.

use std::time::Duration;

/// Proposal payload size, identical in all three harnesses.
pub const PAYLOAD_BYTES: usize = 512;
/// Serial workload: one proposal in flight at a time. Sizes follow the C5
/// spec; the first harness run could not finish them until the runtime's
/// per-step costs were made proportional to new work (see METHODOLOGY.md).
pub const SERIAL_PROPOSALS: usize = 2_000;
/// Pipelined workload: up to [`PIPELINE_DEPTH`] proposals in flight.
pub const PIPELINED_PROPOSALS: usize = 20_000;
/// Maximum number of proposals in flight for the pipelined workload.
pub const PIPELINE_DEPTH: usize = 64;

/// Fills a payload with a recognizable byte, mirroring
/// `rafter-bench-cluster`'s proposal payloads.
#[must_use]
pub fn payload() -> Vec<u8> {
    vec![0xA5; PAYLOAD_BYTES]
}

/// One workload's measurements: wall time plus per-proposal commit latency.
#[derive(Debug)]
pub struct WorkloadMetrics {
    pub name: &'static str,
    pub proposals: usize,
    pub max_in_flight: usize,
    pub elapsed: Duration,
    pub latencies: Vec<Duration>,
}

impl WorkloadMetrics {
    fn json(&self) -> String {
        let mut sorted = self.latencies.clone();
        sorted.sort();
        #[allow(clippy::cast_precision_loss)]
        let proposals_per_s = self.proposals as f64 / self.elapsed.as_secs_f64();
        format!(
            "    {{\"name\": \"{}\", \"proposals\": {}, \"payload_bytes\": {}, \"max_in_flight\": {}, \"elapsed_ms\": {:.3}, \"proposals_per_s\": {:.0}, \"commit_latency_us\": {{\"p50\": {:.1}, \"p99\": {:.1}}}}}",
            self.name,
            self.proposals,
            PAYLOAD_BYTES,
            self.max_in_flight,
            self.elapsed.as_secs_f64() * 1_000.0,
            proposals_per_s,
            percentile(&sorted, 0.50).as_secs_f64() * 1_000_000.0,
            percentile(&sorted, 0.99).as_secs_f64() * 1_000_000.0,
        )
    }
}

/// Nearest-rank percentile over an ascending sample, matching
/// `rafter-bench-cluster`'s percentile math.
#[must_use]
pub fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    let rank = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[rank.min(sorted.len() - 1)]
}

/// Renders the single JSON object a library run prints on stdout.
#[must_use]
pub fn report_json(
    library: &str,
    version: &str,
    commit_latency_definition: &str,
    workloads: &[WorkloadMetrics],
) -> String {
    use std::fmt::Write as _;

    let mut out = String::from("{\n");
    out.push_str("  \"harness\": \"bench-compare\",\n");
    let _ = writeln!(out, "  \"library\": \"{library}\",");
    let _ = writeln!(out, "  \"version\": \"{version}\",");
    let _ = writeln!(
        out,
        "  \"commit_latency_definition\": \"{commit_latency_definition}\","
    );
    out.push_str("  \"workloads\": [\n");
    let rendered: Vec<String> = workloads.iter().map(WorkloadMetrics::json).collect();
    out.push_str(&rendered.join(",\n"));
    out.push_str("\n  ]\n}");
    out
}
