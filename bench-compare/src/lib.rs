//! Shared workload definitions and reporting for the C5 comparison harness.
//!
//! Every comparison binary drives the same protocol workloads over a 3-voter
//! in-process cluster with in-memory storage and reports through
//! [`report_json`], so the emitted schema is identical across libraries.

use std::time::Duration;

/// Default proposal payload size, identical in all three harnesses for the C5
/// serial and pipelined workloads.
pub const PAYLOAD_BYTES: usize = 512;
/// Large-payload probe: one application entry just under Rafter's default
/// append-entry byte budget.
pub const LARGE_PAYLOAD_BYTES: usize = 512 * 1024 - 64;
/// Serial workload: one proposal in flight at a time. Sizes follow the C5
/// spec; the first harness run could not finish them until the runtime's
/// per-step costs were made proportional to new work (see METHODOLOGY.md).
pub const SERIAL_PROPOSALS: usize = 2_000;
/// Pipelined workload: up to [`PIPELINE_DEPTH`] proposals in flight.
pub const PIPELINED_PROPOSALS: usize = 20_000;
/// Maximum number of proposals in flight for the pipelined workload.
pub const PIPELINE_DEPTH: usize = 64;
/// Large-payload workload: enough proposals to expose append-budget shape
/// without turning every comparison run into a bulk-copy benchmark.
pub const LARGE_PAYLOAD_PROPOSALS: usize = 256;
/// Large-payload workload burst size.
pub const LARGE_PAYLOAD_PIPELINE_DEPTH: usize = 16;
/// Rafter-service tracked-write workload size.
pub const SERVICE_TRACKED_PROPOSALS: usize = 2_048;
/// Rafter-service tracked-write batch size.
pub const SERVICE_WRITE_BATCH_DEPTH: usize = 32;
/// Mixed write/read-index workload size.
pub const READ_LOAD_PROPOSALS: usize = 4_096;
/// Mixed write/read-index workload write burst size. One read barrier is
/// registered after every burst while those writes are still in flight.
pub const READ_LOAD_WRITE_BATCH_DEPTH: usize = 32;
/// Batched read-index workload size. These read barriers are grouped into
/// explicit deterministic read batches after a current-term commit.
pub const READ_BATCH_REQUESTS: usize = 4_096;
/// Batched read-index workload batch size. One confirmation round should
/// cover every read barrier in the batch.
pub const READ_BATCH_DEPTH: usize = 64;
/// Lease-read workload size. These read barriers are served from an active
/// leader lease after the benchmark explicitly establishes the lease with a
/// current-term commit and quorum acknowledgement.
pub const LEASE_READ_REQUESTS: usize = 4_096;
/// Codec workload: AppendEntries frames with many normal application entries.
pub const CODEC_BATCH_FRAMES: usize = 4_096;
/// Codec workload: AppendEntries frames near the default append byte budget.
pub const CODEC_LARGE_FRAMES: usize = 256;
/// MultiRaft workload: independently hosted groups stepped in round-robin
/// order.
pub const MULTIRAFT_GROUPS: usize = 32;
/// MultiRaft workload: proposal batches per group.
pub const MULTIRAFT_ROUNDS: usize = 64;
/// MultiRaft workload: proposal batch size submitted per group turn.
pub const MULTIRAFT_BATCH_DEPTH: usize = 16;
/// Leader-failover workload: independent failovers to sample.
pub const FAILOVER_ROUNDS: usize = 64;
/// Leader-failover workload: queued proposals replicated to the successor
/// before the old leader is partitioned.
pub const FAILOVER_QUEUED_PROPOSALS: usize = 64;

/// Fills a payload with a recognizable byte, mirroring
/// `rafter-bench-cluster`'s proposal payloads.
#[must_use]
pub fn payload() -> Vec<u8> {
    payload_of_size(PAYLOAD_BYTES)
}

/// Fills a proposal payload of `bytes` with a recognizable byte.
#[must_use]
pub fn payload_of_size(bytes: usize) -> Vec<u8> {
    vec![0xA5; bytes]
}

/// One workload's measurements: wall time plus per-proposal commit latency.
#[derive(Debug)]
pub struct WorkloadMetrics {
    pub name: &'static str,
    pub proposals: usize,
    pub payload_bytes: usize,
    pub max_in_flight: usize,
    pub elapsed: Duration,
    pub latencies: Vec<Duration>,
    pub shape: Option<ShapeMetrics>,
    pub service_shape: Option<ServiceShapeMetrics>,
    pub read_shape: Option<ReadShapeMetrics>,
    pub codec_shape: Option<CodecShapeMetrics>,
    pub multiraft_shape: Option<MultiRaftShapeMetrics>,
    pub failover_shape: Option<FailoverShapeMetrics>,
}

/// Protocol-shape counters for harnesses that can observe their output stream.
#[derive(Debug, Default)]
pub struct ShapeMetrics {
    pub proposal_batches: usize,
    pub append_messages: usize,
    pub append_entries: usize,
    pub commit_evaluations: usize,
    pub leader_broadcast_rounds: usize,
    pub outputs: usize,
}

/// Managed-service shape counters for the Rafter-only tracked-write harness.
#[derive(Debug, Default)]
pub struct ServiceShapeMetrics {
    pub write_batches: usize,
    pub runtime_step_batches: usize,
    pub tracked_proposals: usize,
    pub applied_writes: usize,
}

/// Read-index counters for mixed write/read workloads.
#[derive(Debug, Default)]
pub struct ReadShapeMetrics {
    pub read_requests: usize,
    pub read_grants: usize,
    pub confirmation_rounds: usize,
    pub latencies: Vec<Duration>,
}

/// Codec counters for AppendEntries encode/decode workloads.
#[derive(Debug, Default)]
pub struct CodecShapeMetrics {
    pub frames: usize,
    pub entries: usize,
    pub encoded_bytes: usize,
    pub allocation_events: usize,
}

/// MultiRaft counters for round-robin many-group workloads.
#[derive(Debug, Default)]
pub struct MultiRaftShapeMetrics {
    pub groups: usize,
    pub rounds: usize,
    pub group_batches: usize,
    pub runtime_step_batches: usize,
    pub tracked_proposals: usize,
    pub applied_proposals: usize,
}

/// Leader failover counters for queued-proposal recovery workloads.
#[derive(Debug, Default)]
pub struct FailoverShapeMetrics {
    pub failovers: usize,
    pub queued_proposals: usize,
    pub successor_prefailover_append_messages: usize,
    pub old_leader_applies: usize,
    pub successor_applies: usize,
    pub election_ticks: usize,
}

impl WorkloadMetrics {
    fn json(&self) -> String {
        let mut sorted = self.latencies.clone();
        sorted.sort();
        #[allow(clippy::cast_precision_loss)]
        let proposals_per_s = self.proposals as f64 / self.elapsed.as_secs_f64();
        let shape = self
            .shape
            .as_ref()
            .map(|shape| shape.json(self.proposals))
            .unwrap_or_default();
        let service_shape = self
            .service_shape
            .as_ref()
            .map(|shape| shape.json())
            .unwrap_or_default();
        let read_shape = self
            .read_shape
            .as_ref()
            .map(|shape| shape.json())
            .unwrap_or_default();
        let codec_shape = self
            .codec_shape
            .as_ref()
            .map(|shape| shape.json(self.elapsed))
            .unwrap_or_default();
        let multiraft_shape = self
            .multiraft_shape
            .as_ref()
            .map(|shape| shape.json())
            .unwrap_or_default();
        let failover_shape = self
            .failover_shape
            .as_ref()
            .map(|shape| shape.json())
            .unwrap_or_default();
        format!(
            "    {{\"name\": \"{}\", \"proposals\": {}, \"payload_bytes\": {}, \"max_in_flight\": {}, \"elapsed_ms\": {:.3}, \"proposals_per_s\": {:.0}, \"commit_latency_us\": {{\"p50\": {:.1}, \"p99\": {:.1}}}{}{}{}{}{}{}}}",
            self.name,
            self.proposals,
            self.payload_bytes,
            self.max_in_flight,
            self.elapsed.as_secs_f64() * 1_000.0,
            proposals_per_s,
            percentile(&sorted, 0.50).as_secs_f64() * 1_000_000.0,
            percentile(&sorted, 0.99).as_secs_f64() * 1_000_000.0,
            shape,
            service_shape,
            read_shape,
            codec_shape,
            multiraft_shape,
            failover_shape,
        )
    }
}

impl ShapeMetrics {
    fn json(&self, proposals: usize) -> String {
        format!(
            ", \"shape\": {{\"proposal_batches\": {}, \"append_messages\": {}, \"append_entries\": {}, \"commit_evaluations\": {}, \"leader_broadcast_rounds\": {}, \"outputs\": {}, \"append_messages_per_proposal\": {:.6}, \"append_entries_per_append_message\": {:.3}, \"log_entry_materializations_per_proposal\": {:.3}, \"commit_evaluations_per_committed_entry\": {:.6}, \"leader_broadcast_rounds_per_proposal_batch\": {:.3}, \"outputs_per_proposal\": {:.3}}}",
            self.proposal_batches,
            self.append_messages,
            self.append_entries,
            self.commit_evaluations,
            self.leader_broadcast_rounds,
            self.outputs,
            ratio(self.append_messages, proposals),
            ratio(self.append_entries, self.append_messages),
            ratio(self.append_entries, proposals),
            ratio(self.commit_evaluations, proposals),
            ratio(self.leader_broadcast_rounds, self.proposal_batches),
            ratio(self.outputs, proposals),
        )
    }
}

impl CodecShapeMetrics {
    fn json(&self, elapsed: Duration) -> String {
        format!(
            ", \"codec_shape\": {{\"frames\": {}, \"entries\": {}, \"encoded_bytes\": {}, \"allocation_events\": {}, \"entries_per_frame\": {:.3}, \"encoded_bytes_per_frame\": {:.1}, \"encoded_bytes_per_entry\": {:.1}, \"encoded_mb_per_s\": {:.1}, \"allocations_per_frame\": {:.3}}}",
            self.frames,
            self.entries,
            self.encoded_bytes,
            self.allocation_events,
            ratio(self.entries, self.frames),
            ratio(self.encoded_bytes, self.frames),
            ratio(self.encoded_bytes, self.entries),
            megabytes_per_second(self.encoded_bytes, elapsed),
            ratio(self.allocation_events, self.frames),
        )
    }
}

impl ReadShapeMetrics {
    fn json(&self) -> String {
        let mut sorted = self.latencies.clone();
        sorted.sort();
        format!(
            ", \"read_shape\": {{\"read_requests\": {}, \"read_grants\": {}, \"confirmation_rounds\": {}, \"read_grants_per_request\": {:.3}, \"confirmation_rounds_per_request\": {:.3}, \"read_latency_us\": {{\"p50\": {:.1}, \"p99\": {:.1}}}}}",
            self.read_requests,
            self.read_grants,
            self.confirmation_rounds,
            ratio(self.read_grants, self.read_requests),
            ratio(self.confirmation_rounds, self.read_requests),
            percentile(&sorted, 0.50).as_secs_f64() * 1_000_000.0,
            percentile(&sorted, 0.99).as_secs_f64() * 1_000_000.0,
        )
    }
}

impl ServiceShapeMetrics {
    fn json(&self) -> String {
        format!(
            ", \"service_shape\": {{\"write_batches\": {}, \"runtime_step_batches\": {}, \"tracked_proposals\": {}, \"applied_writes\": {}, \"runtime_batches_per_write_batch\": {:.3}, \"tracked_proposals_per_runtime_batch\": {:.3}, \"applied_writes_per_tracked_proposal\": {:.3}}}",
            self.write_batches,
            self.runtime_step_batches,
            self.tracked_proposals,
            self.applied_writes,
            ratio(self.runtime_step_batches, self.write_batches),
            ratio(self.tracked_proposals, self.runtime_step_batches),
            ratio(self.applied_writes, self.tracked_proposals),
        )
    }
}

impl MultiRaftShapeMetrics {
    fn json(&self) -> String {
        format!(
            ", \"multiraft_shape\": {{\"groups\": {}, \"rounds\": {}, \"group_batches\": {}, \"runtime_step_batches\": {}, \"tracked_proposals\": {}, \"applied_proposals\": {}, \"runtime_batches_per_group_batch\": {:.3}, \"tracked_proposals_per_runtime_batch\": {:.3}, \"applied_proposals_per_tracked_proposal\": {:.3}}}",
            self.groups,
            self.rounds,
            self.group_batches,
            self.runtime_step_batches,
            self.tracked_proposals,
            self.applied_proposals,
            ratio(self.runtime_step_batches, self.group_batches),
            ratio(self.tracked_proposals, self.runtime_step_batches),
            ratio(self.applied_proposals, self.tracked_proposals),
        )
    }
}

impl FailoverShapeMetrics {
    fn json(&self) -> String {
        format!(
            ", \"failover_shape\": {{\"failovers\": {}, \"queued_proposals\": {}, \"successor_prefailover_append_messages\": {}, \"old_leader_applies\": {}, \"successor_applies\": {}, \"election_ticks\": {}, \"queued_proposals_per_failover\": {:.3}, \"prefailover_append_messages_per_failover\": {:.3}, \"successor_applies_per_queued_proposal\": {:.3}, \"election_ticks_per_failover\": {:.3}}}",
            self.failovers,
            self.queued_proposals,
            self.successor_prefailover_append_messages,
            self.old_leader_applies,
            self.successor_applies,
            self.election_ticks,
            ratio(self.queued_proposals, self.failovers),
            ratio(self.successor_prefailover_append_messages, self.failovers),
            ratio(self.successor_applies, self.queued_proposals),
            ratio(self.election_ticks, self.failovers),
        )
    }
}

fn ratio(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        return 0.0;
    }
    #[allow(clippy::cast_precision_loss)]
    {
        numerator as f64 / denominator as f64
    }
}

fn megabytes_per_second(bytes: usize, elapsed: Duration) -> f64 {
    let seconds = elapsed.as_secs_f64();
    if seconds == 0.0 {
        return 0.0;
    }
    #[allow(clippy::cast_precision_loss)]
    {
        bytes as f64 / (1024.0 * 1024.0) / seconds
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
