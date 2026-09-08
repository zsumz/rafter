//! Fixed-size proposal and snapshot workloads with explicit timing boundaries.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use rafter::{Input as RaftInput, LogIndex, NodeId, RaftSnapshotMetadata};
use rafter_storage::PersistedRaftSnapshot;

use super::{
    cluster::Cluster,
    config::Config,
    report::{ProposalMetrics, SnapshotMetrics},
};

/// Drives the configured proposals through an elected leader, feeding
/// follower responses synchronously, submitting `batch_size` inputs per
/// `step_batch` call. Commit latency is measured from batch submission to
/// the leader's `Apply` for each proposal's index.
pub(super) fn proposal_workload(
    directory: &std::path::Path,
    batch_size: usize,
    config: &Config,
) -> ProposalMetrics {
    let mut cluster = Cluster::elect(directory);

    let mut latencies: Vec<Duration> = Vec::with_capacity(config.proposals);
    let mut submitted_at: BTreeMap<LogIndex, Instant> = BTreeMap::new();
    let mut next_index = cluster.leader().last_log_index();
    let started = Instant::now();

    let mut batch_latencies = Vec::with_capacity(config.proposals.div_ceil(batch_size));
    let mut leader_steps = Duration::ZERO;
    let mut message_pumps = Duration::ZERO;
    let mut remaining = config.proposals;
    while remaining > 0 {
        let batch = remaining.min(batch_size);
        remaining -= batch;
        let inputs: Vec<RaftInput> = (0..batch)
            .map(|_| RaftInput::ClientProposal {
                payload: vec![0xA5; config.payload_bytes],
            })
            .collect();
        let now = Instant::now();
        for _ in 0..batch {
            next_index = LogIndex(next_index.0 + 1);
            submitted_at.insert(next_index, now);
        }
        let leader_started = Instant::now();
        let outputs = cluster.step_leader_batch(inputs);
        leader_steps += leader_started.elapsed();
        let pump_started = Instant::now();
        cluster.pump(outputs, &mut |index| {
            if let Some(at) = submitted_at.remove(&index) {
                latencies.push(at.elapsed());
            }
        });
        message_pumps += pump_started.elapsed();
        batch_latencies.push(now.elapsed());
    }
    assert!(
        submitted_at.is_empty(),
        "every proposal commits and applies at the leader"
    );

    ProposalMetrics {
        batch_size,
        proposals: config.proposals,
        payload_bytes: config.payload_bytes,
        elapsed: started.elapsed(),
        latencies,
        batch_latencies,
        leader_steps,
        message_pumps,
    }
}

/// A leader with a large durable snapshot streams it to a follower that
/// never saw the compacted prefix; measures end-to-end transfer throughput
/// including the receiver's staged appends and promotion.
///
/// The lagging follower is held back from the boundary commit (its match
/// stays honestly at zero) rather than wiped after acknowledging: a
/// follower that loses acknowledged entries violates Raft's durability
/// assumption, and the leader's match floor deliberately refuses to
/// walk back below an acknowledgement.
pub(super) fn snapshot_workload(
    directory: &std::path::Path,
    payload_bytes: usize,
) -> SnapshotMetrics {
    let mut cluster = Cluster::elect(directory);

    // Commit one entry so the boundary exists — with every message to the
    // lagging follower dropped, so the quorum is the leader plus node 2 —
    // then compact through it under a synthetic payload.
    let lagging = NodeId(3);
    cluster.drop_sends_to = Some(lagging);
    let outputs = cluster.step_leader_batch(vec![RaftInput::ClientProposal {
        payload: b"boundary".to_vec(),
    }]);
    cluster.pump(outputs, &mut |_| {});
    cluster.drop_sends_to = None;
    let leader = cluster.leader();
    let boundary = leader.commit_index();
    let term = leader.current_term();
    let metadata = RaftSnapshotMetadata::new(
        rafter::SnapshotGroupId::new("bench-group").expect("valid group id"),
        cluster.leader_id,
        boundary,
        term,
        term,
        rafter::ApplicationSnapshotMetadata::new(
            rafter::ApplicationSnapshotKind::new("bench_state").expect("valid kind"),
            rafter::ApplicationSnapshotVersion::new(1).expect("valid version"),
        ),
    )
    .expect("valid snapshot metadata");
    let preparation_started = Instant::now();
    cluster
        .leader()
        .compact_log_with_snapshot(PersistedRaftSnapshot {
            metadata,
            application_payload: vec![0x5A; payload_bytes],
        })
        .expect("leader compacts through its snapshot");
    let preparation = preparation_started.elapsed();

    let started = Instant::now();
    // Ticking the leader reaches the lagging follower, whose rejection
    // walks the leader to the snapshot path; pumping runs the chunked
    // transfer to installation.
    while cluster.node(lagging).snapshot_index() < boundary {
        let outputs = cluster.step_leader_batch(vec![RaftInput::Tick]);
        cluster.pump(outputs, &mut |_| {});
    }

    SnapshotMetrics {
        payload_bytes,
        elapsed: started.elapsed(),
        preparation,
    }
}
