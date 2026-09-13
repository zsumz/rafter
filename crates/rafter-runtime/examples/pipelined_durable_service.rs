//! Reference durable service over the public WAL and one-credit pipeline APIs.
//!
//! The example keeps peer and ordered-application admission bounded, persists
//! application state and its applied floor before acknowledging a proposal,
//! recovers that floor, compacts through a real application snapshot, catches
//! up a lagging follower, and shuts every worker down explicitly. The in-process
//! transport is deliberately unauthenticated demo plumbing; a production
//! embedding must supply authenticated peer identities and its own
//! queue-overload policy.
//!
//! Run with:
//!
//! ```text
//! cargo run -p rafter-runtime --example pipelined_durable_service
//! ```

#[allow(
    dead_code,
    reason = "the shared application module also serves the process example"
)]
#[path = "replicated_kv/app_state.rs"]
mod app_state;
#[path = "pipelined_durable_service/application.rs"]
mod application;
#[path = "pipelined_durable_service/cluster.rs"]
mod cluster;
#[allow(
    dead_code,
    reason = "the shared example codec also serves the process example"
)]
#[path = "replicated_kv/codec.rs"]
mod codec;
#[path = "pipelined_durable_service/driver.rs"]
mod driver;
#[path = "pipelined_durable_service/storage.rs"]
mod storage;

use std::collections::BTreeMap;

use rafter::{LogIndex, NodeId};

/// Observable result of the reference service lifecycle.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServiceReport {
    /// Values read after snapshot catch-up and restart recovery.
    pub final_values: BTreeMap<String, String>,
    /// Number of proposal batches that used the one-credit persistence worker.
    pub pipelined_operations: u64,
    /// Number of worker submission failures recovered by synchronous persistence.
    pub synchronous_fallbacks: u64,
    /// Largest number of peer messages retained by the bounded queue.
    pub max_peer_queue_depth: usize,
    /// Configured peer-message queue capacity.
    pub peer_queue_capacity: usize,
    /// Application snapshot boundary used for follower catch-up.
    pub snapshot_index: LogIndex,
    /// Durable application floor loaded when node 2 reopened.
    pub restarted_applied_floor: LogIndex,
    /// Initial elected leader.
    pub initial_leader: NodeId,
}

/// Runs the complete reference lifecycle below `root`.
#[must_use]
pub fn run_service(root: &std::path::Path, keep_dir: bool) -> ServiceReport {
    cluster::run(root, keep_dir)
}

fn main() {
    let root = std::env::temp_dir().join(format!(
        "rafter-pipelined-durable-service-{}",
        std::process::id()
    ));
    let report = run_service(&root, false);
    println!(
        "pipelined durable service: {} operations, queue {}/{}, snapshot {}, values {:?}",
        report.pipelined_operations,
        report.max_peer_queue_depth,
        report.peer_queue_capacity,
        report.snapshot_index.0,
        report.final_values
    );
}
