//! Static node configuration and effective protocol posture.
//!
//! `NodeConfig` records requested protocol features and derives the behavior
//! that is safe under the configured timing. It also owns startup membership
//! and replication-window budgets. Dynamic membership lives in
//! `node::membership`, not here.

mod error;
mod features;
mod options;
mod state;

pub use error::NodeConfigError;
use features::RequestedFeatures;

use crate::{MembershipConfig, NodeId};

/// Default append-entries batch budget.
///
/// The budget bounds a batch beyond its first entry. A single entry may exceed
/// it, so transports with hard frame limits must still admit the largest valid
/// proposal or replicated entry.
pub const DEFAULT_MAX_APPEND_ENTRIES_BYTES: usize = 512 * 1024;

/// Default per-follower in-flight append window, in batches.
///
/// A window of one serializes replication on the round trip. The default
/// pipelines eight batches so a lagging follower catches up at wire speed
/// instead of acknowledgement pace.
pub const DEFAULT_MAX_INFLIGHT_APPENDS: usize = 8;

/// Default per-follower in-flight append window, in payload bytes: the
/// default batch count at the default append budget.
pub const DEFAULT_MAX_INFLIGHT_BYTES: usize =
    DEFAULT_MAX_INFLIGHT_APPENDS * DEFAULT_MAX_APPEND_ENTRIES_BYTES;

/// Default leader heartbeat interval. One preserves the historical behavior:
/// every leader tick broadcasts `AppendEntries`.
pub const DEFAULT_HEARTBEAT_INTERVAL_TICKS: u64 = 1;

/// Check-quorum needs at least one leader tick to broadcast before the
/// election-timeout window can close.
pub const MIN_CHECK_QUORUM_ELECTION_TIMEOUT_TICKS: u64 = 2;

/// Static configuration for one Raft node.
///
/// Builder methods retain the caller's requested feature posture. Accessors
/// expose the behavior that is currently effective after applying timing and
/// feature-dependency rules.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct NodeConfig {
    id: NodeId,
    peers: Vec<NodeId>,
    static_voters: Vec<NodeId>,
    static_membership: MembershipConfig,

    election_timeout_ticks: u64,
    election_jitter_ticks: u64,
    requested_heartbeat_interval_ticks: u64,

    max_append_entries_bytes: usize,
    max_inflight_appends: usize,
    max_inflight_bytes: usize,

    requested_features: RequestedFeatures,
}
