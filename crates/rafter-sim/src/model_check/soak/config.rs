use rafter::NodeId;

use crate::SimSeed;

/// Configuration for deterministic randomized Raft soak runs.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SoakConfig {
    pub(in crate::model_check) seed: SimSeed,
    pub(in crate::model_check) steps: usize,
    pub(in crate::model_check) max_proposals: usize,
    pub(in crate::model_check) max_restarts: usize,
    pub(in crate::model_check) max_read_indexes: usize,
    pub(in crate::model_check) max_membership_changes: usize,
    pub(in crate::model_check) max_transfers: usize,
    pub(in crate::model_check) max_partitions: usize,
    pub(in crate::model_check) max_lossy_restarts: usize,
    pub(in crate::model_check) snapshot_catchup_probe: bool,
    /// Tick-rate skew: `(node, weight)` makes tick actions favour `node`
    /// `weight`-to-one over each other node, modelling one process driving
    /// its kernel faster than its peers.
    pub(in crate::model_check) tick_skew: Option<(NodeId, u32)>,
}

impl SoakConfig {
    /// Constructs a deterministic soak configuration.
    #[must_use]
    pub const fn new(seed: SimSeed, steps: usize) -> Self {
        Self {
            seed,
            steps,
            max_proposals: 0,
            max_restarts: 0,
            max_read_indexes: 0,
            max_membership_changes: 0,
            max_transfers: 0,
            max_partitions: 0,
            max_lossy_restarts: 0,
            snapshot_catchup_probe: false,
            tick_skew: None,
        }
    }

    /// Allows the soak to inject up to `max_proposals` client proposals.
    #[must_use]
    pub const fn with_max_proposals(mut self, max_proposals: usize) -> Self {
        self.max_proposals = max_proposals;
        self
    }

    /// Allows the soak to restart up to `max_restarts` nodes.
    #[must_use]
    pub const fn with_max_restarts(mut self, max_restarts: usize) -> Self {
        self.max_restarts = max_restarts;
        self
    }

    /// Allows the soak to register up to `max_read_indexes` read barriers.
    #[must_use]
    pub const fn with_max_read_indexes(mut self, max_read_indexes: usize) -> Self {
        self.max_read_indexes = max_read_indexes;
        self
    }

    /// Allows the soak to inject up to `max_membership_changes` membership
    /// proposals.
    #[must_use]
    pub const fn with_max_membership_changes(mut self, max_membership_changes: usize) -> Self {
        self.max_membership_changes = max_membership_changes;
        self
    }

    /// Allows the soak to request up to `max_transfers` leadership
    /// transfers.
    #[must_use]
    pub const fn with_max_transfers(mut self, max_transfers: usize) -> Self {
        self.max_transfers = max_transfers;
        self
    }

    /// Allows the soak to install up to `max_partitions` sustained
    /// partitions (healing is always enabled once one exists).
    #[must_use]
    pub const fn with_max_partitions(mut self, max_partitions: usize) -> Self {
        self.max_partitions = max_partitions;
        self
    }

    /// Allows up to `max_lossy_restarts` floor-truncating lossy restarts -
    /// legal by construction, so safety invariants stay in force.
    #[must_use]
    pub const fn with_max_lossy_restarts(mut self, max_lossy_restarts: usize) -> Self {
        self.max_lossy_restarts = max_lossy_restarts;
        self
    }

    /// Enables a bounded post-heal snapshot catch-up probe.
    #[must_use]
    pub const fn with_snapshot_catchup_probe(mut self) -> Self {
        self.snapshot_catchup_probe = true;
        self
    }

    /// Skews tick selection `weight`-to-one toward `node`.
    #[must_use]
    pub const fn with_tick_skew(mut self, node: NodeId, weight: u32) -> Self {
        self.tick_skew = Some((node, weight));
        self
    }

    /// Returns the deterministic simulator seed.
    #[must_use]
    pub const fn seed(self) -> SimSeed {
        self.seed
    }

    /// Returns the configured step count.
    #[must_use]
    pub const fn steps(self) -> usize {
        self.steps
    }

    /// Returns whether bounded read-barrier progress is required.
    #[must_use]
    pub const fn checks_read_progress(self) -> bool {
        self.max_read_indexes > 0
    }

    /// Returns whether bounded membership-transition progress is required.
    #[must_use]
    pub const fn checks_membership_progress(self) -> bool {
        self.max_membership_changes > 0
    }

    /// Returns whether bounded leadership-transfer progress is required.
    #[must_use]
    pub const fn checks_transfer_progress(self) -> bool {
        self.max_transfers > 0
    }

    /// Returns whether bounded snapshot catch-up progress is required.
    #[must_use]
    pub const fn checks_snapshot_progress(self) -> bool {
        self.snapshot_catchup_probe
    }
}
