use std::time::Duration;

/// Bound settings for an in-repo model-checking run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Bounds {
    pub(in crate::model_check) depth: usize,
    pub(in crate::model_check) proposal_count: usize,
    pub(in crate::model_check) restart_count: usize,
    pub(in crate::model_check) read_index_count: usize,
    pub(in crate::model_check) membership_change_count: usize,
    pub(in crate::model_check) max_unique_states: Option<usize>,
    pub(in crate::model_check) max_wall_clock: Option<Duration>,
}

impl Bounds {
    /// Constructs a bounded exploration configuration.
    #[must_use]
    pub const fn new(max_depth: usize) -> Self {
        Self {
            depth: max_depth,
            proposal_count: 0,
            restart_count: 0,
            read_index_count: 0,
            membership_change_count: 0,
            max_unique_states: None,
            max_wall_clock: None,
        }
    }

    /// Allows the checker to inject up to `max_proposals` client proposals.
    #[must_use]
    pub const fn with_max_proposals(mut self, max_proposals: usize) -> Self {
        self.proposal_count = max_proposals;
        self
    }

    /// Allows the checker to restart up to `max_restarts` nodes.
    #[must_use]
    pub const fn with_max_restarts(mut self, max_restarts: usize) -> Self {
        self.restart_count = max_restarts;
        self
    }

    /// Allows the checker to register up to `max_read_indexes` read barriers.
    #[must_use]
    pub const fn with_max_read_indexes(mut self, max_read_indexes: usize) -> Self {
        self.read_index_count = max_read_indexes;
        self
    }

    /// Allows the checker to inject up to `max_membership_changes`
    /// membership proposals.
    #[must_use]
    pub const fn with_max_membership_changes(mut self, max_membership_changes: usize) -> Self {
        self.membership_change_count = max_membership_changes;
        self
    }

    /// Stops admitting new canonical states after `max_unique_states` have
    /// been reached. Already-seen states are still counted as raw visits, and
    /// may be re-expanded if reached with more depth remaining.
    #[must_use]
    pub const fn with_max_unique_states(mut self, max_unique_states: usize) -> Self {
        self.max_unique_states = Some(max_unique_states);
        self
    }

    /// Stops expanding new canonical states after the wall-clock budget
    /// elapses. The checker returns the partial summary collected so far.
    #[must_use]
    pub const fn with_max_wall_clock(mut self, max_wall_clock: Duration) -> Self {
        self.max_wall_clock = Some(max_wall_clock);
        self
    }

    /// Returns the maximum action depth explored from the initial state.
    #[must_use]
    pub const fn max_depth(self) -> usize {
        self.depth
    }

    /// Returns the maximum number of proposals the checker may inject.
    #[must_use]
    pub const fn max_proposals(self) -> usize {
        self.proposal_count
    }

    /// Returns the maximum number of restarts the checker may inject.
    #[must_use]
    pub const fn max_restarts(self) -> usize {
        self.restart_count
    }

    /// Returns the maximum number of membership changes the checker may
    /// inject.
    #[must_use]
    pub const fn max_membership_changes(self) -> usize {
        self.membership_change_count
    }

    /// Returns the configured unique-state budget, if any.
    #[must_use]
    pub const fn max_unique_states(self) -> Option<usize> {
        self.max_unique_states
    }

    /// Returns the configured wall-clock budget, if any.
    #[must_use]
    pub const fn max_wall_clock(self) -> Option<Duration> {
        self.max_wall_clock
    }
}
