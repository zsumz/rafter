/// Summary for a successful bounded model-checking run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Summary {
    pub(in crate::model_check) explored_states: usize,
    pub(in crate::model_check) unique_states: usize,
    pub(in crate::model_check) unique_protocol_states: usize,
    pub(in crate::model_check) explored_actions: usize,
    pub(in crate::model_check) max_depth: usize,
}

impl Summary {
    /// Returns the number of recursive state visits, including duplicates
    /// pruned by canonical-state deduplication.
    #[must_use]
    pub const fn explored_states(self) -> usize {
        self.explored_states
    }

    /// Returns the number of distinct canonical states reached by the
    /// deduplicated search.
    #[must_use]
    pub const fn unique_states(self) -> usize {
        self.unique_states
    }

    /// Returns the number of distinct protocol states reached by the search,
    /// excluding verifier-only history retained to check temporal properties.
    #[must_use]
    pub const fn unique_protocol_states(self) -> usize {
        self.unique_protocol_states
    }

    /// Returns the number of verifier-inclusive canonical states used for
    /// deduplication, caps, and scheduled profile floors.
    #[must_use]
    pub const fn unique_verifier_states(self) -> usize {
        self.unique_states
    }

    /// Returns the number of actions applied while exploring the state space.
    #[must_use]
    pub const fn explored_actions(self) -> usize {
        self.explored_actions
    }

    /// Returns the maximum configured action depth.
    #[must_use]
    pub const fn max_depth(self) -> usize {
        self.max_depth
    }

    pub(in crate::model_check) const fn combined(self, other: Self) -> Self {
        Self {
            explored_states: self.explored_states + other.explored_states,
            unique_states: self.unique_states + other.unique_states,
            unique_protocol_states: self.unique_protocol_states + other.unique_protocol_states,
            explored_actions: self.explored_actions + other.explored_actions,
            max_depth: if self.max_depth > other.max_depth {
                self.max_depth
            } else {
                other.max_depth
            },
        }
    }
}
