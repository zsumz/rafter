use super::super::observations::ObservationSet;

/// Summary for a successful bounded model-checking run.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Summary {
    pub(in crate::model_check) explored_states: usize,
    pub(in crate::model_check) unique_states: usize,
    pub(in crate::model_check) unique_protocol_states: usize,
    pub(in crate::model_check) explored_actions: usize,
    pub(in crate::model_check) configured_depth: usize,
    pub(in crate::model_check) reached_depth: usize,
    pub(in crate::model_check) completion: ExplorationCompletion,
    pub(in crate::model_check) observations: ObservationSet,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// Exhaustive reasons why a bounded state-space exploration stopped.
pub enum ExplorationCompletion {
    FrontierExhausted,
    UniqueStateLimit,
    WallClockLimit,
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
        self.configured_depth
    }

    /// Returns the deepest state actually admitted by the exploration.
    #[must_use]
    pub const fn reached_depth(self) -> usize {
        self.reached_depth
    }

    /// Returns whether the frontier closed or an exploration budget stopped it.
    #[must_use]
    pub const fn completion(self) -> ExplorationCompletion {
        self.completion
    }

    /// Returns the semantic detector branches exercised by this run.
    pub fn observation_labels(self) -> impl Iterator<Item = &'static str> {
        self.observations.labels()
    }

    pub(in crate::model_check) const fn combined(self, other: Self) -> Self {
        Self {
            explored_states: self.explored_states + other.explored_states,
            unique_states: self.unique_states + other.unique_states,
            unique_protocol_states: self.unique_protocol_states + other.unique_protocol_states,
            explored_actions: self.explored_actions + other.explored_actions,
            configured_depth: if self.configured_depth > other.configured_depth {
                self.configured_depth
            } else {
                other.configured_depth
            },
            reached_depth: if self.reached_depth > other.reached_depth {
                self.reached_depth
            } else {
                other.reached_depth
            },
            completion: self.completion.combined(other.completion),
            observations: {
                let mut observations = self.observations;
                observations.union_with(other.observations);
                observations
            },
        }
    }
}

impl ExplorationCompletion {
    const fn combined(self, other: Self) -> Self {
        match (self, other) {
            (Self::WallClockLimit, _) | (_, Self::WallClockLimit) => Self::WallClockLimit,
            (Self::UniqueStateLimit, _) | (_, Self::UniqueStateLimit) => Self::UniqueStateLimit,
            _ => Self::FrontierExhausted,
        }
    }
}

impl fmt::Display for ExplorationCompletion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::FrontierExhausted => "frontier_exhausted",
            Self::UniqueStateLimit => "unique_state_limit",
            Self::WallClockLimit => "wall_clock_limit",
        })
    }
}
use std::fmt;
