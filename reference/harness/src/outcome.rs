use crate::OperationId;

/// What one successful bounded search covered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SearchReport {
    searched_operations: usize,
    discharged_operations: usize,
    configurations: usize,
}

impl SearchReport {
    pub(crate) const fn new(
        searched_operations: usize,
        discharged_operations: usize,
        configurations: usize,
    ) -> Self {
        Self {
            searched_operations,
            discharged_operations,
            configurations,
        }
    }

    /// Returns how many operations the search placed.
    #[must_use]
    pub const fn searched_operations(self) -> usize {
        self.searched_operations
    }

    /// Returns how many operations the caller discharged before searching.
    #[must_use]
    pub const fn discharged_operations(self) -> usize {
        self.discharged_operations
    }

    /// Returns how many configurations the search visited.
    #[must_use]
    pub const fn configurations(self) -> usize {
        self.configurations
    }
}

/// Why the bounded engine did not produce a legal ordering.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SearchError<M> {
    /// More operations need placement than the configured search admits.
    TooManyOperations { operations: usize, bound: usize },
    /// The configured visit budget ended before a decision.
    BudgetExhausted { configurations: usize, bound: usize },
    /// Every real-time-respecting ordering failed.
    NoOrder(SearchFrontier<M>),
}

/// Deepest failed search position retained for caller-owned diagnostics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SearchFrontier<M> {
    placed: Vec<OperationId>,
    candidates: Vec<Candidate<M>>,
}

impl<M> SearchFrontier<M> {
    pub(crate) const fn new(placed: Vec<OperationId>, candidates: Vec<Candidate<M>>) -> Self {
        Self { placed, candidates }
    }

    /// Returns the deepest successfully placed prefix.
    #[must_use]
    pub fn placed(&self) -> &[OperationId] {
        &self.placed
    }

    /// Returns every candidate that failed at the retained position.
    #[must_use]
    pub fn candidates(&self) -> &[Candidate<M>] {
        &self.candidates
    }

    /// Consumes the position into its caller-owned diagnostic parts.
    #[must_use]
    pub fn into_parts(self) -> (Vec<OperationId>, Vec<Candidate<M>>) {
        (self.placed, self.candidates)
    }
}

/// One candidate and why it failed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Candidate<M> {
    /// Candidate operation.
    pub operation_id: OperationId,
    /// Failure at this search position.
    pub reason: CandidateReason<M>,
}

/// Why one candidate did not yield a legal ordering.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CandidateReason<M> {
    /// The caller's sequential semantics contradicted the observation.
    Mismatch(M),
    /// The action was legal here, but every continuation failed.
    NoContinuation,
}
