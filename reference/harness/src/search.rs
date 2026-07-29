use std::collections::HashSet;

use crate::{
    Candidate, CandidateReason, Operation, OperationId, SearchError, SearchFrontier, SearchReport,
    SequentialSpec, Step,
};

const REPRESENTABLE_OPERATIONS: usize = u32::BITS as usize;

/// Explicit bounds for one ordering search.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SearchLimits {
    operations: usize,
    configurations: usize,
}

impl SearchLimits {
    /// Creates limits that fit the engine's representation and visit rules.
    ///
    /// Returns `None` for a zero configuration budget or an operation bound
    /// wider than the predecessor bit set.
    #[must_use]
    pub const fn new(operations: usize, configurations: usize) -> Option<Self> {
        if configurations == 0 || operations > REPRESENTABLE_OPERATIONS {
            None
        } else {
            Some(Self {
                operations,
                configurations,
            })
        }
    }

    /// Returns the maximum operations requiring placement.
    #[must_use]
    pub const fn operations(self) -> usize {
        self.operations
    }

    /// Returns the maximum configurations the search may visit.
    #[must_use]
    pub const fn configurations(self) -> usize {
        self.configurations
    }
}

/// Searches already-parsed intervals for a legal real-time ordering.
///
/// `discharged_operations` is caller-owned parse evidence for complete
/// operations that constrain neither state nor observations. It is reported
/// but never interpreted here.
///
/// # Errors
///
/// Returns [`SearchError::TooManyOperations`] before searching,
/// [`SearchError::BudgetExhausted`] when the configured visit bound is crossed,
/// and [`SearchError::NoOrder`] with the deepest retained position when every
/// ordering fails.
pub fn search<A, S>(
    operations: &[Operation<A>],
    discharged_operations: usize,
    initial_state: &S::State,
    specification: &S,
    limits: SearchLimits,
) -> Result<SearchReport, SearchError<S::Mismatch>>
where
    S: SequentialSpec<A>,
{
    if operations.len() > limits.operations {
        return Err(SearchError::TooManyOperations {
            operations: operations.len(),
            bound: limits.operations,
        });
    }

    let prepared = prepare(operations);
    let all_unplaced = low_bits(prepared.len());
    let mut engine = Engine {
        operations: &prepared,
        specification,
        limits,
        failed: HashSet::new(),
        configurations: 0,
        placed: Vec::new(),
        deepest: Position::default(),
    };
    match engine.explore(all_unplaced, initial_state) {
        Ok(true) => Ok(SearchReport::new(
            operations.len(),
            discharged_operations,
            engine.configurations,
        )),
        Ok(false) => Err(SearchError::NoOrder(SearchFrontier::new(
            engine.deepest.placed,
            engine.deepest.candidates,
        ))),
        Err(BudgetEnded) => Err(SearchError::BudgetExhausted {
            configurations: engine.configurations,
            bound: limits.configurations,
        }),
    }
}

struct Prepared<'a, A> {
    operation: &'a Operation<A>,
    must_follow: u32,
}

fn prepare<A>(operations: &[Operation<A>]) -> Vec<Prepared<'_, A>> {
    operations
        .iter()
        .map(|operation| Prepared {
            operation,
            must_follow: operations
                .iter()
                .enumerate()
                .filter(|(_, predecessor)| predecessor.returned_at() < operation.invoked_at())
                .fold(0, |mask, (index, _)| mask | bit(index)),
        })
        .collect()
}

struct BudgetEnded;

struct Position<M> {
    placed: Vec<OperationId>,
    candidates: Vec<Candidate<M>>,
}

impl<M> Default for Position<M> {
    fn default() -> Self {
        Self {
            placed: Vec::new(),
            candidates: Vec::new(),
        }
    }
}

struct Engine<'a, A, S>
where
    S: SequentialSpec<A>,
{
    operations: &'a [Prepared<'a, A>],
    specification: &'a S,
    limits: SearchLimits,
    failed: HashSet<(u32, S::Key)>,
    configurations: usize,
    placed: Vec<OperationId>,
    deepest: Position<S::Mismatch>,
}

impl<A, S> Engine<'_, A, S>
where
    S: SequentialSpec<A>,
{
    fn explore(&mut self, unplaced: u32, state: &S::State) -> Result<bool, BudgetEnded> {
        if unplaced == 0 {
            return Ok(true);
        }
        self.configurations += 1;
        if self.configurations > self.limits.configurations {
            return Err(BudgetEnded);
        }
        let configuration = (unplaced, self.specification.key(state));
        if self.failed.contains(&configuration) {
            return Ok(false);
        }

        let mut candidates = Vec::new();
        for index in self.candidates(unplaced) {
            let operation = self.operations[index].operation;
            let remainder = unplaced & !bit(index);
            let result = match self.specification.step(state, operation.action()) {
                Step::Impossible(reason) => {
                    CandidateResult::Failed(CandidateReason::Mismatch(reason))
                }
                Step::Next(next) => self.descend(operation.id(), remainder, &next)?,
                Step::Choice { first, second } => {
                    if matches!(
                        self.descend(operation.id(), remainder, &first)?,
                        CandidateResult::Ordered
                    ) {
                        return Ok(true);
                    }
                    self.descend(operation.id(), remainder, &second)?
                }
            };
            match result {
                CandidateResult::Ordered => return Ok(true),
                CandidateResult::Failed(reason) => candidates.push(Candidate {
                    operation_id: operation.id(),
                    reason,
                }),
            }
        }

        if self.placed.len() >= self.deepest.placed.len() {
            self.deepest = Position {
                placed: self.placed.clone(),
                candidates,
            };
        }
        self.failed.insert(configuration);
        Ok(false)
    }

    fn descend(
        &mut self,
        id: OperationId,
        remainder: u32,
        state: &S::State,
    ) -> Result<CandidateResult<S::Mismatch>, BudgetEnded> {
        self.placed.push(id);
        let result = self.explore(remainder, state);
        self.placed.pop();
        Ok(if result? {
            CandidateResult::Ordered
        } else {
            CandidateResult::Failed(CandidateReason::NoContinuation)
        })
    }

    fn candidates(&self, unplaced: u32) -> Vec<usize> {
        (0..self.operations.len())
            .filter(|index| unplaced & bit(*index) != 0)
            .filter(|index| self.operations[*index].must_follow & unplaced == 0)
            .collect()
    }
}

enum CandidateResult<M> {
    Ordered,
    Failed(CandidateReason<M>),
}

const fn bit(index: usize) -> u32 {
    1_u32 << index
}

const fn low_bits(count: usize) -> u32 {
    if count == REPRESENTABLE_OPERATIONS {
        u32::MAX
    } else {
        (1_u32 << count) - 1
    }
}
