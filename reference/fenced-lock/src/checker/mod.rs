//! Black-box linearizability checker over fenced-lock histories.
//!
//! The checker sees only client-visible invocation and terminal events. It
//! never reads replicas, logs, applied indexes, membership, or adapter state.
//! Its sequential specification is [`crate::ReferenceLockService`], whose
//! transition code is structurally independent from [`crate::LockService`].
//!
//! Search is a bounded Wing–Gong-style backtracking over operations minimal in
//! the recorded real-time order. Unknown mutations branch into both legal
//! fates, provably uncommitted mutations and unanswered queries are discharged,
//! and failed `(unplaced operations, oracle state)` configurations are
//! memoized. Hitting either bound is undecided and therefore an error, never a
//! green result.

mod error;
mod parse;

pub use error::{Blocked, BlockedReason, CheckError, CheckReport, HistoryDefect, Violation};

use rafter_reference_harness::{
    search, CandidateReason, SearchError, SearchLimits, SequentialSpec, Step,
};

use crate::{
    HistoryEvent, LockConfig, LockQuery, LockQueryResult, ReferenceLockService, ServiceView,
};

use parse::Action;

/// Maximum number of operations one checked history may require the search to
/// place.
///
/// Provably uncommitted mutations and unanswered queries are discharged before
/// this bound is applied.
pub const MAX_HISTORY_OPERATIONS: usize = 24;

/// Default maximum number of search configurations one check may visit.
pub const MAX_SEARCH_CONFIGURATIONS: usize = 200_000;

/// The unplaced-operation set is a bit set, so the history bound must fit it.
const _: () = assert!(MAX_HISTORY_OPERATIONS <= u32::BITS as usize);

/// Explicit configuration budget for one bounded search.
///
/// A smaller budget is useful when a caller prefers an early undecided result
/// to additional search work. Zero is rejected because it would not inspect a
/// single nonempty configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SearchBudget {
    limits: SearchLimits,
}

impl SearchBudget {
    /// Creates a nonzero configuration budget.
    #[must_use]
    pub const fn new(configurations: usize) -> Option<Self> {
        match SearchLimits::new(MAX_HISTORY_OPERATIONS, configurations) {
            Some(limits) => Some(Self { limits }),
            None => None,
        }
    }

    /// Returns the maximum configurations this budget permits.
    #[must_use]
    pub const fn configurations(self) -> usize {
        self.limits.configurations()
    }
}

const DEFAULT_SEARCH_BUDGET: SearchBudget = match SearchBudget::new(MAX_SEARCH_CONFIGURATIONS) {
    Some(budget) => budget,
    None => panic!("the reviewed limits must fit the shared engine"),
};

/// Checks that a recorded history admits a legal real-time ordering.
///
/// `config` must contain the same client and resource bounds as the observed
/// service.
///
/// # Errors
///
/// Returns [`CheckError::NotLinearizable`] with replayable evidence when no
/// ordering explains the history, [`CheckError::Malformed`] when the recorder
/// produced incoherent intervals, and an undecided error when a bound is
/// reached.
pub fn check_linearizable(
    config: LockConfig,
    history: &[HistoryEvent],
) -> Result<CheckReport, CheckError> {
    check_linearizable_with_budget(config, history, DEFAULT_SEARCH_BUDGET)
}

/// Checks a history under an explicit search budget.
///
/// This has the same semantics as [`check_linearizable`]. Only the maximum
/// number of visited configurations differs.
///
/// # Errors
///
/// Returns the same errors as [`check_linearizable`], including
/// [`CheckError::BudgetExhausted`] when `budget` is consumed.
pub fn check_linearizable_with_budget(
    config: LockConfig,
    history: &[HistoryEvent],
    budget: SearchBudget,
) -> Result<CheckReport, CheckError> {
    let parsed = parse::parse(history)?;
    match search(
        &parsed.operations,
        parsed.discharged,
        &ReferenceLockService::new(config),
        &LockSpecification,
        budget.limits,
    ) {
        Ok(report) => Ok(CheckReport::new(
            report.searched_operations(),
            report.discharged_operations(),
            report.configurations(),
        )),
        Err(SearchError::TooManyOperations { operations, bound }) => {
            Err(CheckError::HistoryTooLong { operations, bound })
        }
        Err(SearchError::BudgetExhausted {
            configurations,
            bound,
        }) => Err(CheckError::BudgetExhausted {
            configurations,
            bound,
        }),
        Err(SearchError::NoOrder(frontier)) => {
            let (placed, candidates) = frontier.into_parts();
            let blocked = candidates
                .into_iter()
                .map(|candidate| Blocked {
                    operation_id: candidate.operation_id,
                    reason: match candidate.reason {
                        CandidateReason::Mismatch(reason) => reason,
                        CandidateReason::NoContinuation => BlockedReason::NoContinuation,
                    },
                })
                .collect();
            Err(CheckError::NotLinearizable(Violation::new(
                history.to_vec(),
                placed,
                blocked,
            )))
        }
    }
}

struct LockSpecification;

impl SequentialSpec<Action> for LockSpecification {
    type State = ReferenceLockService;
    type Key = ServiceView;
    type Mismatch = BlockedReason;

    fn key(&self, state: &Self::State) -> Self::Key {
        state.view()
    }

    fn step(&self, state: &Self::State, action: &Action) -> Step<Self::State, Self::Mismatch> {
        match *action {
            Action::Mutation { command, outcome } => {
                let mut next = state.clone();
                let specified = next.apply(command);
                if specified == outcome {
                    Step::Next(next)
                } else {
                    Step::Impossible(BlockedReason::OutcomeMismatch {
                        expected: specified,
                        observed: outcome,
                    })
                }
            }
            Action::UnknownMutation { command } => {
                let mut applied = state.clone();
                applied.apply(command);
                Step::Choice {
                    first: applied,
                    second: state.clone(),
                }
            }
            Action::Query { query, result } => {
                let specified = match query {
                    LockQuery::GetLock { resource } => {
                        LockQueryResult::Lock(state.status(resource))
                    }
                };
                if specified == result {
                    Step::Next(state.clone())
                } else {
                    Step::Impossible(BlockedReason::QueryMismatch {
                        expected: specified,
                        observed: result,
                    })
                }
            }
        }
    }
}
