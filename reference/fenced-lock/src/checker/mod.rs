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
mod search;

pub use error::{Blocked, BlockedReason, CheckError, CheckReport, HistoryDefect, Violation};

use crate::{HistoryEvent, LockConfig};

use search::{BudgetExhausted, Search};

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
    configurations: usize,
}

impl SearchBudget {
    /// Creates a nonzero configuration budget.
    #[must_use]
    pub const fn new(configurations: usize) -> Option<Self> {
        if configurations == 0 {
            None
        } else {
            Some(Self { configurations })
        }
    }

    /// Returns the maximum configurations this budget permits.
    #[must_use]
    pub const fn configurations(self) -> usize {
        self.configurations
    }
}

const DEFAULT_SEARCH_BUDGET: SearchBudget = SearchBudget {
    configurations: MAX_SEARCH_CONFIGURATIONS,
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
    if parsed.operations.len() > MAX_HISTORY_OPERATIONS {
        return Err(CheckError::HistoryTooLong {
            operations: parsed.operations.len(),
            bound: MAX_HISTORY_OPERATIONS,
        });
    }

    let all_unplaced = (1_u32 << parsed.operations.len()) - 1;
    let mut search = Search::new(&parsed.operations, budget);
    match search.explore(all_unplaced, &crate::ReferenceLockService::new(config)) {
        Ok(true) => Ok(CheckReport::new(
            parsed.operations.len(),
            parsed.discharged,
            search.configurations(),
        )),
        Ok(false) => Err(CheckError::NotLinearizable(Violation::new(
            history.to_vec(),
            search.deepest_placed().to_vec(),
            search.deepest_blocked().to_vec(),
        ))),
        Err(BudgetExhausted) => Err(CheckError::BudgetExhausted {
            configurations: search.configurations(),
            bound: budget.configurations(),
        }),
    }
}
