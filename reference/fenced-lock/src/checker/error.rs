use std::{error::Error, fmt};

use crate::{ApplyOutcome, HistoryEvent, LockQueryResult, OperationId};

/// What one successful check actually covered.
///
/// A green empty history proves nothing, so searched and discharged counts are
/// part of the result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckReport {
    searched_operations: usize,
    discharged_operations: usize,
    configurations: usize,
}

impl CheckReport {
    pub(super) const fn new(
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

    /// Returns how many operations were discharged without searching.
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

/// Why a history could not be checked or explained.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckError {
    /// The recorder produced an incoherent history.
    Malformed(HistoryDefect),
    /// The history requires more searched operations than the checker admits.
    HistoryTooLong { operations: usize, bound: usize },
    /// The search stopped without a verdict at its explicit budget.
    BudgetExhausted { configurations: usize, bound: usize },
    /// No legal real-time ordering explains the history.
    NotLinearizable(Violation),
}

/// A client history that does not describe coherent operation intervals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryDefect {
    /// One operation identity was invoked more than once.
    RepeatedInvocation { operation_id: OperationId },
    /// A terminal event names an operation never invoked.
    TerminalWithoutInvocation { operation_id: OperationId },
    /// One operation reached more than one terminal event.
    RepeatedTerminal { operation_id: OperationId },
    /// A mutation ended as a query or a query ended as a mutation.
    MismatchedTerminal { operation_id: OperationId },
    /// An invocation never received a terminal event.
    UnterminatedOperation { operation_id: OperationId },
}

/// Replayable evidence that a history admits no legal ordering.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Violation {
    history: Vec<HistoryEvent>,
    placed: Vec<OperationId>,
    blocked: Vec<Blocked>,
}

impl Violation {
    pub(super) const fn new(
        history: Vec<HistoryEvent>,
        placed: Vec<OperationId>,
        blocked: Vec<Blocked>,
    ) -> Self {
        Self {
            history,
            placed,
            blocked,
        }
    }

    /// Returns the exact history that failed.
    #[must_use]
    pub fn history(&self) -> &[HistoryEvent] {
        &self.history
    }

    /// Returns the deepest operation prefix the search placed.
    #[must_use]
    pub fn placed(&self) -> &[OperationId] {
        &self.placed
    }

    /// Returns every candidate blocked at the deepest frontier.
    #[must_use]
    pub fn blocked(&self) -> &[Blocked] {
        &self.blocked
    }
}

/// One candidate operation the search could not place.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Blocked {
    /// Candidate operation.
    pub operation_id: OperationId,
    /// Why it could not lead to a legal continuation.
    pub reason: BlockedReason,
}

/// Why one candidate could not be placed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BlockedReason {
    /// The oracle produced another mutation outcome at this point.
    OutcomeMismatch {
        expected: ApplyOutcome,
        observed: ApplyOutcome,
    },
    /// The oracle produced another query result at this point.
    QueryMismatch {
        expected: LockQueryResult,
        observed: LockQueryResult,
    },
    /// The candidate was legal here but every continuation failed.
    NoContinuation,
}

impl fmt::Display for CheckError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(defect) => {
                write!(formatter, "the recorded history is malformed: {defect:?}")
            }
            Self::HistoryTooLong { operations, bound } => write!(
                formatter,
                "the history needs {operations} operations placed, above this checker's bound of \
                 {bound}; shorten the history rather than trusting an unchecked one"
            ),
            Self::BudgetExhausted {
                configurations,
                bound,
            } => write!(
                formatter,
                "the search visited {configurations} configurations without deciding, at its \
                 bound of {bound}; the history is undecided, not linearizable"
            ),
            Self::NotLinearizable(violation) => violation.fmt(formatter),
        }
    }
}

impl Error for CheckError {}

impl fmt::Display for Violation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            formatter,
            "no real-time ordering explains this history; the deepest ordering placed {} \
             operation(s)",
            self.placed.len()
        )?;
        writeln!(formatter, "history:")?;
        for (position, event) in self.history.iter().enumerate() {
            writeln!(formatter, "  {position:>3}: {event:?}")?;
        }
        writeln!(formatter, "placed in order: {:?}", self.placed)?;
        writeln!(formatter, "then every remaining candidate failed:")?;
        for blocked in &self.blocked {
            writeln!(
                formatter,
                "  operation {}: {}",
                blocked.operation_id.get(),
                blocked.reason
            )?;
        }
        Ok(())
    }
}

impl fmt::Display for BlockedReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::OutcomeMismatch { expected, observed } => write!(
                formatter,
                "the specification answers {expected:?} here, but the client observed \
                 {observed:?}"
            ),
            Self::QueryMismatch { expected, observed } => write!(
                formatter,
                "the specification answers {expected:?} here, but the client observed \
                 {observed:?}"
            ),
            Self::NoContinuation => {
                formatter.write_str("legal here, but no ordering after it works")
            }
        }
    }
}
