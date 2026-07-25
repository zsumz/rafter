//! Black-box linearizability checker over recorded ledger histories.
//!
//! The checker reads a [`HistoryEvent`] sequence and decides whether the
//! operations it records admit a legal real-time ordering. It sees only what a
//! client saw: which operations were invoked, in what order they returned, and
//! what each one returned. It never inspects replicas, logs, applied indexes,
//! or the implementation model.
//!
//! # Independence
//!
//! The sequential specification is [`ReferenceLedger`], the structurally
//! independent oracle. The checker shares no transition, validation,
//! deduplication, or mutation code with [`crate::Ledger`], and it never calls
//! the adapter. A bug that lives in the implementation therefore cannot make
//! the checker agree with it.
//!
//! # Algorithm
//!
//! A Wing & Gong search over minimal operations. At each step the remaining
//! operations that no other remaining operation must precede are the
//! candidates; the search applies one to the specification, recurses, and
//! backtracks when a candidate's recorded answer disagrees with the
//! specification's or when no continuation exists. Real-time order is enforced
//! by the candidate rule: an operation that returned before another was invoked
//! is never offered after it.
//!
//! Two prunings keep that search tractable. A candidate whose recorded answer
//! the specification cannot produce at this point is rejected without
//! recursing, and every configuration that fails is memoized so a different
//! interleaving reaching the same configuration fails immediately. A
//! configuration is the pair of the still-unplaced operation set and the
//! specification state, keyed on [`crate::LedgerView`] because equal views are
//! indistinguishable to every later command and query.
//!
//! Worst-case work is exponential in the number of operations: without
//! memoization the search is `O(n!)`, and with it the search expands at most
//! one node per distinct (unplaced set, state) pair, each trying at most `n`
//! candidates. Concurrency width, not history length, drives the real cost —
//! a history whose operations never overlap has exactly one candidate per step
//! and costs `O(n)`. Both [`MAX_HISTORY_OPERATIONS`] and
//! [`MAX_SEARCH_CONFIGURATIONS`] are refusals rather than truncations: a
//! history the checker cannot decide is reported as undecided, never as
//! checked.

use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    error::Error,
    fmt,
};

use crate::{
    Command, HistoryEvent, LedgerConfig, LedgerQuery, LedgerQueryResult, LedgerResponse,
    LedgerView, OperationId, ReferenceLedger,
};

/// Maximum number of operations one checked history may require the search to
/// place.
///
/// Operations the checker can discharge without searching — a provably
/// uncommitted mutation, a query that returned nothing — do not count against
/// this bound.
pub const MAX_HISTORY_OPERATIONS: usize = 24;

/// Maximum number of search configurations one check may visit.
///
/// Memoized configurations count: they are work the search performed, and
/// bounding visits rather than expansions keeps the budget a bound on time.
pub const MAX_SEARCH_CONFIGURATIONS: usize = 200_000;

/// The unplaced-operation set is a bit set, so the history bound has to fit it.
const _: () = assert!(MAX_HISTORY_OPERATIONS <= u32::BITS as usize);

/// Checks that a recorded history admits a legal real-time ordering.
///
/// `config` must be the bounds the observed ledger ran under, because the
/// specification enforces the same account and client-slot limits.
///
/// # Errors
///
/// Returns [`CheckError::NotLinearizable`] with replayable evidence when no
/// ordering explains the history, [`CheckError::Malformed`] when the recorder
/// produced a history that is not well formed, and
/// [`CheckError::HistoryTooLong`] or [`CheckError::BudgetExhausted`] when the
/// history is beyond what this checker is willing to decide.
pub fn check_linearizable(
    config: LedgerConfig,
    history: &[HistoryEvent],
) -> Result<CheckReport, CheckError> {
    let parsed = parse(history)?;
    if parsed.operations.len() > MAX_HISTORY_OPERATIONS {
        return Err(CheckError::HistoryTooLong {
            operations: parsed.operations.len(),
            bound: MAX_HISTORY_OPERATIONS,
        });
    }

    // The bound checked above keeps the count strictly below `u32::BITS`, so
    // this shift is always defined.
    let all_unplaced = (1_u32 << parsed.operations.len()) - 1;
    let mut search = Search {
        operations: &parsed.operations,
        failed: HashSet::new(),
        configurations: 0,
        placed: Vec::new(),
        deepest: Frontier::default(),
    };
    match search.explore(all_unplaced, &ReferenceLedger::new(config)) {
        Ok(true) => Ok(CheckReport {
            checked_operations: parsed.operations.len(),
            discharged_operations: parsed.discharged,
            configurations: search.configurations,
        }),
        Ok(false) => Err(CheckError::NotLinearizable(Violation {
            history: history.to_vec(),
            placed: search.deepest.placed,
            blocked: search.deepest.blocked,
        })),
        Err(BudgetExhausted) => Err(CheckError::BudgetExhausted {
            configurations: search.configurations,
            bound: MAX_SEARCH_CONFIGURATIONS,
        }),
    }
}

/// What one successful check actually covered.
///
/// A green check over an empty history proves nothing, so the counts are part
/// of the result rather than a debugging aid.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CheckReport {
    checked_operations: usize,
    discharged_operations: usize,
    configurations: usize,
}

impl CheckReport {
    /// Returns how many operations the search had to place.
    #[must_use]
    pub const fn checked_operations(self) -> usize {
        self.checked_operations
    }

    /// Returns how many operations were discharged without searching because
    /// they provably neither took effect nor observed one.
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

/// Why a history could not be checked, or could not be explained.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CheckError {
    /// The recorder produced a history that is not well formed.
    Malformed(HistoryDefect),
    /// The history needs more operations placed than this checker will decide.
    HistoryTooLong { operations: usize, bound: usize },
    /// The search ran out of budget before deciding either way.
    BudgetExhausted { configurations: usize, bound: usize },
    /// No legal real-time ordering explains the history.
    NotLinearizable(Violation),
}

/// A history that does not describe a coherent set of client operations.
///
/// Every variant is a recorder bug rather than a property violation. The
/// checker refuses such a history instead of checking a weaker one, so a
/// regression in the recording path fails loudly rather than quietly removing
/// operations from the check.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryDefect {
    /// One operation identity was invoked more than once.
    RepeatedInvocation { operation_id: OperationId },
    /// A terminal event names an operation that was never invoked.
    TerminalWithoutInvocation { operation_id: OperationId },
    /// One operation reached a terminal event more than once.
    RepeatedTerminal { operation_id: OperationId },
    /// A mutation ended in a query's terminal event, or a query in a
    /// mutation's.
    MismatchedTerminal { operation_id: OperationId },
    /// An operation was invoked and never reached a terminal event.
    ///
    /// A client that is still waiting is representable — that is what
    /// [`HistoryEvent::Unknown`] and [`HistoryEvent::QueryAbandoned`] are for —
    /// so a missing terminal event means the recorder lost one.
    UnterminatedOperation { operation_id: OperationId },
}

/// Evidence that a history admits no legal ordering.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Violation {
    history: Vec<HistoryEvent>,
    placed: Vec<OperationId>,
    blocked: Vec<Blocked>,
}

impl Violation {
    /// Returns the exact history that failed, for replay.
    #[must_use]
    pub fn history(&self) -> &[HistoryEvent] {
        &self.history
    }

    /// Returns the longest prefix the search placed before it got stuck.
    #[must_use]
    pub fn placed(&self) -> &[OperationId] {
        &self.placed
    }

    /// Returns every candidate at that point and why each one failed.
    #[must_use]
    pub fn blocked(&self) -> &[Blocked] {
        &self.blocked
    }
}

/// One candidate operation the search could not place, and why.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Blocked {
    /// The candidate.
    pub operation_id: OperationId,
    /// Why placing it here did not work.
    pub reason: BlockedReason,
}

/// Why one candidate could not be placed.
///
/// `expected` always names what the sequential specification produces at that
/// point; `observed` always names what the client actually saw.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BlockedReason {
    /// The specification answers this command differently here.
    ResponseMismatch {
        expected: LedgerResponse,
        observed: LedgerResponse,
    },
    /// The specification answers this query differently here.
    QueryMismatch {
        expected: LedgerQueryResult,
        observed: LedgerQueryResult,
    },
    /// The candidate is legal here, but every ordering that starts with it
    /// fails later.
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
                "the history needs {operations} operations placed, above this checker's bound of {bound}; \
                 shorten the history rather than trusting an unchecked one"
            ),
            Self::BudgetExhausted {
                configurations,
                bound,
            } => write!(
                formatter,
                "the search visited {configurations} configurations without deciding, at its bound of {bound}; \
                 the history is undecided, not linearizable"
            ),
            Self::NotLinearizable(violation) => violation.fmt(formatter),
        }
    }
}

impl Error for CheckError {}

impl fmt::Display for Violation {
    /// Prints the whole history and the frontier that defeated it.
    ///
    /// The history is printed in full and in order because it is the only
    /// input needed to replay the failure, and a randomized workload that
    /// prints only its seed forces a rerun to see what happened.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            formatter,
            "no real-time ordering explains this history; the deepest ordering placed {} operation(s)",
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
            Self::ResponseMismatch { expected, observed } => write!(
                formatter,
                "the specification answers {expected:?} here, but the client observed {observed:?}"
            ),
            Self::QueryMismatch { expected, observed } => write!(
                formatter,
                "the specification answers {expected:?} here, but the client observed {observed:?}"
            ),
            Self::NoContinuation => {
                write!(formatter, "legal here, but no ordering after it works")
            }
        }
    }
}

/// One operation the search must place.
#[derive(Clone, Debug)]
struct Operation {
    id: OperationId,
    action: Action,
    /// Operations that returned before this one was invoked.
    ///
    /// Real time forces every one of them ahead of this operation, so this is
    /// the whole of the ordering constraint the history imposes.
    must_follow: u32,
}

/// What one placed operation does to the specification.
#[derive(Clone, Debug)]
enum Action {
    /// A command whose replicated response the client observed.
    Mutation {
        command: Command,
        response: LedgerResponse,
    },
    /// A command the client could not resolve either way.
    ///
    /// It may have taken effect or not, so the search must try both readings.
    UnknownMutation { command: Command },
    /// A query whose result the client observed.
    Query {
        query: LedgerQuery,
        result: LedgerQueryResult,
    },
}

struct Parsed {
    operations: Vec<Operation>,
    discharged: usize,
}

/// Marker that the search stopped at its configuration budget.
struct BudgetExhausted;

#[derive(Default)]
struct Frontier {
    placed: Vec<OperationId>,
    blocked: Vec<Blocked>,
}

struct Search<'a> {
    operations: &'a [Operation],
    /// Configurations already proven unexplainable.
    ///
    /// The set is only ever inserted into and probed, never iterated, so the
    /// standard hasher's per-process seed cannot make one check disagree with
    /// another over the same history.
    failed: HashSet<(u32, LedgerView)>,
    configurations: usize,
    /// Operations placed on the path currently being explored.
    placed: Vec<OperationId>,
    /// The deepest dead end reached so far, kept as failure evidence.
    deepest: Frontier,
}

impl Search<'_> {
    /// Returns whether `unplaced` can be ordered legally from `state`.
    fn explore(&mut self, unplaced: u32, state: &ReferenceLedger) -> Result<bool, BudgetExhausted> {
        if unplaced == 0 {
            return Ok(true);
        }
        self.configurations += 1;
        if self.configurations > MAX_SEARCH_CONFIGURATIONS {
            return Err(BudgetExhausted);
        }
        let configuration = (unplaced, state.view());
        if self.failed.contains(&configuration) {
            return Ok(false);
        }

        // Real-time precedence is a strict partial order, so a nonempty
        // unplaced set always has a minimal element and this loop always
        // attempts at least one candidate.
        let mut blocked = Vec::new();
        for index in self.candidates(unplaced) {
            let operation = &self.operations[index];
            let remainder = unplaced & !bit(index);
            let attempt = match &operation.action {
                Action::Mutation { command, response } => {
                    self.place_mutation(operation.id, command, response, remainder, state)?
                }
                Action::UnknownMutation { command } => {
                    self.place_unknown_mutation(operation.id, command, remainder, state)?
                }
                Action::Query { query, result } => {
                    self.place_query(operation.id, *query, *result, remainder, state)?
                }
            };
            match attempt {
                Attempt::Linearized => return Ok(true),
                Attempt::Blocked(reason) => blocked.push(Blocked {
                    operation_id: operation.id,
                    reason,
                }),
            }
        }

        // A configuration already known to fail returns above without coming
        // back here, so the recorded frontier can name a shallower path than
        // the deepest one actually reached. That costs evidence detail, never
        // soundness.
        if self.placed.len() >= self.deepest.placed.len() {
            self.deepest = Frontier {
                placed: self.placed.clone(),
                blocked,
            };
        }
        self.failed.insert(configuration);
        Ok(false)
    }

    fn place_mutation(
        &mut self,
        id: OperationId,
        command: &Command,
        response: &LedgerResponse,
        remainder: u32,
        state: &ReferenceLedger,
    ) -> Result<Attempt, BudgetExhausted> {
        let mut next = state.clone();
        let specified = next.apply(command.clone()).response;
        if specified != *response {
            return Ok(Attempt::Blocked(BlockedReason::ResponseMismatch {
                expected: specified,
                observed: response.clone(),
            }));
        }
        self.descend(id, remainder, &next)
    }

    /// Tries an unresolved command as taken-effect first, then as never-run.
    ///
    /// Both readings are legal for the client, so both must be explored before
    /// the operation is called a dead end. Trying taken-effect first is
    /// arbitrary; the search backtracks into the other reading whenever a later
    /// observation rules the first one out.
    fn place_unknown_mutation(
        &mut self,
        id: OperationId,
        command: &Command,
        remainder: u32,
        state: &ReferenceLedger,
    ) -> Result<Attempt, BudgetExhausted> {
        let mut applied = state.clone();
        applied.apply(command.clone());
        if matches!(self.descend(id, remainder, &applied)?, Attempt::Linearized) {
            return Ok(Attempt::Linearized);
        }
        self.descend(id, remainder, state)
    }

    fn place_query(
        &mut self,
        id: OperationId,
        query: LedgerQuery,
        result: LedgerQueryResult,
        remainder: u32,
        state: &ReferenceLedger,
    ) -> Result<Attempt, BudgetExhausted> {
        let specified = state.query(query);
        if specified != result {
            return Ok(Attempt::Blocked(BlockedReason::QueryMismatch {
                expected: specified,
                observed: result,
            }));
        }
        // A query leaves the specification where it was, so the continuation
        // starts from the same state.
        self.descend(id, remainder, state)
    }

    fn descend(
        &mut self,
        id: OperationId,
        remainder: u32,
        state: &ReferenceLedger,
    ) -> Result<Attempt, BudgetExhausted> {
        self.placed.push(id);
        let linearized = self.explore(remainder, state)?;
        self.placed.pop();
        Ok(if linearized {
            Attempt::Linearized
        } else {
            Attempt::Blocked(BlockedReason::NoContinuation)
        })
    }

    /// Returns the unplaced operations no other unplaced operation precedes.
    ///
    /// The result is collected rather than iterated lazily because the search
    /// mutates its own memo while descending.
    fn candidates(&self, unplaced: u32) -> Vec<usize> {
        (0..self.operations.len())
            .filter(|index| unplaced & bit(*index) != 0)
            .filter(|index| self.operations[*index].must_follow & unplaced == 0)
            .collect()
    }
}

/// Outcome of trying one candidate at one point in the search.
enum Attempt {
    Linearized,
    Blocked(BlockedReason),
}

const fn bit(index: usize) -> u32 {
    1_u32 << index
}

/// What one invocation event asked for.
#[derive(Clone, Debug)]
enum Invocation {
    Mutation(Command),
    Query(LedgerQuery),
}

/// What one terminal event told the client.
#[derive(Clone, Debug)]
enum Terminal {
    Response(LedgerResponse),
    Unknown,
    NotCommitted,
    QueryResult(LedgerQueryResult),
    QueryAbandoned,
}

/// Turns an event sequence into operations with their real-time constraints.
///
/// Two kinds of operation are discharged here instead of being searched. A
/// [`HistoryEvent::NotCommitted`] mutation provably never ran, and a
/// [`HistoryEvent::QueryAbandoned`] query provably returned nothing, so neither
/// changes the specification nor has an answer to explain. Removing them is
/// exact rather than merely sound: ordering constraints between the operations
/// that remain never pass through a removed one.
fn parse(history: &[HistoryEvent]) -> Result<Parsed, CheckError> {
    let mut invoked = Vec::new();
    let mut invoked_ids = BTreeSet::new();
    let mut terminals = BTreeMap::new();

    for (position, event) in history.iter().enumerate() {
        let operation_id = event.operation_id();
        let terminal = match event {
            HistoryEvent::Invoked { command, .. } => {
                record_invocation(
                    &mut invoked,
                    &mut invoked_ids,
                    operation_id,
                    Invocation::Mutation(command.clone()),
                    position,
                )?;
                continue;
            }
            HistoryEvent::QueryInvoked { query, .. } => {
                record_invocation(
                    &mut invoked,
                    &mut invoked_ids,
                    operation_id,
                    Invocation::Query(*query),
                    position,
                )?;
                continue;
            }
            HistoryEvent::Completed { response, .. } => Terminal::Response(response.clone()),
            HistoryEvent::Unknown { .. } => Terminal::Unknown,
            HistoryEvent::NotCommitted { .. } => Terminal::NotCommitted,
            HistoryEvent::QueryCompleted { result, .. } => Terminal::QueryResult(*result),
            HistoryEvent::QueryAbandoned { .. } => Terminal::QueryAbandoned,
        };

        if !invoked_ids.contains(&operation_id) {
            return Err(CheckError::Malformed(
                HistoryDefect::TerminalWithoutInvocation { operation_id },
            ));
        }
        if terminals
            .insert(operation_id, (position, terminal))
            .is_some()
        {
            return Err(CheckError::Malformed(HistoryDefect::RepeatedTerminal {
                operation_id,
            }));
        }
    }

    build_operations(&invoked, &terminals)
}

fn record_invocation(
    invoked: &mut Vec<(OperationId, Invocation, usize)>,
    invoked_ids: &mut BTreeSet<OperationId>,
    operation_id: OperationId,
    invocation: Invocation,
    position: usize,
) -> Result<(), CheckError> {
    if !invoked_ids.insert(operation_id) {
        return Err(CheckError::Malformed(HistoryDefect::RepeatedInvocation {
            operation_id,
        }));
    }
    invoked.push((operation_id, invocation, position));
    Ok(())
}

fn build_operations(
    invoked: &[(OperationId, Invocation, usize)],
    terminals: &BTreeMap<OperationId, (usize, Terminal)>,
) -> Result<Parsed, CheckError> {
    let mut searchable = Vec::new();
    let mut discharged = 0;
    for (operation_id, invocation, invoked_at) in invoked {
        let Some((returned_at, terminal)) = terminals.get(operation_id) else {
            return Err(CheckError::Malformed(
                HistoryDefect::UnterminatedOperation {
                    operation_id: *operation_id,
                },
            ));
        };
        match action_for(invocation, terminal, *operation_id)? {
            Some(action) => searchable.push((*operation_id, action, *invoked_at, *returned_at)),
            None => discharged += 1,
        }
    }

    let operations = searchable
        .iter()
        .map(|(operation_id, action, invoked_at, _)| Operation {
            id: *operation_id,
            action: action.clone(),
            must_follow: searchable
                .iter()
                .enumerate()
                .filter(|(_, (_, _, _, returned_at))| returned_at < invoked_at)
                .fold(0, |mask, (index, _)| mask | bit(index)),
        })
        .collect();
    Ok(Parsed {
        operations,
        discharged,
    })
}

/// Returns what the search must do with one operation, or `None` when the
/// operation is discharged without searching.
fn action_for(
    invocation: &Invocation,
    terminal: &Terminal,
    operation_id: OperationId,
) -> Result<Option<Action>, CheckError> {
    Ok(match (invocation, terminal) {
        (Invocation::Mutation(command), Terminal::Response(response)) => Some(Action::Mutation {
            command: command.clone(),
            response: response.clone(),
        }),
        (Invocation::Mutation(command), Terminal::Unknown) => Some(Action::UnknownMutation {
            command: command.clone(),
        }),
        (Invocation::Query(query), Terminal::QueryResult(result)) => Some(Action::Query {
            query: *query,
            result: *result,
        }),
        (Invocation::Mutation(_), Terminal::NotCommitted)
        | (Invocation::Query(_), Terminal::QueryAbandoned) => None,
        _ => {
            return Err(CheckError::Malformed(HistoryDefect::MismatchedTerminal {
                operation_id,
            }));
        }
    })
}
