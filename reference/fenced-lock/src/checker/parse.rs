use std::collections::{BTreeMap, BTreeSet};

use rafter_reference_harness::Operation;

use crate::{ApplyOutcome, Command, HistoryEvent, LockQuery, LockQueryResult, OperationId};

use super::{CheckError, HistoryDefect};

/// What one placed operation does to the specification.
#[derive(Clone, Copy, Debug)]
pub(super) enum Action {
    Mutation {
        command: Command,
        outcome: ApplyOutcome,
    },
    UnknownMutation {
        command: Command,
    },
    Query {
        query: LockQuery,
        result: LockQueryResult,
    },
}

pub(super) struct Parsed {
    pub(super) operations: Vec<Operation<Action>>,
    pub(super) discharged: usize,
}

#[derive(Clone, Copy, Debug)]
enum Invocation {
    Mutation(Command),
    Query(LockQuery),
}

#[derive(Clone, Copy, Debug)]
enum Terminal {
    Outcome(ApplyOutcome),
    Unknown,
    NotCommitted,
    QueryResult(LockQueryResult),
    QueryAbandoned,
}

/// Turns client events into searched operations and real-time predecessors.
pub(super) fn parse(history: &[HistoryEvent]) -> Result<Parsed, CheckError> {
    let mut invoked = Vec::new();
    let mut invoked_ids = BTreeSet::new();
    let mut terminals = BTreeMap::new();

    for (position, event) in history.iter().enumerate() {
        let operation_id = event.operation_id();
        let terminal = match *event {
            HistoryEvent::Invoked { command, .. } => {
                record_invocation(
                    &mut invoked,
                    &mut invoked_ids,
                    operation_id,
                    Invocation::Mutation(command),
                    position,
                )?;
                continue;
            }
            HistoryEvent::QueryInvoked { query, .. } => {
                record_invocation(
                    &mut invoked,
                    &mut invoked_ids,
                    operation_id,
                    Invocation::Query(query),
                    position,
                )?;
                continue;
            }
            HistoryEvent::Completed { outcome, .. } => Terminal::Outcome(outcome),
            HistoryEvent::Unknown { .. } => Terminal::Unknown,
            HistoryEvent::NotCommitted { .. } => Terminal::NotCommitted,
            HistoryEvent::QueryCompleted { result, .. } => Terminal::QueryResult(result),
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
        match action_for(*invocation, *terminal, *operation_id)? {
            Some(action) => searchable.push((*operation_id, action, *invoked_at, *returned_at)),
            None => discharged += 1,
        }
    }

    let operations = searchable
        .iter()
        .map(|(operation_id, action, invoked_at, returned_at)| {
            Operation::new(*operation_id, *action, *invoked_at, *returned_at)
        })
        .collect();
    Ok(Parsed {
        operations,
        discharged,
    })
}

fn action_for(
    invocation: Invocation,
    terminal: Terminal,
    operation_id: OperationId,
) -> Result<Option<Action>, CheckError> {
    Ok(match (invocation, terminal) {
        (Invocation::Mutation(command), Terminal::Outcome(outcome)) => {
            Some(Action::Mutation { command, outcome })
        }
        (Invocation::Mutation(command), Terminal::Unknown) => {
            Some(Action::UnknownMutation { command })
        }
        (Invocation::Query(query), Terminal::QueryResult(result)) => {
            Some(Action::Query { query, result })
        }
        (Invocation::Mutation(_), Terminal::NotCommitted)
        | (Invocation::Query(_), Terminal::QueryAbandoned) => None,
        _ => {
            return Err(CheckError::Malformed(HistoryDefect::MismatchedTerminal {
                operation_id,
            }));
        }
    })
}
