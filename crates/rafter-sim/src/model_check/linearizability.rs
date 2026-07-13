use std::collections::BTreeSet;

use rafter::SharedPayload;

use super::catalog;
use super::state::{ClientHistory, ClientReadOutcome, ClientWriteStatus};
use super::ProposalId;

pub(super) const CLIENT_HISTORY_LINEARIZABILITY_INVARIANT: &str =
    catalog::RD_06_CLIENT_HISTORY_LINEARIZABILITY;

pub(super) fn check_client_history_linearizable(history: &ClientHistory) -> Result<(), String> {
    if let Some(error) = history.instrumentation_errors.iter().next() {
        return Err(format!("client-history instrumentation failed: {error}"));
    }
    if let Some(error) = history.read_instrumentation_errors.iter().next() {
        return Err(format!("client-read instrumentation failed: {error}"));
    }
    let operations = observed_operations(history);
    if operations.is_empty() || is_linearizable(history.initial_value.as_ref(), &operations) {
        return Ok(());
    }

    Err(format!(
        "observed client history is not linearizable for single-register model: {}",
        describe_operations(&operations)
    ))
}

fn observed_operations(history: &ClientHistory) -> Vec<Operation> {
    let mut operations = Vec::new();
    for write in history.writes.values() {
        let (completed_at, optional) = match write.status {
            ClientWriteStatus::Completed { completed_at, .. } => (completed_at, false),
            ClientWriteStatus::Unknown { .. } => (u64::MAX, true),
            ClientWriteStatus::Pending
            | ClientWriteStatus::Accepted { .. }
            | ClientWriteStatus::Rejected => continue,
        };
        operations.push(Operation {
            id: OperationId::Write(write.proposal_id),
            started_at: write.started_at,
            completed_at,
            optional,
            kind: OperationKind::Write {
                value: write.payload.clone(),
            },
        });
    }

    for read in history.reads.values() {
        let ClientReadOutcome::Completed {
            result,
            completed_at,
            ..
        } = &read.outcome
        else {
            continue;
        };
        operations.push(Operation {
            id: OperationId::Read(read.operation_id),
            started_at: read.started_at,
            completed_at: *completed_at,
            optional: false,
            kind: OperationKind::Read {
                result: result.clone(),
            },
        });
    }

    operations
        .sort_by_key(|operation| (operation.started_at, operation.completed_at, operation.id));
    operations
}

fn is_linearizable(initial_value: Option<&SharedPayload>, operations: &[Operation]) -> bool {
    let predecessors = predecessors(operations);
    let mut placed = vec![false; operations.len()];
    let mut dead_ends = BTreeSet::new();
    search(
        operations,
        &predecessors,
        &mut placed,
        initial_value,
        &mut dead_ends,
    )
}

fn predecessors(operations: &[Operation]) -> Vec<Vec<usize>> {
    let mut predecessors = vec![Vec::new(); operations.len()];
    for (left_index, left) in operations.iter().enumerate() {
        for (right_index, right) in operations.iter().enumerate() {
            if left_index != right_index && left.completed_at <= right.started_at {
                predecessors[right_index].push(left_index);
            }
        }
    }
    predecessors
}

fn search(
    operations: &[Operation],
    predecessors: &[Vec<usize>],
    placed: &mut [bool],
    value: Option<&SharedPayload>,
    dead_ends: &mut BTreeSet<SearchKey>,
) -> bool {
    if placed.iter().all(|placed| *placed) {
        return true;
    }

    let key = SearchKey {
        placed: placed.to_vec(),
        value: value.cloned(),
    };
    if !dead_ends.insert(key) {
        return false;
    }

    for index in enabled_operation_indexes(predecessors, placed) {
        if operations[index].optional {
            placed[index] = true;
            if search(operations, predecessors, placed, value, dead_ends) {
                return true;
            }
            placed[index] = false;
        }
        let ApplyOutcome::Accepted(next_value) = operations[index].kind.apply(value) else {
            continue;
        };
        placed[index] = true;
        if search(
            operations,
            predecessors,
            placed,
            next_value.as_ref(),
            dead_ends,
        ) {
            return true;
        }
        placed[index] = false;
    }

    false
}

fn enabled_operation_indexes(predecessors: &[Vec<usize>], placed: &[bool]) -> Vec<usize> {
    placed
        .iter()
        .enumerate()
        .filter_map(|(index, is_placed)| {
            if *is_placed || predecessors[index].iter().any(|before| !placed[*before]) {
                None
            } else {
                Some(index)
            }
        })
        .collect()
}

fn describe_operations(operations: &[Operation]) -> String {
    operations
        .iter()
        .map(Operation::describe)
        .collect::<Vec<_>>()
        .join("; ")
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SearchKey {
    placed: Vec<bool>,
    value: Option<SharedPayload>,
}

#[derive(Clone, Debug)]
struct Operation {
    id: OperationId,
    started_at: u64,
    completed_at: u64,
    optional: bool,
    kind: OperationKind,
}

impl Operation {
    fn describe(&self) -> String {
        match &self.kind {
            OperationKind::Write { value } => format!(
                "{}write {} start={} end={} value={}",
                if self.optional { "optional " } else { "" },
                self.id.label(),
                self.started_at,
                if self.optional {
                    "unknown".to_owned()
                } else {
                    self.completed_at.to_string()
                },
                format_payload(Some(value))
            ),
            OperationKind::Read { result } => format!(
                "read {} start={} end={} result={}",
                self.id.label(),
                self.started_at,
                self.completed_at,
                format_payload(result.as_ref())
            ),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum OperationId {
    Write(ProposalId),
    Read(u64),
}

impl OperationId {
    const fn label(self) -> u64 {
        match self {
            Self::Write(proposal_id) => proposal_id.0,
            Self::Read(operation_id) => operation_id,
        }
    }
}

#[derive(Clone, Debug)]
enum OperationKind {
    Write { value: SharedPayload },
    Read { result: Option<SharedPayload> },
}

enum ApplyOutcome {
    Accepted(Option<SharedPayload>),
    Rejected,
}

impl OperationKind {
    fn apply(&self, value: Option<&SharedPayload>) -> ApplyOutcome {
        match self {
            Self::Write { value } => ApplyOutcome::Accepted(Some(value.clone())),
            Self::Read { result } if result.as_ref() == value => {
                ApplyOutcome::Accepted(value.cloned())
            }
            Self::Read { .. } => ApplyOutcome::Rejected,
        }
    }
}

fn format_payload(payload: Option<&SharedPayload>) -> String {
    let Some(payload) = payload else {
        return "None".to_owned();
    };
    String::from_utf8_lossy(payload.as_slice()).into_owned()
}
