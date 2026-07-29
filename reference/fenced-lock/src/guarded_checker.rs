use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt,
};

use crate::{
    FencingToken, GuardedHistoryEvent, GuardedRejection, GuardedWrite, OperationId, ResourceName,
};

/// What one successful guarded-resource check covered.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GuardedCheckReport {
    checked_operations: usize,
    resources: usize,
}

impl GuardedCheckReport {
    /// Returns how many complete guarded operations were checked.
    #[must_use]
    pub const fn checked_operations(self) -> usize {
        self.checked_operations
    }

    /// Returns how many distinct guarded resources were checked.
    #[must_use]
    pub const fn resources(self) -> usize {
        self.resources
    }
}

/// Why a guarded-resource history could not be checked.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GuardedCheckError {
    /// The recorder produced incoherent operation intervals.
    Malformed(GuardedHistoryDefect),
    /// A terminal result contradicts fencing semantics.
    Violation(GuardedViolation),
}

/// A guarded history that does not describe complete operation intervals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GuardedHistoryDefect {
    /// One operation identity was invoked more than once.
    RepeatedInvocation { operation_id: OperationId },
    /// A completion names an operation never invoked.
    TerminalWithoutInvocation { operation_id: OperationId },
    /// One operation reached more than one completion.
    RepeatedTerminal { operation_id: OperationId },
    /// An invocation never received a completion.
    UnterminatedOperation { operation_id: OperationId },
}

/// Replayable evidence that one guarded result violated fencing semantics.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GuardedViolation {
    history: Vec<GuardedHistoryEvent>,
    checked_prefix: Vec<OperationId>,
    operation_id: OperationId,
    expected: Result<u64, GuardedRejection>,
    observed: Result<u64, GuardedRejection>,
}

impl GuardedViolation {
    /// Returns the exact guarded history that failed.
    #[must_use]
    pub fn history(&self) -> &[GuardedHistoryEvent] {
        &self.history
    }

    /// Returns operations successfully checked before the violation.
    #[must_use]
    pub fn checked_prefix(&self) -> &[OperationId] {
        &self.checked_prefix
    }

    /// Returns the operation whose result contradicted the guard model.
    #[must_use]
    pub const fn operation_id(&self) -> OperationId {
        self.operation_id
    }

    /// Returns the result required by the guard model.
    pub const fn expected(&self) -> &Result<u64, GuardedRejection> {
        &self.expected
    }

    /// Returns the result recorded by the caller.
    pub const fn observed(&self) -> &Result<u64, GuardedRejection> {
        &self.observed
    }
}

/// Checks exact guarded-resource outcomes in their recorded completion order.
///
/// Each protected resource has an independent token floor. Equal-token retries
/// remain legal, strictly older tokens are refused after a later acceptance,
/// and a write naming another resource receives `WrongResource`.
///
/// # Errors
///
/// Returns [`GuardedCheckError::Malformed`] for incoherent intervals and
/// [`GuardedCheckError::Violation`] with replayable evidence for an impossible
/// result.
pub fn check_guarded_history(
    history: &[GuardedHistoryEvent],
) -> Result<GuardedCheckReport, GuardedCheckError> {
    let operations = parse(history)?;
    let mut highest_by_resource = BTreeMap::<ResourceName, FencingToken>::new();
    let mut checked_resources = BTreeSet::new();
    let mut checked_prefix = Vec::with_capacity(operations.len());

    for operation in operations {
        checked_resources.insert(operation.guarded_resource);
        let expected = expected_result(
            operation.guarded_resource,
            operation.write,
            &highest_by_resource,
        );
        if expected != operation.result {
            return Err(GuardedCheckError::Violation(GuardedViolation {
                history: history.to_vec(),
                checked_prefix,
                operation_id: operation.operation_id,
                expected,
                observed: operation.result,
            }));
        }
        if operation.write.resource == operation.guarded_resource && operation.result.is_ok() {
            highest_by_resource
                .entry(operation.guarded_resource)
                .and_modify(|highest| *highest = (*highest).max(operation.write.token))
                .or_insert(operation.write.token);
        }
        checked_prefix.push(operation.operation_id);
    }

    Ok(GuardedCheckReport {
        checked_operations: checked_prefix.len(),
        resources: checked_resources.len(),
    })
}

fn expected_result(
    guarded_resource: ResourceName,
    write: GuardedWrite,
    highest_by_resource: &BTreeMap<ResourceName, FencingToken>,
) -> Result<u64, GuardedRejection> {
    if write.resource != guarded_resource {
        return Err(GuardedRejection::WrongResource);
    }
    if let Some(highest_accepted) = highest_by_resource.get(&guarded_resource) {
        if write.token < *highest_accepted {
            return Err(GuardedRejection::StaleFencingToken {
                highest_accepted: *highest_accepted,
            });
        }
    }
    Ok(write.value)
}

#[derive(Clone, Copy)]
struct GuardedOperation {
    operation_id: OperationId,
    guarded_resource: ResourceName,
    write: GuardedWrite,
    result: Result<u64, GuardedRejection>,
    completed_at: usize,
}

fn parse(history: &[GuardedHistoryEvent]) -> Result<Vec<GuardedOperation>, GuardedCheckError> {
    let mut invocations = BTreeMap::new();
    let mut completions = BTreeMap::new();
    let mut invoked_ids = BTreeSet::new();

    for (position, event) in history.iter().enumerate() {
        match *event {
            GuardedHistoryEvent::Invoked {
                operation_id,
                guarded_resource,
                write,
            } => {
                if !invoked_ids.insert(operation_id) {
                    return Err(GuardedCheckError::Malformed(
                        GuardedHistoryDefect::RepeatedInvocation { operation_id },
                    ));
                }
                invocations.insert(operation_id, (guarded_resource, write));
            }
            GuardedHistoryEvent::Completed {
                operation_id,
                result,
            } => {
                if !invoked_ids.contains(&operation_id) {
                    return Err(GuardedCheckError::Malformed(
                        GuardedHistoryDefect::TerminalWithoutInvocation { operation_id },
                    ));
                }
                if completions
                    .insert(operation_id, (position, result))
                    .is_some()
                {
                    return Err(GuardedCheckError::Malformed(
                        GuardedHistoryDefect::RepeatedTerminal { operation_id },
                    ));
                }
            }
        }
    }

    let mut operations = Vec::with_capacity(invocations.len());
    for (operation_id, (guarded_resource, write)) in invocations {
        let Some((completed_at, result)) = completions.get(&operation_id) else {
            return Err(GuardedCheckError::Malformed(
                GuardedHistoryDefect::UnterminatedOperation { operation_id },
            ));
        };
        operations.push(GuardedOperation {
            operation_id,
            guarded_resource,
            write,
            result: *result,
            completed_at: *completed_at,
        });
    }
    operations.sort_by_key(|operation| operation.completed_at);
    Ok(operations)
}

impl fmt::Display for GuardedCheckError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(defect) => {
                write!(formatter, "the guarded history is malformed: {defect:?}")
            }
            Self::Violation(violation) => violation.fmt(formatter),
        }
    }
}

impl Error for GuardedCheckError {}

impl fmt::Display for GuardedViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(
            formatter,
            "guarded operation {} returned {:?}, but fencing semantics require {:?}",
            self.operation_id.get(),
            self.observed,
            self.expected
        )?;
        writeln!(formatter, "guarded history:")?;
        for (position, event) in self.history.iter().enumerate() {
            writeln!(formatter, "  {position:>3}: {event:?}")?;
        }
        write!(
            formatter,
            "checked prefix before failure: {:?}",
            self.checked_prefix
        )
    }
}
