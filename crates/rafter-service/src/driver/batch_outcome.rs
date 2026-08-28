#![allow(
    clippy::wildcard_imports,
    reason = "the driver is one state machine deliberately split across focused files, each opening the shared driver namespace"
)]

//! Reading one step's report against the entries of a batch.
//!
//! The report carries events for whichever proposals moved, and the batch is
//! ordered by local proposal ID, so each event finds its entry by search rather
//! than by a scan per entry. What an entry has observed — an append, a terminal
//! outcome, nothing yet — is the only thing that decides the fate it is finally
//! reported with.

use super::batch::BatchWriteState;
use super::*;

pub(super) fn observe_batch_report<G, R>(
    states: &mut [BatchWriteState<R>],
    report: &GroupStepReport<G, R>,
) where
    R: Clone,
{
    debug_assert!(states
        .windows(2)
        .all(|window| window[0].local_proposal_id < window[1].local_proposal_id));

    for event in &report.proposal_events {
        let Some(local_proposal_id) = proposal_event_local_id(event) else {
            continue;
        };
        let Ok(position) =
            states.binary_search_by_key(&local_proposal_id, |state| state.local_proposal_id)
        else {
            continue;
        };
        let state = &mut states[position];
        if state.outcome.is_some() {
            continue;
        }

        match event {
            ProposalEvent::Appended { .. } => {
                state.saw_local_append = true;
            }
            ProposalEvent::Applied {
                index,
                term,
                result,
                ..
            } => {
                state.outcome = Some(Ok(WriteReceipt {
                    index: *index,
                    term: *term,
                    result: result.clone(),
                }));
            }
            ProposalEvent::Rejected {
                reason,
                leader_hint,
                ..
            } => {
                state.outcome = Some(Err(write_error_from_rejection(
                    reason.clone(),
                    *leader_hint,
                )));
            }
            ProposalEvent::UnknownOutcome {
                client_request_id,
                reason,
                ..
            } => {
                state.outcome = Some(Err(WriteError::UnknownOutcome {
                    local_proposal_id,
                    client_request_id: client_request_id.or(state.options.client_request_id),
                    reason: managed_unknown_reason_from_app(reason),
                }));
            }
            _ => {}
        }
    }
}

fn proposal_event_local_id<R>(event: &ProposalEvent<R>) -> Option<LocalProposalId> {
    match event {
        ProposalEvent::Appended {
            local_proposal_id, ..
        }
        | ProposalEvent::Applied {
            local_proposal_id, ..
        }
        | ProposalEvent::Rejected {
            local_proposal_id, ..
        }
        | ProposalEvent::UnknownOutcome {
            local_proposal_id, ..
        } => Some(*local_proposal_id),
        _ => None,
    }
}

/// Restamps a mapped error with the fate the driver observed for one entry.
///
/// The category is a property of the failure and is shared across the batch;
/// the fate is a property of the entry. Variants whose fate follows from the
/// variant alone are left as the mapper produced them.
pub(super) fn with_observed_fate(error: &WriteError, saw_local_append: bool) -> WriteError {
    let observed = if saw_local_append {
        WriteFate::Unresolved
    } else {
        WriteFate::NotAppended
    };
    let mut error = error.clone();
    match &mut error {
        WriteError::StateMachine { fate, .. }
        | WriteError::Storage { fate, .. }
        | WriteError::Transport { fate, .. }
        | WriteError::Poisoned { fate, .. } => *fate = observed,
        _ => {}
    }
    error
}

pub(super) fn write_batch_complete<R>(states: &[BatchWriteState<R>]) -> bool {
    states.iter().all(|state| state.outcome.is_some())
}

pub(super) fn complete_unresolved_writes<R>(
    states: &mut [BatchWriteState<R>],
    error_for: impl Fn(&BatchWriteState<R>) -> WriteError,
) {
    for state in states {
        if state.outcome.is_none() {
            state.outcome = Some(Err(error_for(state)));
        }
    }
}

pub(super) fn finish_write_batch<R>(
    states: Vec<BatchWriteState<R>>,
) -> Vec<Result<WriteReceipt<R>, WriteError>> {
    states
        .into_iter()
        .map(|state| match state.outcome {
            Some(outcome) => outcome,
            None => Err(WriteError::ManagedInvariantViolation {
                fate: WriteFate::Unresolved,
                message: "managed write batch finished without an outcome".to_owned(),
            }),
        })
        .collect()
}

pub(super) fn repeat_write_error<R>(
    count: usize,
    error: &WriteError,
) -> Vec<Result<WriteReceipt<R>, WriteError>> {
    (0..count).map(|_| Err(error.clone())).collect()
}
