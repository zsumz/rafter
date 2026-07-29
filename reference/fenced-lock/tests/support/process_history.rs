use rafter_reference_fenced_lock::{
    ApplyOutcome, ClientId, FencingToken, HistoryEvent, LockHolderView, LockQuery, LockQueryResult,
    LogicalTime, OperationId, ResourceName, ResourceStatus,
};

use super::{LockView, QueryOutcome, SubmitOutcome};

pub(super) const fn query_invocation(
    operation_id: OperationId,
    resource: ResourceName,
) -> HistoryEvent {
    HistoryEvent::QueryInvoked {
        operation_id,
        query: LockQuery::GetLock { resource },
    }
}

/// Converts the independently decoded wire view into the typed result retained
/// by the black-box history.
#[track_caller]
fn query_result(view: &LockView) -> LockQueryResult {
    let holder = match (view.owner, view.held_token, view.expiry) {
        (None, None, None) => None,
        (Some(owner), Some(token), Some(expiry)) => Some(LockHolderView {
            owner: ClientId::new(owner),
            token: FencingToken::new(token).expect("a held token is nonzero"),
            expiry: LogicalTime::new(expiry),
        }),
        fields => panic!("a lock holder has all three fields or none, observed {fields:?}"),
    };
    LockQueryResult::Lock(ResourceStatus {
        resource: ResourceName::new(&view.resource)
            .expect("the process returned a bounded resource name"),
        holder,
        token_floor: view
            .token_floor
            .map(|token| FencingToken::new(token).expect("a token floor is nonzero")),
        logical_time: LogicalTime::new(view.logical_time),
    })
}

pub(super) fn query_terminal(operation_id: OperationId, outcome: &QueryOutcome) -> HistoryEvent {
    match outcome {
        QueryOutcome::Ready(view) => HistoryEvent::QueryCompleted {
            operation_id,
            result: query_result(view),
        },
        QueryOutcome::Abandoned { .. } | QueryOutcome::NotReady { .. } => {
            HistoryEvent::QueryAbandoned { operation_id }
        }
    }
}

pub(super) fn submit_terminal(operation_id: OperationId, outcome: &SubmitOutcome) -> HistoryEvent {
    match outcome {
        SubmitOutcome::Applied {
            disposition,
            response,
        } => HistoryEvent::Completed {
            operation_id,
            outcome: ApplyOutcome {
                disposition: *disposition,
                response: *response,
            },
        },
        // Both refusals prove the bytes never reached a replicated log:
        // NOTCOMMITTED is the driver's NotAppended fate and NOTREADY refused
        // before the command reached rafter-service.
        SubmitOutcome::NotCommitted { .. } | SubmitOutcome::NotReady { .. } => {
            HistoryEvent::NotCommitted { operation_id }
        }
        SubmitOutcome::Unknown { .. } => HistoryEvent::Unknown { operation_id },
    }
}
