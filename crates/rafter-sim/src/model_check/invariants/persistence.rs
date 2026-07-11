use rafter::{LogIndex, NodeId, SharedPayload};

use crate::{Applied, Cluster, DurableStateDigest};

use super::{catalog, summarize, Action, Failure};

pub(crate) fn check_exact_durable_restart(
    cluster: &Cluster,
    node_id: NodeId,
    before: &DurableStateDigest,
    after: &DurableStateDigest,
    trace: &[Action],
) -> Result<(), Failure> {
    if before != after {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::PS_03_EXACT_DURABLE_RESTART,
            message: format!("{node_id} restart changed durable state digest"),
            trace: trace.to_vec(),
            state: summarize(cluster),
        });
    }

    Ok(())
}

#[derive(Clone, Copy)]
pub(crate) struct AppliedFloorRecovery<'a> {
    pub(crate) node_id: NodeId,
    pub(crate) applied_floor: LogIndex,
    pub(crate) commit_index: LogIndex,
    pub(crate) last_log_index: LogIndex,
    pub(crate) expected_replay: &'a [(LogIndex, SharedPayload)],
    pub(crate) recovered_applies: &'a [Applied],
}

pub(crate) fn check_applied_floor_recovery(
    cluster: &Cluster,
    recovery: AppliedFloorRecovery<'_>,
    trace: &[Action],
) -> Result<(), Failure> {
    let AppliedFloorRecovery {
        node_id,
        applied_floor,
        commit_index,
        last_log_index,
        expected_replay,
        recovered_applies,
    } = recovery;

    if applied_floor > commit_index {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::PS_04_APPLIED_FLOOR_RECOVERY,
            message: format!(
                "{node_id} durable applied floor {applied_floor} exceeds commit index {commit_index}"
            ),
            trace: trace.to_vec(),
            state: summarize(cluster),
        });
    }
    if applied_floor > last_log_index {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::PS_04_APPLIED_FLOOR_RECOVERY,
            message: format!(
                "{node_id} durable applied floor {applied_floor} exceeds local last log index {last_log_index}"
            ),
            trace: trace.to_vec(),
            state: summarize(cluster),
        });
    }

    let actual_replay = recovered_applies
        .iter()
        .filter(|applied| applied.node_id == node_id)
        .map(|applied| (applied.index, applied.payload.clone()))
        .collect::<Vec<_>>();
    if let Some((index, _)) = actual_replay
        .iter()
        .find(|(index, _)| *index <= applied_floor)
    {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::PS_04_APPLIED_FLOOR_RECOVERY,
            message: format!(
                "{node_id} replayed application entry at {index} at or below durable applied floor {applied_floor}"
            ),
            trace: trace.to_vec(),
            state: summarize(cluster),
        });
    }
    if let Some((index, _)) = actual_replay
        .iter()
        .find(|(index, _)| *index > commit_index)
    {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::PS_04_APPLIED_FLOOR_RECOVERY,
            message: format!(
                "{node_id} replayed application entry at {index} above commit index {commit_index}"
            ),
            trace: trace.to_vec(),
            state: summarize(cluster),
        });
    }
    if actual_replay != expected_replay {
        let actual_indexes = actual_replay
            .iter()
            .map(|(index, _)| *index)
            .collect::<Vec<_>>();
        let expected_indexes = expected_replay
            .iter()
            .map(|(index, _)| *index)
            .collect::<Vec<_>>();
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::PS_04_APPLIED_FLOOR_RECOVERY,
            message: format!(
                "{node_id} replayed application indexes {actual_indexes:?}; expected {expected_indexes:?} above durable applied floor {applied_floor}"
            ),
            trace: trace.to_vec(),
            state: summarize(cluster),
        });
    }

    Ok(())
}
