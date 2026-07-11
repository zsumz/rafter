use super::{catalog, summarize, Action, BTreeMap, Cluster, ExplorationState, Failure};
use super::{LogIndex, NodeId, SharedPayload};

pub(super) fn check_applied_payload_agreement(
    cluster: &Cluster,
    trace: &[Action],
) -> Result<(), Failure> {
    let mut payload_by_index = BTreeMap::<LogIndex, SharedPayload>::new();
    for applied in &cluster.applied {
        if let Some(previous) = payload_by_index.get(&applied.index) {
            if previous != &applied.payload {
                return Err(Failure {
                    kind: crate::model_check::FailureKind::InvariantViolation,
                    invariant: catalog::AP_02_STATE_MACHINE_SAFETY,
                    message: format!("different payloads applied at log index {}", applied.index),
                    trace: trace.to_vec(),
                    state: summarize(cluster),
                });
            }
        } else {
            payload_by_index.insert(applied.index, applied.payload.clone());
        }
    }

    let mut snapshot_by_boundary = BTreeMap::<LogIndex, (crate::SnapshotInstalled, NodeId)>::new();
    for install in cluster.snapshot_installs() {
        if let Some((previous, previous_node)) =
            snapshot_by_boundary.get(&install.last_included_index)
        {
            if previous.last_included_term != install.last_included_term
                || previous.committed_membership != install.committed_membership
                || previous.payload != install.payload
            {
                return Err(Failure {
                    kind: crate::model_check::FailureKind::InvariantViolation,
                    invariant: catalog::SS_05_SNAPSHOT_SEMANTIC_EQUIVALENCE,
                    message: format!(
                        "{} and {} installed disagreeing snapshots at index {}",
                        previous_node, install.node_id, install.last_included_index
                    ),
                    trace: trace.to_vec(),
                    state: summarize(cluster),
                });
            }
        } else {
            snapshot_by_boundary.insert(
                install.last_included_index,
                (install.clone(), install.node_id),
            );
        }
    }
    Ok(())
}

pub(super) fn check_internal_derived_state(
    cluster: &Cluster,
    trace: &[Action],
) -> Result<(), Failure> {
    for (node_id, node) in &cluster.nodes {
        if let Err(error) = node.validate_derived_state() {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::ST_01_STATE_WELL_FORMEDNESS,
                message: format!("{node_id}: {error}"),
                trace: trace.to_vec(),
                state: summarize(cluster),
            });
        }
    }
    Ok(())
}

pub(super) fn check_applied_order(cluster: &Cluster, trace: &[Action]) -> Result<(), Failure> {
    let mut last_applied_by_node_epoch = BTreeMap::<(NodeId, u64), LogIndex>::new();
    let mut installs = cluster.snapshot_installs().iter().peekable();
    for (position, applied) in cluster.applied.iter().enumerate() {
        while let Some(install) = installs.peek() {
            if install.applied_records_before_install > position {
                break;
            }
            let cursor = last_applied_by_node_epoch
                .entry((install.node_id, install.application_epoch))
                .or_insert(LogIndex::ZERO);
            if install.last_included_index <= *cursor {
                return Err(Failure {
                    kind: crate::model_check::FailureKind::InvariantViolation,
                    invariant: catalog::AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION,
                    message: format!(
                        "{} epoch {} installed a snapshot at index {} at or below its applied index {}",
                        install.node_id,
                        install.application_epoch,
                        install.last_included_index,
                        cursor
                    ),
                    trace: trace.to_vec(),
                    state: summarize(cluster),
                });
            }
            *cursor = install.last_included_index;
            installs.next();
        }
        let previous = last_applied_by_node_epoch
            .get(&(applied.node_id, applied.application_epoch))
            .copied()
            .unwrap_or(LogIndex::ZERO);
        if applied.index <= previous {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION,
                message: format!(
                    "{} epoch {} applied index {} at or below prior applied/snapshot index {}",
                    applied.node_id, applied.application_epoch, applied.index, previous
                ),
                trace: trace.to_vec(),
                state: summarize(cluster),
            });
        }
        last_applied_by_node_epoch
            .insert((applied.node_id, applied.application_epoch), applied.index);
    }
    for install in installs {
        let cursor = last_applied_by_node_epoch
            .entry((install.node_id, install.application_epoch))
            .or_insert(LogIndex::ZERO);
        if install.last_included_index <= *cursor {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION,
                message: format!(
                    "{} epoch {} installed a snapshot at index {} at or below its applied index {}",
                    install.node_id, install.application_epoch, install.last_included_index, cursor
                ),
                trace: trace.to_vec(),
                state: summarize(cluster),
            });
        }
        *cursor = install.last_included_index;
    }
    Ok(())
}

pub(super) fn check_forbidden_applied_payloads(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    for applied in &state.cluster.applied {
        if state.forbidden_applied_payloads.contains(&applied.payload) {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::LG_04_COMMITTED_PREFIX_STABILITY,
                message: format!("forbidden payload applied at log index {}", applied.index),
                trace: trace.to_vec(),
                state: summarize(&state.cluster),
            });
        }
    }
    Ok(())
}

pub(super) fn check_required_applied_payloads(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    for ((node_id, index), payload) in &state.required_applied_payloads {
        if state.cluster.commit_index(*node_id) < *index {
            continue;
        }
        let current_epoch = state.cluster.application_epoch(*node_id);
        if state.cluster.applied().iter().any(|applied| {
            applied.node_id == *node_id
                && applied.application_epoch == current_epoch
                && applied.index == *index
                && &applied.payload == payload
        }) {
            continue;
        }
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION,
            message: format!(
                "{node_id} committed required payload at index {index} without emitting Apply"
            ),
            trace: trace.to_vec(),
            state: summarize(&state.cluster),
        });
    }
    Ok(())
}
