use std::collections::BTreeSet;

use rafter::{CommittedConfiguration, LogEntryKind};

use super::{catalog, summarize, Action, BTreeMap, Cluster, ExplorationState, Failure};
use super::{LogIndex, NodeId};

pub(super) fn check_execution_history_agreement(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(error) = state.execution_instrumentation_errors().iter().next() {
        return Err(Failure {
            kind: crate::model_check::FailureKind::HarnessError,
            invariant: catalog::AP_02_STATE_MACHINE_SAFETY,
            message: format!("execution-history instrumentation failed: {error}"),
            trace: trace.to_vec(),
            state: summarize(state.cluster()),
        });
    }

    let mut witness_by_index = BTreeMap::<LogIndex, &crate::ExecutionWitness>::new();
    for witness in state.application_history() {
        let derived_result = independently_derive_reference_result(witness);
        if witness.resulting_state != derived_result {
            return Err(ap_02_failure(
                state.cluster(),
                trace,
                format!(
                    "{} epoch {} recorded an invalid reference-state result at log index {}",
                    witness.node_id, witness.application_epoch, witness.entry.index
                ),
            ));
        }

        if let Some(previous) = witness_by_index.get(&witness.entry.index) {
            if previous.entry.term != witness.entry.term
                || previous.entry.kind != witness.entry.kind
            {
                return Err(ap_02_failure(
                    state.cluster(),
                    trace,
                    format!(
                        "{} and {} applied different term/kind/input identities at log index {}",
                        previous.node_id, witness.node_id, witness.entry.index
                    ),
                ));
            }
            if previous.prior_state != witness.prior_state
                || previous.resulting_state != witness.resulting_state
            {
                return Err(ap_02_failure(
                    state.cluster(),
                    trace,
                    format!(
                        "{} and {} obtained different prior/result state identities at log index {}",
                        previous.node_id, witness.node_id, witness.entry.index
                    ),
                ));
            }
        } else {
            witness_by_index.insert(witness.entry.index, witness);
        }
    }
    Ok(())
}

fn independently_derive_reference_result(
    witness: &crate::ExecutionWitness,
) -> crate::ReferenceState {
    let mut result = witness.prior_state.clone();
    match &witness.entry.kind {
        LogEntryKind::Application(payload) => {
            result.application_value.clone_from(payload);
        }
        LogEntryKind::Configuration(configuration) => {
            result.committed_membership = configuration.membership_config();
            result.committed_configuration = Some(CommittedConfiguration {
                index: witness.entry.index,
                config_id: configuration.config_id(),
            });
        }
        LogEntryKind::Noop => {}
    }
    result
}

fn ap_02_failure(cluster: &Cluster, trace: &[Action], message: String) -> Failure {
    Failure {
        kind: crate::model_check::FailureKind::InvariantViolation,
        invariant: catalog::AP_02_STATE_MACHINE_SAFETY,
        message,
        trace: trace.to_vec(),
        state: summarize(cluster),
    }
}

pub(super) fn check_applied_payload_agreement(
    cluster: &Cluster,
    trace: &[Action],
) -> Result<(), Failure> {
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
    check_applied_cursor_monotonicity(cluster, trace)?;
    check_applied_exactly_once(cluster, trace)?;
    check_applied_commit_bound(cluster, trace)
}

pub(super) fn check_applied_cursor_monotonicity(
    cluster: &Cluster,
    trace: &[Action],
) -> Result<(), Failure> {
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

pub(super) fn check_applied_exactly_once(
    cluster: &Cluster,
    trace: &[Action],
) -> Result<(), Failure> {
    let mut applied_indexes = BTreeSet::new();
    for applied in &cluster.applied {
        if !applied_indexes.insert((applied.node_id, applied.application_epoch, applied.index)) {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION,
                message: format!(
                    "{} epoch {} applied index {} more than once",
                    applied.node_id, applied.application_epoch, applied.index
                ),
                trace: trace.to_vec(),
                state: summarize(cluster),
            });
        }
    }
    Ok(())
}

pub(super) fn check_applied_commit_bound(
    cluster: &Cluster,
    trace: &[Action],
) -> Result<(), Failure> {
    for applied in &cluster.applied {
        if applied.index > applied.commit_index_at_emit {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION,
                message: format!(
                    "{} epoch {} applied index {} when its commit index at emit was {}",
                    applied.node_id,
                    applied.application_epoch,
                    applied.index,
                    applied.commit_index_at_emit
                ),
                trace: trace.to_vec(),
                state: summarize(cluster),
            });
        }
    }
    Ok(())
}

pub(super) fn check_forbidden_applied_payloads(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    for applied in &state.cluster().applied {
        if state
            .forbidden_applied_payloads()
            .contains(&applied.payload)
        {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::LG_04_COMMITTED_PREFIX_STABILITY,
                message: format!("forbidden payload applied at log index {}", applied.index),
                trace: trace.to_vec(),
                state: summarize(state.cluster()),
            });
        }
    }
    Ok(())
}

pub(super) fn check_required_applied_payloads(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    for ((node_id, index), payload) in state.required_applied_payloads() {
        if state.cluster().commit_index(*node_id) < *index {
            continue;
        }
        let current_epoch = state.cluster().application_epoch(*node_id);
        if state.cluster().applied().iter().any(|applied| {
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
            state: summarize(state.cluster()),
        });
    }
    Ok(())
}
