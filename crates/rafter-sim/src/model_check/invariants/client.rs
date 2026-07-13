use super::{catalog, summarize, Action, Failure};
use super::{
    check_client_history_linearizable, BTreeMap, ClientRead, ClientReadOutcome, ClientReadProof,
    ClientWriteStatus, Cluster, ExplorationState, CLIENT_HISTORY_LINEARIZABILITY_INVARIANT,
};

/// The committed-floor check: a granted read barrier must cover every entry
/// that any node had committed before the barrier was registered. A grant below
/// its registration floor means an isolated or stale leader certified a read
/// that misses acknowledged writes (thesis 6.4).
pub(crate) fn check_read_barrier_safety(
    cluster: &Cluster,
    trace: &[Action],
) -> Result<(), Failure> {
    check_registered_read_grants(cluster, trace)?;
    check_read_grant_committed_floors(cluster, trace)
}

pub(super) fn check_registered_read_grants(
    cluster: &Cluster,
    trace: &[Action],
) -> Result<(), Failure> {
    for grant in cluster.read_grants() {
        if !cluster.read_registrations().iter().any(|registration| {
            grant.operation_id == Some(registration.operation_id)
                && registration.node_id == grant.node_id
                && registration.request_id == grant.request_id
        }) {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::RD_03_READ_BARRIER_COVERS_COMMITTED_FLOOR,
                message: format!(
                    "{} granted read barrier {} that was never registered",
                    grant.node_id, grant.request_id
                ),
                trace: trace.to_vec(),
                state: summarize(cluster),
            });
        }
    }
    Ok(())
}

pub(super) fn check_read_grant_committed_floors(
    cluster: &Cluster,
    trace: &[Action],
) -> Result<(), Failure> {
    for grant in cluster.read_grants() {
        let Some(registration) = cluster.read_registrations().iter().find(|registration| {
            grant.operation_id == Some(registration.operation_id)
                && registration.node_id == grant.node_id
                && registration.request_id == grant.request_id
        }) else {
            continue;
        };
        if grant.read_index < registration.committed_floor {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::RD_03_READ_BARRIER_COVERS_COMMITTED_FLOOR,
                message: format!(
                    "{} granted read barrier {} at index {} below the committed floor {} at registration",
                    grant.node_id,
                    grant.request_id,
                    grant.read_index,
                    registration.committed_floor
                ),
                trace: trace.to_vec(),
                state: summarize(cluster),
            });
        }
    }
    Ok(())
}

pub(super) fn check_client_history_read_write_invariants(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    check_client_read_instrumentation(state, trace)?;
    check_completed_write_indexes(state, trace)?;

    for read in state.client_history().reads.values() {
        let proof = match &read.outcome {
            ClientReadOutcome::Pending
            | ClientReadOutcome::Rejected { .. }
            | ClientReadOutcome::Canceled { .. } => continue,
            ClientReadOutcome::ProofGranted { proof }
            | ClientReadOutcome::Completed { proof, .. } => *proof,
        };
        if proof.read_index < read.committed_floor {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::RD_03_READ_BARRIER_COVERS_COMMITTED_FLOOR,
                message: format!(
                    "{} read {} proof index {} is below registration floor {}",
                    read.node_id, read.request_id, proof.read_index, read.committed_floor
                ),
                trace: trace.to_vec(),
                state: summarize(state.cluster()),
            });
        }
        if let ClientReadOutcome::Completed { proof, .. } = &read.outcome {
            check_completed_read(state, read, *proof, trace)?;
        }
    }

    Ok(())
}

fn check_client_read_instrumentation(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    if let Some(error) = state
        .client_history()
        .read_instrumentation_errors
        .iter()
        .next()
    {
        return Err(Failure {
            kind: crate::model_check::FailureKind::HarnessError,
            invariant: catalog::RD_06_CLIENT_HISTORY_LINEARIZABILITY,
            message: format!("client-read instrumentation failed: {error}"),
            trace: trace.to_vec(),
            state: summarize(state.cluster()),
        });
    }
    if let Some(error) = state
        .cluster()
        .read_output_correlation_errors()
        .iter()
        .next()
    {
        return Err(Failure {
            kind: crate::model_check::FailureKind::HarnessError,
            invariant: catalog::RD_03_READ_BARRIER_COVERS_COMMITTED_FLOOR,
            message: format!("read-output recorder correlation failed: {error}"),
            trace: trace.to_vec(),
            state: summarize(state.cluster()),
        });
    }
    Ok(())
}

fn check_completed_write_indexes(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    let mut completed_by_index = BTreeMap::new();
    for write in state.client_history().writes.values() {
        if let ClientWriteStatus::Completed { index, .. } = write.status {
            if let Some(previous) = completed_by_index.insert(index, write.proposal_id) {
                if previous != write.proposal_id {
                    return Err(Failure {
                        kind: crate::model_check::FailureKind::InvariantViolation,
                        invariant: catalog::RD_06_CLIENT_HISTORY_LINEARIZABILITY,
                        message: format!(
                            "client writes {} and {} both completed at log index {index}",
                            previous.0, write.proposal_id.0
                        ),
                        trace: trace.to_vec(),
                        state: summarize(state.cluster()),
                    });
                }
            }
        }
    }
    Ok(())
}

fn check_completed_read(
    state: &ExplorationState,
    read: &ClientRead,
    proof: ClientReadProof,
    trace: &[Action],
) -> Result<(), Failure> {
    if proof.local_applied_index < proof.read_index {
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::RD_04_APPLY_BEFORE_SERVING_A_READ,
            message: format!(
                "{} completed read {} at local applied {} below required index {}",
                read.node_id, read.request_id, proof.local_applied_index, proof.read_index
            ),
            trace: trace.to_vec(),
            state: summarize(state.cluster()),
        });
    }
    for write in state.client_history().writes.values() {
        let ClientWriteStatus::Completed { index, .. } = write.status else {
            continue;
        };
        if index <= read.committed_floor && index > proof.read_index {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::RD_03_READ_BARRIER_COVERS_COMMITTED_FLOOR,
                message: format!(
                    "{} completed read {} at {} without covering completed write {} at {}",
                    read.node_id, read.request_id, proof.read_index, write.proposal_id.0, index
                ),
                trace: trace.to_vec(),
                state: summarize(state.cluster()),
            });
        }
    }
    Ok(())
}

pub(super) fn check_client_history_linearizability(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    let instrumentation_failed = !state.client_history().instrumentation_errors.is_empty()
        || !state
            .client_history()
            .read_instrumentation_errors
            .is_empty();
    check_client_history_linearizable(state.client_history()).map_err(|message| Failure {
        kind: if instrumentation_failed {
            crate::model_check::FailureKind::HarnessError
        } else {
            crate::model_check::FailureKind::InvariantViolation
        },
        invariant: CLIENT_HISTORY_LINEARIZABILITY_INVARIANT,
        message,
        trace: trace.to_vec(),
        state: summarize(state.cluster()),
    })
}
