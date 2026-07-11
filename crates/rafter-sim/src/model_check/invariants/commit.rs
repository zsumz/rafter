use super::{catalog, summarize, Action, Failure};
use super::{
    BTreeMap, Cluster, CommittedConfiguration, ExplorationState, LogEntry, LogIndex,
    MembershipConfig, NodeId,
};

pub(super) fn check_commit_index_monotonicity(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    for (node_id, floor) in &state.commit_floor_by_node {
        let commit_index = state.cluster.commit_index(*node_id);
        if commit_index < *floor {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::CM_01_COMMIT_INDEX_MONOTONICITY_AND_BOUNDS,
                message: format!(
                    "{node_id} commit index regressed from observed floor {floor} to {commit_index}"
                ),
                trace: trace.to_vec(),
                state: summarize(&state.cluster),
            });
        }
    }
    Ok(())
}

pub(super) fn check_committed_configuration_monotonicity(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    for (node_id, floor) in &state.committed_configuration_floor_by_node {
        let Some(floor) = floor else {
            continue;
        };
        let actual = state.cluster.committed_configuration_state(*node_id);
        match actual {
            None => {
                return Err(committed_configuration_regression(
                    state, trace, *node_id, *floor, actual,
                ));
            }
            Some(actual)
                if actual.index < floor.index
                    || (actual.index == floor.index && actual.config_id != floor.config_id) =>
            {
                return Err(committed_configuration_regression(
                    state,
                    trace,
                    *node_id,
                    *floor,
                    Some(actual),
                ));
            }
            Some(_) => {}
        }
    }
    Ok(())
}

fn committed_configuration_regression(
    state: &ExplorationState,
    trace: &[Action],
    node_id: NodeId,
    floor: CommittedConfiguration,
    actual: Option<CommittedConfiguration>,
) -> Failure {
    Failure {
        kind: crate::model_check::FailureKind::InvariantViolation,
        invariant: catalog::MB_04_MONOTONE_CONFIGURATION_TRANSITION_AND_IDENTITY,
        message: format!(
            "{node_id} committed configuration regressed from observed floor {floor:?} to {actual:?}"
        ),
        trace: trace.to_vec(),
        state: summarize(&state.cluster),
    }
}

pub(super) fn check_committed_prefixes(cluster: &Cluster, trace: &[Action]) -> Result<(), Failure> {
    let mut committed_by_index = BTreeMap::<LogIndex, LogEntry>::new();
    for (node_id, node) in &cluster.nodes {
        let commit_index = node.commit_index();
        let snapshot_index = node.snapshot_index();
        let last_log_index = node.last_log_index();
        if commit_index > last_log_index {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::CM_01_COMMIT_INDEX_MONOTONICITY_AND_BOUNDS,
                message: format!(
                    "{node_id} commit index {commit_index} is beyond local last log index {last_log_index}"
                ),
                trace: trace.to_vec(),
                state: summarize(cluster),
            });
        }

        let first_log_index = snapshot_index.next();
        let entries = node.log_entries_from(first_log_index);
        for (offset, entry) in entries.into_iter().enumerate() {
            let log_index = LogIndex(first_log_index.0 + offset as u64);
            if log_index > commit_index {
                break;
            }
            if let Some(previous) = committed_by_index.get(&log_index) {
                if previous != &entry {
                    return Err(Failure {
                        kind: crate::model_check::FailureKind::InvariantViolation,
                        invariant: catalog::LG_04_COMMITTED_PREFIX_STABILITY,
                        message: format!("committed prefix diverged at log index {log_index}"),
                        trace: trace.to_vec(),
                        state: summarize(cluster),
                    });
                }
            } else {
                committed_by_index.insert(log_index, entry);
            }
        }
    }
    Ok(())
}

pub(super) fn check_membership_quorum_validity(
    cluster: &Cluster,
    trace: &[Action],
) -> Result<(), Failure> {
    for (node_id, node) in &cluster.nodes {
        match node.effective_membership() {
            MembershipConfig::Stable(stable) if stable.voters().is_empty() => {
                return Err(Failure {
                    kind: crate::model_check::FailureKind::InvariantViolation,
                    invariant: catalog::MB_01_MEMBERSHIP_WELL_FORMEDNESS,
                    message: format!("{node_id} has an empty stable voter set"),
                    trace: trace.to_vec(),
                    state: summarize(cluster),
                });
            }
            MembershipConfig::Joint(joint)
                if joint.old().voters().is_empty()
                    || joint.new_membership().voters().is_empty() =>
            {
                return Err(Failure {
                    kind: crate::model_check::FailureKind::InvariantViolation,
                    invariant: catalog::MB_01_MEMBERSHIP_WELL_FORMEDNESS,
                    message: format!("{node_id} has an empty joint voter set"),
                    trace: trace.to_vec(),
                    state: summarize(cluster),
                });
            }
            MembershipConfig::Stable(_) | MembershipConfig::Joint(_) => {}
        }
    }
    Ok(())
}

pub(super) fn check_no_overlapping_uncommitted_configurations(
    cluster: &Cluster,
    trace: &[Action],
) -> Result<(), Failure> {
    for node_id in cluster.nodes.keys() {
        let bootstrap = cluster.bootstrap_state(*node_id);
        let uncommitted_configurations = bootstrap
            .log
            .iter()
            .filter(|entry| entry.index > bootstrap.commit_index && entry.kind.is_configuration())
            .count();
        if uncommitted_configurations > 1 {
            return Err(Failure {
                kind: crate::model_check::FailureKind::InvariantViolation,
                invariant: catalog::MB_03_SERIALIZED_CONFIGURATION_CHANGES,
                message: format!(
                    "{node_id} has {uncommitted_configurations} uncommitted configuration entries"
                ),
                trace: trace.to_vec(),
                state: summarize(cluster),
            });
        }
    }
    Ok(())
}

pub(super) fn check_required_committed_configurations(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    for ((node_id, index), expected) in &state.required_committed_configurations {
        if state.cluster.commit_index(*node_id) < *index {
            continue;
        }
        let actual = state.cluster.committed_configuration_state(*node_id);
        if actual == Some(*expected) {
            continue;
        }
        return Err(Failure {
            kind: crate::model_check::FailureKind::InvariantViolation,
            invariant: catalog::MB_04_MONOTONE_CONFIGURATION_TRANSITION_AND_IDENTITY,
            message: format!(
                "{node_id} committed required configuration at index {index} as {actual:?}, expected {expected:?}"
            ),
            trace: trace.to_vec(),
            state: summarize(&state.cluster),
        });
    }
    Ok(())
}
