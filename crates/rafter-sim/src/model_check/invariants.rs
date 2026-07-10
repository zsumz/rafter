use std::collections::BTreeMap;

use rafter::{
    CommittedConfiguration, LogEntry, LogIndex, MembershipConfig, NodeId, Role, SharedPayload, Term,
};

use crate::Cluster;

use super::catalog;
use super::linearizability::{
    check_client_history_linearizable, CLIENT_HISTORY_LINEARIZABILITY_INVARIANT,
};
use super::state::{ClientReadOutcome, ClientWriteStatus};
use super::{
    summarize, Action, ElectionSafetyExplorer, ExplorationState, Failure, ReplayCheck,
    RestartSnapshotState,
};

pub(super) fn check_election_safety(cluster: &Cluster, trace: &[Action]) -> Result<(), Failure> {
    check_internal_derived_state(cluster, trace)?;

    let mut leaders_by_term = BTreeMap::<Term, Vec<NodeId>>::new();
    for (node_id, node) in &cluster.nodes {
        if node.role() == Role::Leader {
            leaders_by_term
                .entry(node.current_term())
                .or_default()
                .push(*node_id);
        }
    }

    for (term, leaders) in leaders_by_term {
        if leaders.len() > 1 {
            return Err(Failure {
                invariant: ElectionSafetyExplorer::INVARIANT,
                message: format!("term {term} has multiple leaders: {leaders:?}"),
                trace: trace.to_vec(),
                state: summarize(cluster),
            });
        }
    }

    Ok(())
}

pub(super) fn check_commit_safety(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    check_commit_index_monotonicity(state, trace)?;
    check_committed_configuration_monotonicity(state, trace)?;
    check_applied_payload_agreement(&state.cluster, trace)?;
    check_applied_order(&state.cluster, trace)?;
    check_committed_prefixes(&state.cluster, trace)?;
    check_membership_quorum_validity(&state.cluster, trace)?;
    check_no_overlapping_uncommitted_configurations(&state.cluster, trace)?;
    check_client_history_read_write_invariants(state, trace)?;
    check_client_history_linearizability(state, trace)?;
    check_forbidden_applied_payloads(state, trace)?;
    check_required_applied_payloads(state, trace)?;
    check_required_committed_configurations(state, trace)
}

pub(super) fn check_restart_snapshot_safety(
    state: &RestartSnapshotState,
    trace: &[Action],
) -> Result<(), Failure> {
    check_internal_derived_state(&state.state.cluster, trace)?;
    check_applied_payload_agreement(&state.state.cluster, trace)?;
    check_committed_prefixes(&state.state.cluster, trace)?;

    let Some(expected) = &state.expected_snapshot else {
        return Ok(());
    };

    for applied in &state.state.cluster.applied {
        if applied.payload == expected.payload {
            return Err(Failure {
                invariant: catalog::SS_05_SNAPSHOT_SEMANTIC_EQUIVALENCE,
                message: "snapshot bytes were exposed as an applied log entry".to_string(),
                trace: trace.to_vec(),
                state: summarize(&state.state.cluster),
            });
        }
    }

    for (node_id, node) in &state.state.cluster.nodes {
        if let Some(pending) = node.pending_snapshot_transfer() {
            if pending.is_complete() {
                return Err(Failure {
                    invariant: catalog::SS_04_SNAPSHOT_TRANSFER_INTEGRITY,
                    message: format!("{node_id} retained a complete pending snapshot transfer"),
                    trace: trace.to_vec(),
                    state: summarize(&state.state.cluster),
                });
            }
            if pending.received_bytes() > pending.total_payload_len {
                return Err(Failure {
                    invariant: catalog::SS_04_SNAPSHOT_TRANSFER_INTEGRITY,
                    message: format!(
                        "{node_id} pending snapshot bytes {} exceed total {}",
                        pending.received_bytes(),
                        pending.total_payload_len
                    ),
                    trace: trace.to_vec(),
                    state: summarize(&state.state.cluster),
                });
            }
            if pending.metadata.last_included_index <= node.snapshot_index() {
                return Err(Failure {
                    invariant: catalog::SS_04_SNAPSHOT_TRANSFER_INTEGRITY,
                    message: format!(
                        "{node_id} retained a stale pending snapshot at {} after installing {}",
                        pending.metadata.last_included_index,
                        node.snapshot_index()
                    ),
                    trace: trace.to_vec(),
                    state: summarize(&state.state.cluster),
                });
            }
        }

        if node.snapshot_index() < expected.snapshot.metadata.last_included_index {
            continue;
        }

        let bootstrap = state.state.cluster.bootstrap_state(*node_id);
        if bootstrap.snapshot.as_ref() == Some(&expected.snapshot)
            && state
                .state
                .cluster
                .snapshot_payload(*node_id, &expected.snapshot)
                != Some(expected.payload.as_slice())
        {
            return Err(Failure {
                invariant: catalog::SS_01_ATOMIC_MONOTONE_SNAPSHOT_STATE,
                message: format!("{node_id} installed expected metadata with different bytes"),
                trace: trace.to_vec(),
                state: summarize(&state.state.cluster),
            });
        }

        for entry in bootstrap.log {
            if state
                .divergent_payloads
                .iter()
                .any(|payload| entry.kind.application_payload() == Some(payload.as_slice()))
            {
                return Err(Failure {
                    invariant: catalog::SS_05_SNAPSHOT_SEMANTIC_EQUIVALENCE,
                    message: format!(
                        "{node_id} resurrected divergent suffix at log index {}",
                        entry.index
                    ),
                    trace: trace.to_vec(),
                    state: summarize(&state.state.cluster),
                });
            }
        }
    }

    Ok(())
}

pub(super) fn run_replay_check(
    state: &ExplorationState,
    check: ReplayCheck,
    trace: &[Action],
) -> Result<(), Failure> {
    match check {
        ReplayCheck::ElectionSafety => check_election_safety(&state.cluster, trace),
        ReplayCheck::CommitSafety => {
            check_election_safety(&state.cluster, trace)?;
            check_commit_safety(state, trace)?;
            check_read_barrier_safety(&state.cluster, trace)
        }
    }
}

fn check_applied_payload_agreement(cluster: &Cluster, trace: &[Action]) -> Result<(), Failure> {
    let mut payload_by_index = BTreeMap::<LogIndex, SharedPayload>::new();
    for applied in &cluster.applied {
        if let Some(previous) = payload_by_index.get(&applied.index) {
            if previous != &applied.payload {
                return Err(Failure {
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
                || previous.payload != install.payload
            {
                return Err(Failure {
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

fn check_commit_index_monotonicity(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    for (node_id, floor) in &state.commit_floor_by_node {
        let commit_index = state.cluster.commit_index(*node_id);
        if commit_index < *floor {
            return Err(Failure {
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

fn check_committed_configuration_monotonicity(
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
        invariant: catalog::MB_04_MONOTONE_CONFIGURATION_TRANSITION_AND_IDENTITY,
        message: format!(
            "{node_id} committed configuration regressed from observed floor {floor:?} to {actual:?}"
        ),
        trace: trace.to_vec(),
        state: summarize(&state.cluster),
    }
}

fn check_internal_derived_state(cluster: &Cluster, trace: &[Action]) -> Result<(), Failure> {
    for (node_id, node) in &cluster.nodes {
        if let Err(error) = node.validate_derived_state() {
            return Err(Failure {
                invariant: catalog::ST_01_STATE_WELL_FORMEDNESS,
                message: format!("{node_id}: {error}"),
                trace: trace.to_vec(),
                state: summarize(cluster),
            });
        }
    }
    Ok(())
}

/// The committed-floor check: a granted read barrier must cover every entry
/// that any node had committed before the barrier was registered. A grant below
/// its registration floor means an isolated or stale leader certified a read
/// that misses acknowledged writes (thesis 6.4).
pub(super) fn check_read_barrier_safety(
    cluster: &Cluster,
    trace: &[Action],
) -> Result<(), Failure> {
    for grant in cluster.read_grants() {
        let registration = cluster.read_registrations().iter().find(|registration| {
            registration.node_id == grant.node_id && registration.request_id == grant.request_id
        });
        let Some(registration) = registration else {
            return Err(Failure {
                invariant: catalog::RD_03_READ_BARRIER_COVERS_COMMITTED_FLOOR,
                message: format!(
                    "{} granted read barrier {} that was never registered",
                    grant.node_id, grant.request_id
                ),
                trace: trace.to_vec(),
                state: summarize(cluster),
            });
        };
        if grant.read_index < registration.committed_floor {
            return Err(Failure {
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

fn check_applied_order(cluster: &Cluster, trace: &[Action]) -> Result<(), Failure> {
    let mut last_applied_by_node = BTreeMap::<NodeId, LogIndex>::new();
    let mut installs = cluster.snapshot_installs().iter().peekable();
    for (position, applied) in cluster.applied.iter().enumerate() {
        while let Some(install) = installs.peek() {
            if install.applied_records_before_install > position {
                break;
            }
            let cursor = last_applied_by_node
                .entry(install.node_id)
                .or_insert(LogIndex::ZERO);
            if install.last_included_index <= *cursor {
                return Err(Failure {
                    invariant: catalog::AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION,
                    message: format!(
                        "{} installed a snapshot at index {} at or below its applied index {}",
                        install.node_id, install.last_included_index, cursor
                    ),
                    trace: trace.to_vec(),
                    state: summarize(cluster),
                });
            }
            *cursor = install.last_included_index;
            installs.next();
        }
        let previous = last_applied_by_node
            .get(&applied.node_id)
            .copied()
            .unwrap_or(LogIndex::ZERO);
        if applied.index <= previous {
            return Err(Failure {
                invariant: catalog::AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION,
                message: format!(
                    "{} applied index {} at or below prior applied/snapshot index {}",
                    applied.node_id, applied.index, previous
                ),
                trace: trace.to_vec(),
                state: summarize(cluster),
            });
        }
        last_applied_by_node.insert(applied.node_id, applied.index);
    }
    for install in installs {
        let cursor = last_applied_by_node
            .entry(install.node_id)
            .or_insert(LogIndex::ZERO);
        if install.last_included_index <= *cursor {
            return Err(Failure {
                invariant: catalog::AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION,
                message: format!(
                    "{} installed a snapshot at index {} at or below its applied index {}",
                    install.node_id, install.last_included_index, cursor
                ),
                trace: trace.to_vec(),
                state: summarize(cluster),
            });
        }
        *cursor = install.last_included_index;
    }
    Ok(())
}

fn check_committed_prefixes(cluster: &Cluster, trace: &[Action]) -> Result<(), Failure> {
    let mut committed_by_index = BTreeMap::<LogIndex, LogEntry>::new();
    for (node_id, node) in &cluster.nodes {
        let commit_index = node.commit_index();
        let snapshot_index = node.snapshot_index();
        let last_log_index = node.last_log_index();
        if commit_index > last_log_index {
            return Err(Failure {
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

fn check_membership_quorum_validity(cluster: &Cluster, trace: &[Action]) -> Result<(), Failure> {
    for (node_id, node) in &cluster.nodes {
        match node.effective_membership() {
            MembershipConfig::Stable(stable) if stable.voters().is_empty() => {
                return Err(Failure {
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

fn check_no_overlapping_uncommitted_configurations(
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

fn check_client_history_read_write_invariants(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    let mut completed_by_index = BTreeMap::new();
    for write in state.client_history.writes.values() {
        if let ClientWriteStatus::Completed { index, .. } = write.status {
            if let Some(previous) = completed_by_index.insert(index, write.proposal_id) {
                if previous != write.proposal_id {
                    return Err(Failure {
                        invariant: catalog::RD_06_CLIENT_HISTORY_LINEARIZABILITY,
                        message: format!(
                            "client writes {} and {} both completed at log index {index}",
                            previous.0, write.proposal_id.0
                        ),
                        trace: trace.to_vec(),
                        state: summarize(&state.cluster),
                    });
                }
            }
        }
    }

    for read in state.client_history.reads.values() {
        let proof = match &read.outcome {
            ClientReadOutcome::Pending => continue,
            ClientReadOutcome::ProofGranted { proof }
            | ClientReadOutcome::Completed { proof, .. } => *proof,
        };
        if proof.read_index < read.committed_floor {
            return Err(Failure {
                invariant: catalog::RD_03_READ_BARRIER_COVERS_COMMITTED_FLOOR,
                message: format!(
                    "{} read {} proof index {} is below registration floor {}",
                    read.node_id, read.request_id, proof.read_index, read.committed_floor
                ),
                trace: trace.to_vec(),
                state: summarize(&state.cluster),
            });
        }
        if let ClientReadOutcome::Completed { proof, .. } = &read.outcome {
            let proof = *proof;
            if proof.local_applied_index < proof.read_index {
                return Err(Failure {
                    invariant: catalog::RD_04_APPLY_BEFORE_SERVING_A_READ,
                    message: format!(
                        "{} completed read {} at local applied {} below required index {}",
                        read.node_id, read.request_id, proof.local_applied_index, proof.read_index
                    ),
                    trace: trace.to_vec(),
                    state: summarize(&state.cluster),
                });
            }
            for write in state.client_history.writes.values() {
                let ClientWriteStatus::Completed { index, .. } = write.status else {
                    continue;
                };
                if index <= read.committed_floor && index > proof.read_index {
                    return Err(Failure {
                        invariant: catalog::RD_03_READ_BARRIER_COVERS_COMMITTED_FLOOR,
                        message: format!(
                            "{} completed read {} at {} without covering completed write {} at {}",
                            read.node_id,
                            read.request_id,
                            proof.read_index,
                            write.proposal_id.0,
                            index
                        ),
                        trace: trace.to_vec(),
                        state: summarize(&state.cluster),
                    });
                }
            }
        }
    }

    Ok(())
}

fn check_client_history_linearizability(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    check_client_history_linearizable(&state.client_history).map_err(|message| Failure {
        invariant: CLIENT_HISTORY_LINEARIZABILITY_INVARIANT,
        message,
        trace: trace.to_vec(),
        state: summarize(&state.cluster),
    })
}

fn check_forbidden_applied_payloads(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    for applied in &state.cluster.applied {
        if state.forbidden_applied_payloads.contains(&applied.payload) {
            return Err(Failure {
                invariant: catalog::LG_04_COMMITTED_PREFIX_STABILITY,
                message: format!("forbidden payload applied at log index {}", applied.index),
                trace: trace.to_vec(),
                state: summarize(&state.cluster),
            });
        }
    }
    Ok(())
}

fn check_required_applied_payloads(
    state: &ExplorationState,
    trace: &[Action],
) -> Result<(), Failure> {
    for ((node_id, index), payload) in &state.required_applied_payloads {
        if state.cluster.commit_index(*node_id) < *index {
            continue;
        }
        if state.cluster.applied().iter().any(|applied| {
            applied.node_id == *node_id && applied.index == *index && &applied.payload == payload
        }) {
            continue;
        }
        return Err(Failure {
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

fn check_required_committed_configurations(
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

#[cfg(test)]
mod tests {
    use rafter::{NodeConfig, NodeId, Term};

    use super::super::state::{ClientRead, ClientReadProof, ClientWrite, ClientWriteUnknownReason};
    use super::*;
    use crate::{Applied, SnapshotInstalled};

    fn one_node_cluster() -> Cluster {
        let config = NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 3)
            .expect("test config is valid");
        Cluster::new(vec![config])
    }

    #[test]
    fn applied_order_detects_snapshot_rewinding_applied_entries() {
        let mut cluster = one_node_cluster();
        for index in 1..=3 {
            cluster.applied.push(Applied {
                node_id: NodeId(1),
                index: LogIndex(index),
                payload: vec![u8::try_from(index).unwrap_or(u8::MAX)].into(),
            });
        }
        cluster.snapshot_installs.push(SnapshotInstalled {
            node_id: NodeId(1),
            last_included_index: LogIndex(2),
            last_included_term: Term(1),
            payload: b"rewind".to_vec(),
            applied_records_before_install: 3,
        });

        let failure = check_applied_order(&cluster, &[]).expect_err("rewind must be detected");
        assert_eq!(
            failure.invariant(),
            catalog::AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION
        );
        assert!(
            failure.message.contains("installed a snapshot at index 2"),
            "unexpected failure message: {}",
            failure.message
        );
    }

    #[test]
    fn application_loss_restart_preserves_immutable_event_history_positions() {
        let mut cluster = one_node_cluster();
        cluster.applied.push(Applied {
            node_id: NodeId(1),
            index: LogIndex(1),
            payload: b"node-one-before-loss".to_vec().into(),
        });
        cluster.applied.push(Applied {
            node_id: NodeId(2),
            index: LogIndex(1),
            payload: b"node-two-before-snapshot".to_vec().into(),
        });
        cluster.snapshot_installs.push(SnapshotInstalled {
            node_id: NodeId(2),
            last_included_index: LogIndex(2),
            last_included_term: Term(1),
            payload: b"node-two-snapshot".to_vec(),
            applied_records_before_install: 2,
        });
        cluster.applied.push(Applied {
            node_id: NodeId(2),
            index: LogIndex(3),
            payload: b"node-two-after-snapshot".to_vec().into(),
        });
        let before_applied = cluster.applied().to_vec();
        let before_installs = cluster.snapshot_installs().to_vec();

        cluster
            .restart_node_from_bootstrap_losing_application_state(
                NodeId(1),
                cluster.bootstrap_state(NodeId(1)),
            )
            .expect("empty application-loss restart is valid");

        assert_eq!(cluster.applied(), before_applied.as_slice());
        assert_eq!(cluster.snapshot_installs(), before_installs.as_slice());
        assert!(
            check_applied_order(&cluster, &[]).is_ok(),
            "unchanged snapshot positions should still describe the immutable event stream"
        );
    }

    #[test]
    fn applied_order_detects_apply_at_or_below_snapshot_boundary() {
        let mut cluster = one_node_cluster();
        cluster.snapshot_installs.push(SnapshotInstalled {
            node_id: NodeId(1),
            last_included_index: LogIndex(5),
            last_included_term: Term(1),
            payload: b"snapshot".to_vec(),
            applied_records_before_install: 0,
        });
        cluster.applied.push(Applied {
            node_id: NodeId(1),
            index: LogIndex(3),
            payload: b"stale".to_vec().into(),
        });

        let failure =
            check_applied_order(&cluster, &[]).expect_err("apply below boundary must be detected");
        assert_eq!(
            failure.invariant(),
            catalog::AP_01_ORDERED_EXACTLY_ONCE_COMMITTED_APPLICATION
        );
        assert!(
            failure
                .message
                .contains("at or below prior applied/snapshot index 5"),
            "unexpected failure message: {}",
            failure.message
        );
    }

    #[test]
    fn read_barrier_invariant_detects_grant_below_registration_floor() {
        let mut cluster = one_node_cluster();
        cluster.read_registrations.push(crate::ReadRegistered {
            node_id: NodeId(1),
            request_id: 7,
            committed_floor: LogIndex(5),
        });
        cluster.read_grants.push(crate::ReadGranted {
            node_id: NodeId(1),
            request_id: 7,
            read_index: LogIndex(3),
            local_applied_index: LogIndex(3),
        });

        let failure = check_read_barrier_safety(&cluster, &[])
            .expect_err("a grant below the committed floor must be detected");
        assert_eq!(
            failure.invariant(),
            catalog::RD_03_READ_BARRIER_COVERS_COMMITTED_FLOOR
        );
        assert!(
            failure.message.contains("below the committed floor 5"),
            "unexpected failure message: {}",
            failure.message
        );
    }

    #[test]
    fn read_barrier_invariant_detects_unregistered_grant() {
        let mut cluster = one_node_cluster();
        cluster.read_grants.push(crate::ReadGranted {
            node_id: NodeId(1),
            request_id: 9,
            read_index: LogIndex(1),
            local_applied_index: LogIndex(1),
        });

        let failure = check_read_barrier_safety(&cluster, &[])
            .expect_err("an unregistered grant must be detected");
        assert_eq!(
            failure.invariant(),
            catalog::RD_03_READ_BARRIER_COVERS_COMMITTED_FLOOR
        );
        assert!(failure.message.contains("never registered"));
    }

    #[test]
    fn client_history_detects_completed_read_before_local_apply_floor() {
        let cluster = one_node_cluster();
        let mut state = ExplorationState::new(cluster);
        state.client_history.reads.insert(
            10,
            ClientRead {
                node_id: NodeId(1),
                request_id: 10,
                committed_floor: LogIndex(5),
                started_at: 0,
                outcome: ClientReadOutcome::Completed {
                    proof: ClientReadProof {
                        read_index: LogIndex(5),
                        local_applied_index: LogIndex(4),
                    },
                    result: None,
                    completed_at: 1,
                },
            },
        );

        let failure = check_client_history_read_write_invariants(&state, &[])
            .expect_err("a completed read below its local apply floor must fail");
        assert_eq!(
            failure.invariant(),
            catalog::RD_04_APPLY_BEFORE_SERVING_A_READ
        );
        assert!(
            failure
                .message
                .contains("local applied 4 below required index 5"),
            "unexpected failure message: {}",
            failure.message
        );
    }

    #[test]
    fn client_history_allows_unknown_write_outcomes() {
        let cluster = one_node_cluster();
        let mut state = ExplorationState::new(cluster);
        state.client_history.writes.insert(
            crate::model_check::ProposalId(7),
            ClientWrite {
                proposal_id: crate::model_check::ProposalId(7),
                node_id: NodeId(1),
                payload: b"unknown".to_vec().into(),
                started_at: 0,
                status: ClientWriteStatus::Unknown {
                    reason: ClientWriteUnknownReason::StaleLeader,
                },
            },
        );

        check_client_history_read_write_invariants(&state, &[])
            .expect("unknown write outcomes should not imply confirmed absence");
    }

    #[test]
    fn applied_agreement_detects_disagreeing_snapshots_at_same_boundary() {
        let mut cluster = one_node_cluster();
        for (node, payload) in [(1, b"state-a".to_vec()), (2, b"state-b".to_vec())] {
            cluster.snapshot_installs.push(SnapshotInstalled {
                node_id: NodeId(node),
                last_included_index: LogIndex(4),
                last_included_term: Term(1),
                payload,
                applied_records_before_install: 0,
            });
        }

        let failure = check_applied_payload_agreement(&cluster, &[])
            .expect_err("disagreeing snapshots must be detected");
        assert_eq!(
            failure.invariant(),
            catalog::SS_05_SNAPSHOT_SEMANTIC_EQUIVALENCE
        );
        assert!(
            failure.message.contains("disagreeing snapshots at index 4"),
            "unexpected failure message: {}",
            failure.message
        );
    }
}
