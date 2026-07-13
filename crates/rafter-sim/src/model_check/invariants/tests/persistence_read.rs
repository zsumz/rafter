use super::super::client::{check_read_grant_committed_floors, check_registered_read_grants};
use super::super::persistence::{
    check_recovery_applied_floor_bounds, check_recovery_applied_floor_exclusion,
    check_recovery_exact_committed_suffix, check_restart_acknowledged_entries,
    check_restart_commit_and_configuration, check_restart_log, check_restart_snapshot,
    check_restart_term_and_vote,
};
use super::*;
use crate::model_check::observations::Observation;
use crate::model_check::state::{
    apply_pending_application_replay_seed, restart_node, PendingApplicationReplaySeed,
};
use rafter::BootstrapState;

fn durable_restart_fixture() -> DurableStateDigest {
    DurableStateDigest {
        current_term: Term(2),
        voted_for: Some(NodeId(1)),
        commit_index: LogIndex(2),
        committed_configuration: Some(CommittedConfiguration {
            index: LogIndex(2),
            config_id: ConfigurationId(7),
        }),
        snapshot: Some(crate::DurableSnapshotDigest {
            transfer_id: SnapshotTransferId(11),
            last_included_index: LogIndex(0),
            last_included_term: Term(0),
            hard_state_term: Term(2),
            application_payload_len: 4,
            application_payload_crc32: 17,
            committed_configuration: None,
        }),
        log: vec![
            BootstrapLogEntry {
                index: LogIndex(1),
                term: Term(1),
                kind: LogEntryKind::Application(b"one".to_vec().into()),
            },
            BootstrapLogEntry {
                index: LogIndex(2),
                term: Term(2),
                kind: LogEntryKind::Application(b"two".to_vec().into()),
            },
        ],
        application_epoch: 0,
        applied_through: LogIndex(2),
    }
}

#[test]
fn exact_restart_term_vote_oracle_detects_vote_loss() {
    let cluster = one_node_cluster();
    let before = durable_restart_fixture();
    let mut after = before.clone();
    after.voted_for = None;

    let failure = check_restart_term_and_vote(&cluster, NodeId(1), &before, &after, &[])
        .expect_err("vote loss must fail PS-03.a");
    assert_eq!(failure.invariant(), catalog::PS_03_EXACT_DURABLE_RESTART);
    assert!(failure.message.contains("term or vote"));
}

#[test]
fn exact_restart_log_oracle_detects_payload_change() {
    let cluster = one_node_cluster();
    let before = durable_restart_fixture();
    let mut after = before.clone();
    after.log[1].kind = LogEntryKind::Application(b"changed".to_vec().into());

    let failure = check_restart_log(&cluster, NodeId(1), &before, &after, &[])
        .expect_err("payload change must fail PS-03.b");
    assert_eq!(failure.invariant(), catalog::PS_03_EXACT_DURABLE_RESTART);
    assert!(failure.message.contains("retained log"));
}

#[test]
fn exact_restart_commit_configuration_oracle_detects_identity_change() {
    let cluster = one_node_cluster();
    let before = durable_restart_fixture();
    let mut after = before.clone();
    after.committed_configuration = Some(CommittedConfiguration {
        index: LogIndex(2),
        config_id: ConfigurationId(8),
    });

    let failure = check_restart_commit_and_configuration(&cluster, NodeId(1), &before, &after, &[])
        .expect_err("configuration identity change must fail PS-03.c");
    assert_eq!(failure.invariant(), catalog::PS_03_EXACT_DURABLE_RESTART);
    assert!(failure.message.contains("commit or configuration"));
}

#[test]
fn exact_restart_snapshot_oracle_detects_payload_identity_change() {
    let cluster = one_node_cluster();
    let before = durable_restart_fixture();
    let mut after = before.clone();
    after
        .snapshot
        .as_mut()
        .expect("fixture has snapshot")
        .application_payload_crc32 += 1;

    let failure = check_restart_snapshot(&cluster, NodeId(1), &before, &after, &[])
        .expect_err("snapshot identity change must fail PS-03.d");
    assert_eq!(failure.invariant(), catalog::PS_03_EXACT_DURABLE_RESTART);
    assert!(failure.message.contains("durable snapshot"));
}

#[test]
fn exact_restart_acknowledged_entry_oracle_detects_reindexing() {
    let cluster = one_node_cluster();
    let before = durable_restart_fixture();
    let mut after = before.clone();
    after.log[0].index = LogIndex(3);

    let failure = check_restart_acknowledged_entries(&cluster, NodeId(1), &before, &after, &[])
        .expect_err("reindexing an acknowledged entry must fail PS-03.e");
    assert_eq!(failure.invariant(), catalog::PS_03_EXACT_DURABLE_RESTART);
    assert!(failure.message.contains("lost or reindexed"));
}

#[test]
fn exact_durable_restart_detects_application_recovery_metadata_change() {
    let cluster = one_node_cluster();
    let before = durable_restart_fixture();
    let mut after = before.clone();
    after.applied_through = LogIndex(1);

    let failure = check_exact_durable_restart(
        &cluster,
        NodeId(1),
        &before,
        &after,
        before.applied_through,
        &[],
    )
    .expect_err("digest mismatch must fail PS-03");
    assert_eq!(failure.invariant(), catalog::PS_03_EXACT_DURABLE_RESTART);
    assert!(
        failure.message.contains("application recovery metadata"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn applied_floor_recovery_rejects_replay_at_or_below_floor() {
    let cluster = one_node_cluster();
    let recovered = [Applied {
        node_id: NodeId(1),
        application_epoch: 0,
        commit_index_at_emit: LogIndex(3),
        index: LogIndex(2),
        payload: b"already-applied".to_vec().into(),
    }];

    let failure = check_recovery_applied_floor_exclusion(
        &cluster,
        AppliedFloorRecovery {
            node_id: NodeId(1),
            applied_floor: LogIndex(2),
            commit_index: LogIndex(3),
            last_log_index: LogIndex(3),
            expected_replay: &[],
            recovered_applies: &recovered,
        },
        &[],
    )
    .expect_err("replay at durable floor must fail PS-04");
    assert_eq!(failure.invariant(), catalog::PS_04_APPLIED_FLOOR_RECOVERY);
    assert!(
        failure
            .message
            .contains("at or below durable applied floor"),
        "unexpected failure message: {}",
        failure.message
    );
}

#[test]
fn applied_floor_recovery_rejects_missing_committed_suffix_entry() {
    let cluster = one_node_cluster();
    let expected = [
        (LogIndex(3), b"three".to_vec().into()),
        (LogIndex(4), b"four".to_vec().into()),
    ];
    let recovered = [Applied {
        node_id: NodeId(1),
        application_epoch: 0,
        commit_index_at_emit: LogIndex(4),
        index: LogIndex(4),
        payload: b"four".to_vec().into(),
    }];

    let failure = check_recovery_exact_committed_suffix(
        &cluster,
        AppliedFloorRecovery {
            node_id: NodeId(1),
            applied_floor: LogIndex(2),
            commit_index: LogIndex(4),
            last_log_index: LogIndex(4),
            expected_replay: &expected,
            recovered_applies: &recovered,
        },
        &[],
    )
    .expect_err("omitting a committed suffix entry must fail PS-04.b");
    assert_eq!(failure.invariant(), catalog::PS_04_APPLIED_FLOOR_RECOVERY);
    assert!(failure
        .message
        .contains("expected [LogIndex(3), LogIndex(4)]"));
}

#[test]
fn pending_application_replay_restarts_through_the_instrumented_transition() {
    let config = NodeConfig::new(NodeId(1), Vec::new(), 3).expect("test config is valid");
    let mut state = ExplorationState::new(Cluster::new(vec![config]));
    let payload = b"pending-application-replay".to_vec();
    apply_pending_application_replay_seed(
        &mut state,
        PendingApplicationReplaySeed {
            node_id: NodeId(1),
            bootstrap: BootstrapState {
                current_term: Term(1),
                voted_for: None,
                commit_index: LogIndex(1),
                committed_configuration: None,
                snapshot: None,
                log: vec![BootstrapLogEntry::application(
                    LogIndex(1),
                    Term(1),
                    payload.clone(),
                )],
            },
        },
    )
    .expect("pending replay seed is valid");
    state.witness_seeded_commit_authority(LogIndex::ZERO, LogIndex(1), Term(1));

    assert!(state.cluster().applied().is_empty());
    restart_node(&mut state, NodeId(1), &[]).expect("restart replays the committed suffix");

    let recovered = state.cluster().applied();
    assert_eq!(recovered.len(), 1);
    assert_eq!(recovered[0].index, LogIndex(1));
    assert_eq!(recovered[0].payload.as_ref(), payload.as_slice());
    assert!(state
        .observation_set()
        .contains(Observation::RestartNonemptyExpectedReplayComparisons));
    assert_eq!(
        state
            .cluster()
            .durable_state_digest(NodeId(1))
            .applied_through,
        LogIndex(1)
    );
}

#[test]
fn applied_floor_recovery_rejects_floor_beyond_durable_bounds() {
    let cluster = one_node_cluster();
    let base = AppliedFloorRecovery {
        node_id: NodeId(1),
        applied_floor: LogIndex(4),
        commit_index: LogIndex(3),
        last_log_index: LogIndex(4),
        expected_replay: &[],
        recovered_applies: &[],
    };
    let commit_failure = check_recovery_applied_floor_bounds(&cluster, base, &[])
        .expect_err("floor beyond commit must fail PS-04.c");
    assert!(commit_failure.message.contains("exceeds commit index"));

    let log_failure = check_recovery_applied_floor_bounds(
        &cluster,
        AppliedFloorRecovery {
            commit_index: LogIndex(4),
            last_log_index: LogIndex(3),
            ..base
        },
        &[],
    )
    .expect_err("floor beyond log coverage must fail PS-04.c");
    assert!(log_failure.message.contains("exceeds local last log index"));
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
        application_epoch: 0,
        request_id: 7,
        read_index: LogIndex(3),
        local_applied_index: LogIndex(3),
    });

    let failure = check_read_grant_committed_floors(&cluster, &[])
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
        application_epoch: 0,
        request_id: 9,
        read_index: LogIndex(1),
        local_applied_index: LogIndex(1),
    });

    let failure = check_registered_read_grants(&cluster, &[])
        .expect_err("an unregistered grant must be detected");
    assert_eq!(
        failure.invariant(),
        catalog::RD_03_READ_BARRIER_COVERS_COMMITTED_FLOOR
    );
    assert!(failure.message.contains("never registered"));
}
