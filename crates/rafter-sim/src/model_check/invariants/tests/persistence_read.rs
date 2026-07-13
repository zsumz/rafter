use super::super::client::{check_read_grant_committed_floors, check_registered_read_grants};
use super::super::persistence::{
    check_recovery_applied_floor_bounds, check_recovery_applied_floor_exclusion,
    check_recovery_exact_committed_suffix, check_restart_acknowledged_entries,
    check_restart_commit_and_configuration, check_restart_log, check_restart_snapshot,
    check_restart_term_and_vote,
};
use super::*;
use crate::model_check::helpers::elect_node_one_in_state;
use crate::model_check::observations::Observation;
use crate::model_check::scheduling::Operation;
use crate::model_check::state::{
    apply_pending_application_replay_seed, apply_to_state, restart_node,
    PendingApplicationReplaySeed,
};
use crate::model_check::ProposalId;
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
            application_payload: b"data".to_vec(),
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
fn exact_restart_snapshot_oracle_detects_crc32_collision_payload_change() {
    let cluster = one_node_cluster();
    let first_payload = [0x29, 0x2c, 0x99, 0xbf, 0xb5, 0xb8, 0x20, 0xb7];
    let second_payload = [0x11, 0x98, 0x3d, 0x82, 0xcb, 0x0b, 0xeb, 0xd2];
    let (first_snapshot, _) = test_snapshot(1, 1, 1, 2, &first_payload);
    let (second_snapshot, _) = test_snapshot(1, 1, 1, 2, &second_payload);
    assert_eq!(
        first_snapshot, second_snapshot,
        "fixture payloads must collide in the descriptor's CRC32 identity"
    );

    let mut before = durable_restart_fixture();
    let before_snapshot = before.snapshot.as_mut().expect("fixture has snapshot");
    before_snapshot.transfer_id = first_snapshot.transfer_id();
    before_snapshot.last_included_index = first_snapshot.metadata.last_included_index;
    before_snapshot.last_included_term = first_snapshot.metadata.last_included_term;
    before_snapshot.hard_state_term = first_snapshot.metadata.hard_state_term;
    before_snapshot.application_payload_len = first_snapshot.application_payload_len;
    before_snapshot.application_payload_crc32 = first_snapshot.application_payload_crc32;
    before_snapshot.application_payload = first_payload.to_vec();
    let mut after = before.clone();
    after
        .snapshot
        .as_mut()
        .expect("fixture has snapshot")
        .application_payload = second_payload.to_vec();

    let failure = check_restart_snapshot(&cluster, NodeId(1), &before, &after, &[])
        .expect_err("CRC32-colliding snapshot bytes must fail PS-03.d");
    assert_eq!(failure.invariant(), catalog::PS_03_EXACT_DURABLE_RESTART);
    assert!(failure.message.contains("durable snapshot"));
}

#[test]
fn exact_restart_snapshot_reopens_independent_store_with_exact_payload() {
    let mut state = ExplorationState::new(one_node_cluster());
    let (snapshot, payload) = test_snapshot(1, 1, 1, 2, b"durable snapshot payload");
    state.inject_snapshot_payload(NodeId(1), &snapshot, payload.clone());
    let bootstrap = bootstrap_with_snapshot(Term(2), snapshot.clone(), &[]);
    state
        .inject_bootstrap_state(NodeId(1), bootstrap)
        .expect("snapshot bootstrap opens");
    let before = state
        .cluster()
        .durable_state_digest(NodeId(1))
        .expect("opened fixture has a complete durable image");
    let before_payload_address = state
        .cluster()
        .snapshot_payload(NodeId(1), &snapshot)
        .expect("opened store has payload")
        .as_ptr();

    restart_node(&mut state, NodeId(1), &[]).expect("snapshot bootstrap reopens");

    let after_payload = state
        .cluster()
        .snapshot_payload(NodeId(1), &snapshot)
        .expect("reopened store has payload");
    assert_ne!(
        before_payload_address,
        after_payload.as_ptr(),
        "restart must hydrate a distinct snapshot-store allocation"
    );
    assert_eq!(after_payload, payload);
    let after = state
        .cluster()
        .durable_state_digest(NodeId(1))
        .expect("reopened fixture has a complete durable image");
    check_restart_snapshot(state.cluster(), NodeId(1), &before, &after, &[])
        .expect("separately reopened exact bytes satisfy PS-03.d");
}

#[test]
fn exact_restart_acknowledged_entry_oracle_detects_reindexing() {
    let state = state_with_acknowledged_uncommitted_entry();
    let before = durable_restart_fixture();
    let mut after = before.clone();
    after.log[0].index = LogIndex(3);

    let failure =
        check_restart_acknowledged_entries(state.cluster(), NodeId(2), &before, &after, &[])
            .expect_err("reindexing an acknowledged entry must fail PS-03.e");
    assert_eq!(failure.invariant(), catalog::PS_03_EXACT_DURABLE_RESTART);
    assert!(failure.message.contains("lost or reindexed"));
}

#[test]
fn exact_restart_acknowledged_entry_oracle_checks_acknowledged_uncommitted_entry() {
    let state = state_with_acknowledged_uncommitted_entry();
    let before = state
        .cluster()
        .durable_state_digest(NodeId(2))
        .expect("acknowledgement fixture has a complete durable image");
    let acknowledged_floor = state.cluster().delivered_ack_floor(NodeId(2));
    let mut after = before.clone();
    after.log.retain(|entry| entry.index != acknowledged_floor);
    assert!(acknowledged_floor > before.commit_index);
    assert_eq!(after.log.len() + 1, before.log.len());

    let failure =
        check_restart_acknowledged_entries(state.cluster(), NodeId(2), &before, &after, &[])
            .expect_err("losing an acknowledged but uncommitted entry must fail PS-03.e");
    assert_eq!(failure.invariant(), catalog::PS_03_EXACT_DURABLE_RESTART);
    assert!(failure.message.contains("lost or reindexed"));
}

fn state_with_acknowledged_uncommitted_entry() -> ExplorationState {
    let mut state = ExplorationState::new(two_node_cluster());
    elect_node_one_in_state(&mut state);
    apply_to_state(
        &mut state,
        Operation::Propose {
            to: NodeId(1),
            proposal_id: ProposalId(1),
            stale_leader: false,
        },
    );
    deliver_matching_in_state(&mut state, |envelope| {
        envelope.from == NodeId(1)
            && envelope.to == NodeId(2)
            && matches!(envelope.message, Message::AppendEntries(_))
    });
    deliver_matching_in_state(&mut state, |envelope| {
        envelope.from == NodeId(2)
            && envelope.to == NodeId(1)
            && matches!(
                envelope.message,
                Message::AppendEntriesResponse(ref response) if response.success
            )
    });
    assert!(
        state.cluster().delivered_ack_floor(NodeId(2)) > state.cluster().commit_index(NodeId(2))
    );
    state
}

fn deliver_matching_in_state(state: &mut ExplorationState, predicate: impl Fn(&Envelope) -> bool) {
    let position = state
        .cluster()
        .pending()
        .position(predicate)
        .expect("fixture message is queued");
    apply_to_state(state, Operation::DeliverReadyAt(position));
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
    let (cluster, expected, recovered) = recorded_mixed_recovery();

    let failure = check_recovery_applied_floor_exclusion(
        &cluster,
        AppliedFloorRecovery {
            node_id: NodeId(1),
            application_epoch: 0,
            applied_floor: LogIndex(1),
            commit_index: LogIndex(3),
            last_log_index: LogIndex(3),
            expected_replay: &expected,
            recovered_execution: &recovered,
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
    let (cluster, expected, mut recovered) = recorded_mixed_recovery();
    recovered.remove(1);

    let failure = check_recovery_exact_committed_suffix(
        &cluster,
        AppliedFloorRecovery {
            node_id: NodeId(1),
            application_epoch: 0,
            applied_floor: LogIndex::ZERO,
            commit_index: LogIndex(3),
            last_log_index: LogIndex(3),
            expected_replay: &expected,
            recovered_execution: &recovered,
        },
        &[],
    )
    .expect_err("omitting a committed suffix entry must fail PS-04.b");
    assert_eq!(failure.invariant(), catalog::PS_04_APPLIED_FLOOR_RECOVERY);
    assert!(failure.message.contains("replayed logical entries"));
    assert!(failure.message.contains("Configuration"));
}

#[test]
fn pending_application_replay_restarts_through_the_instrumented_transition() {
    let config = NodeConfig::new(NodeId(1), Vec::new(), 3).expect("test config is valid");
    let mut state = ExplorationState::new(Cluster::new(vec![config]));
    let bootstrap = mixed_replay_bootstrap();
    apply_pending_application_replay_seed(
        &mut state,
        PendingApplicationReplaySeed {
            node_id: NodeId(1),
            bootstrap,
        },
    )
    .expect("pending replay seed is valid");
    state.witness_seeded_commit_authority(LogIndex::ZERO, LogIndex(3), Term(1));

    assert!(state.cluster().applied().is_empty());
    restart_node(&mut state, NodeId(1), &[]).expect("restart replays the committed suffix");

    let recovered = state.cluster().execution_history();
    assert_eq!(recovered.len(), 3);
    assert_eq!(recovered[0].entry.kind, LogEntryKind::Noop);
    assert!(matches!(
        recovered[1].entry.kind,
        LogEntryKind::Configuration(_)
    ));
    assert_eq!(
        recovered[2].entry.kind.application_payload(),
        Some(b"pending-application-replay".as_slice())
    );
    assert!(state
        .observation_set()
        .contains(Observation::RestartNonemptyExpectedReplayComparisons));
    assert_eq!(
        state
            .cluster()
            .durable_state_digest(NodeId(1))
            .expect("recovery fixture has a complete durable image")
            .applied_through,
        LogIndex(3)
    );
}

#[test]
fn applied_floor_recovery_rejects_floor_beyond_durable_bounds() {
    let (cluster, expected, recovered) = recorded_mixed_recovery();
    let base = AppliedFloorRecovery {
        node_id: NodeId(1),
        application_epoch: 0,
        applied_floor: LogIndex(4),
        commit_index: LogIndex(3),
        last_log_index: LogIndex(3),
        expected_replay: &expected,
        recovered_execution: &recovered,
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

fn recorded_mixed_recovery() -> (Cluster, Vec<ExecutedLogEntry>, Vec<ExecutionWitness>) {
    let mut cluster = one_node_cluster();
    let bootstrap = mixed_replay_bootstrap();
    let expected = bootstrap
        .log
        .iter()
        .map(|entry| ExecutedLogEntry {
            index: entry.index,
            term: entry.term,
            kind: entry.kind.clone(),
        })
        .collect::<Vec<_>>();
    cluster
        .seed_pending_application_replay(NodeId(1), bootstrap.clone())
        .expect("mixed pending replay seed is valid");
    let history_start = cluster.execution_history().len();
    cluster
        .restart_node_from_bootstrap(NodeId(1), bootstrap)
        .expect("mixed committed suffix reopens");
    let recovered = cluster.execution_history()[history_start..].to_vec();
    assert_eq!(recovered.len(), expected.len());
    (cluster, expected, recovered)
}

fn mixed_replay_bootstrap() -> BootstrapState {
    let membership = MembershipSet::new(vec![NodeId(1), NodeId(2), NodeId(3)], Vec::new())
        .expect("mixed recovery membership is valid");
    let configuration = ConfigurationEntry::stable(ConfigurationId(7), membership);
    BootstrapState {
        current_term: Term(1),
        voted_for: None,
        commit_index: LogIndex(3),
        committed_configuration: Some(CommittedConfiguration {
            index: LogIndex(2),
            config_id: ConfigurationId(7),
        }),
        snapshot: None,
        log: vec![
            BootstrapLogEntry::noop(LogIndex(1), Term(1)),
            BootstrapLogEntry::configuration(LogIndex(2), Term(1), configuration),
            BootstrapLogEntry::application(
                LogIndex(3),
                Term(1),
                b"pending-application-replay".to_vec(),
            ),
        ],
    }
}

#[test]
fn read_barrier_invariant_detects_grant_below_registration_floor() {
    let mut cluster = one_node_cluster();
    cluster.read_registrations.push(crate::ReadRegistered {
        node_id: NodeId(1),
        operation_id: 0,
        request_id: 7,
        committed_floor: LogIndex(5),
    });
    cluster.read_grants.push(crate::ReadGranted {
        node_id: NodeId(1),
        operation_id: Some(0),
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
        operation_id: None,
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

#[test]
fn read_barrier_invariant_uses_the_exact_reused_id_generation() {
    let mut cluster = one_node_cluster();
    cluster.read_registrations.extend([
        crate::ReadRegistered {
            node_id: NodeId(1),
            operation_id: 0,
            request_id: 7,
            committed_floor: LogIndex(1),
        },
        crate::ReadRegistered {
            node_id: NodeId(1),
            operation_id: 1,
            request_id: 7,
            committed_floor: LogIndex(5),
        },
    ]);
    cluster.read_grants.extend([
        crate::ReadGranted {
            node_id: NodeId(1),
            operation_id: Some(0),
            application_epoch: 0,
            request_id: 7,
            read_index: LogIndex(1),
            local_applied_index: LogIndex(1),
        },
        crate::ReadGranted {
            node_id: NodeId(1),
            operation_id: Some(1),
            application_epoch: 0,
            request_id: 7,
            read_index: LogIndex(3),
            local_applied_index: LogIndex(3),
        },
    ]);

    let failure = check_read_grant_committed_floors(&cluster, &[])
        .expect_err("the reused ID must retain the newer registration floor");
    assert!(failure.message.contains("below the committed floor 5"));
}
