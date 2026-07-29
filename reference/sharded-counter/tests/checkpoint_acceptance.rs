use std::num::NonZeroUsize;

use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    LocalProposalId, LogIndex, NodeConfig, NodeId, Output as RaftOutput, RaftSnapshot,
    RaftSnapshotMetadata, SnapshotGroupId, Term,
};
use rafter_app::{
    group::RaftGroup,
    state_machine::{ApplyBatch, ApplyEntry, ReplicatedStateMachine},
};
use rafter_multiraft::managed::{ManagedConfig, WorkClass};
use rafter_reference_sharded_counter::{
    adapter::{
        CounterApplyResult, CounterSubmitOutcome, ManagedCounterCluster, NetworkConfig,
        ReplicatedCounterCommand, SessionSubmitOutcome,
    },
    ClientId, CounterCommand, CounterResult, Delta, GroupId, GroupIncarnation, GroupLifecycle,
    LifecycleOutcome, LifecycleRequest, RequestFingerprint, RequestIdentity, Sequence,
    SessionEpoch, SystemClass, WorkQuota,
};
use rafter_runtime::DurableRaftNode;
use rafter_storage::InMemoryRaftHardStateStore;

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test bound is nonzero")
}

fn request(sequence: u64, command: CounterCommand) -> RequestIdentity {
    RequestIdentity {
        client_id: ClientId::new(0),
        session_epoch: SessionEpoch::new(1).expect("epoch is nonzero"),
        sequence: Sequence::new(sequence).expect("sequence is nonzero"),
        fingerprint: RequestFingerprint::of(&command),
    }
}

fn serving_cluster(group_queue: usize, quota: u32) -> ManagedCounterCluster {
    let mut cluster = ManagedCounterCluster::new(
        ManagedConfig::new(
            nonzero(2),
            nonzero(group_queue),
            nonzero(group_queue * 2),
            nonzero(4),
        )
        .expect("managed bounds are valid"),
        NetworkConfig {
            max_pending_messages: nonzero(1_024),
            max_sessions_per_group: nonzero(4),
        },
    );
    let group_id = GroupId::new(0);
    assert!(matches!(
        cluster
            .lifecycle(
                group_id,
                LifecycleRequest::Create {
                    quota: WorkQuota::new(quota).expect("quota is nonzero"),
                },
            )
            .expect("group opens")
            .outcome,
        LifecycleOutcome::Created { .. }
    ));
    cluster
        .lifecycle(group_id, LifecycleRequest::Recover)
        .expect("recovery starts");
    cluster.drive_until_idle(256).expect("election quiesces");
    assert!(matches!(
        cluster
            .lifecycle(group_id, LifecycleRequest::Serve)
            .expect("group serves")
            .outcome,
        LifecycleOutcome::Applied {
            to: GroupLifecycle::Serving,
            ..
        }
    ));
    assert!(matches!(
        cluster
            .open_session_for(
                group_id,
                GroupIncarnation::first(),
                ClientId::new(0),
                SessionEpoch::new(1).expect("epoch is nonzero"),
            )
            .expect("session queues"),
        SessionSubmitOutcome::Queued(_)
    ));
    cluster.drive_until_idle(256).expect("session commits");
    cluster
}

fn snapshot_descriptor(index: LogIndex, payload: &[u8]) -> RaftSnapshot {
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("counter-group-0").expect("snapshot group id is valid"),
        NodeId(1),
        index,
        Term(1),
        Term(1),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("counter-v1").expect("snapshot kind is valid"),
            ApplicationSnapshotVersion::new(1).expect("snapshot version is nonzero"),
        ),
    )
    .expect("snapshot metadata is valid");
    RaftSnapshot::from_payload(metadata, payload)
}

#[test]
fn composite_checkpoint_preserves_application_and_outstanding_policy_state() {
    let group_id = GroupId::new(0);
    let mut cluster = serving_cluster(8, 2);
    let add = CounterCommand::Add {
        delta: Delta::new(13).expect("delta is nonzero"),
    };
    let first = cluster
        .submit_for(group_id, GroupIncarnation::first(), request(1, add), add)
        .expect("first request queues");
    assert!(matches!(first, CounterSubmitOutcome::Queued(_)));
    cluster
        .drive_until_idle(256)
        .expect("first request commits");

    let read = CounterCommand::Read;
    let outstanding = cluster
        .submit_for(group_id, GroupIncarnation::first(), request(2, read), read)
        .expect("second request is accepted");
    let CounterSubmitOutcome::Queued(outstanding_receipt) = outstanding else {
        panic!("new request takes a queue slot");
    };

    let checkpoint = cluster
        .checkpoint_group(group_id)
        .expect("composite checkpoint builds");
    assert_eq!(checkpoint.incarnation, GroupIncarnation::first());
    assert_eq!(checkpoint.lifecycle, GroupLifecycle::Serving);
    assert_eq!(checkpoint.quota.get(), 2);
    assert_eq!(checkpoint.sessions.len(), 1);
    assert_eq!(
        checkpoint.sessions[0]
            .outstanding
            .expect("accepted read remains explicit")
            .receipt,
        outstanding_receipt
    );
    let restored = checkpoint
        .restore(4)
        .expect("exact application snapshot installs");
    let view = restored.state_machine.view();
    assert_eq!(view.value, 13);
    assert!(view.applied_index > LogIndex::ZERO);
    assert_eq!(view.sessions.len(), 1);
    assert_eq!(
        view.sessions[0]
            .completed
            .expect("dedup completion survives")
            .result,
        CounterResult::Added { value: 13 }
    );
    assert_eq!(
        restored.sessions[0]
            .outstanding
            .expect("policy restart retains accepted request")
            .sequence,
        Sequence::new(2).expect("sequence is nonzero")
    );

    let mut restored_machine = restored.state_machine;
    let replay = restored_machine
        .apply_batch(ApplyBatch {
            entries: vec![ApplyEntry {
                index: LogIndex(view.applied_index.0 + 1),
                term: Term(2),
                command: ReplicatedCounterCommand::Counter {
                    request: request(1, add),
                    command: add,
                },
                local_proposal_id: Some(LocalProposalId(99)),
            }],
        })
        .expect("exact retry applies as a replay");
    assert_eq!(
        replay[0].result,
        CounterApplyResult::Counter(CounterResult::Added { value: 13 })
    );
    assert_eq!(
        restored_machine.view().value,
        13,
        "snapshot/compaction never makes an acknowledged request executable again"
    );
}

#[test]
fn rafter_snapshot_apply_restores_counter_value_sessions_and_dedup() {
    let mut source = rafter_reference_sharded_counter::adapter::CounterStateMachine::new(4);
    let add = CounterCommand::Add {
        delta: Delta::new(17).expect("delta is nonzero"),
    };
    source
        .apply_batch(ApplyBatch {
            entries: vec![
                ApplyEntry {
                    index: LogIndex(1),
                    term: Term(1),
                    command: ReplicatedCounterCommand::OpenSession {
                        client_id: ClientId::new(0),
                        epoch: SessionEpoch::new(1).expect("epoch is nonzero"),
                    },
                    local_proposal_id: None,
                },
                ApplyEntry {
                    index: LogIndex(2),
                    term: Term(1),
                    command: ReplicatedCounterCommand::Counter {
                        request: request(1, add),
                        command: add,
                    },
                    local_proposal_id: None,
                },
            ],
        })
        .expect("source state applies");
    let expected = source.view();
    let application = source
        .build_snapshot(LogIndex(2))
        .expect("source builds exact counter snapshot");
    let descriptor = snapshot_descriptor(application.applied_index, &application.payload);

    let mut follower = rafter_reference_sharded_counter::adapter::CounterStateMachine::new(4);
    follower
        .register_promoted_snapshot(&descriptor, application.payload)
        .expect("promoted payload matches its descriptor");
    let config = NodeConfig::new(NodeId(1), Vec::new(), 1)
        .expect("single-node snapshot fixture is valid")
        .with_pre_vote(false);
    let runtime = DurableRaftNode::new(config, InMemoryRaftHardStateStore::new())
        .expect("fresh runtime opens");
    let mut group = RaftGroup::new(GroupId::new(0), NodeId(1), runtime, follower);
    let report = group
        .apply_raft_outputs(vec![RaftOutput::ApplySnapshot {
            snapshot: descriptor.clone(),
        }])
        .expect("Rafter descriptor-based snapshot installs");

    assert_eq!(report.snapshot_events.len(), 1);
    assert_eq!(group.state_machine().view(), expected);
    assert_eq!(
        group.runtime().snapshot_index(),
        LogIndex::ZERO,
        "the raw app-layer output pump does not invent runtime persistence"
    );
}

#[test]
fn snapshot_heavy_group_never_removes_another_ready_group_from_its_pass() {
    let mut cluster = serving_cluster(40, 4);
    let second = GroupId::new(1);
    cluster
        .lifecycle(
            second,
            LifecycleRequest::Create {
                quota: WorkQuota::new(1).expect("quota is nonzero"),
            },
        )
        .expect("second group opens");
    cluster
        .lifecycle(second, LifecycleRequest::Recover)
        .expect("second group recovers");
    cluster
        .drive_until_idle(256)
        .expect("second election quiesces");
    cluster
        .lifecycle(second, LifecycleRequest::Serve)
        .expect("second group serves");

    for _ in 0..32 {
        cluster
            .submit_system(
                GroupId::new(0),
                GroupIncarnation::first(),
                SystemClass::Snapshot,
            )
            .expect("bounded snapshot pressure queues");
    }
    let cold = cluster
        .submit_system(second, GroupIncarnation::first(), SystemClass::Bulk)
        .expect("cold group work queues");
    let report = cluster
        .drive_until_idle(512)
        .expect("snapshot-heavy profile quiesces");
    assert_eq!(report.plans.first(), Some(&vec![GroupId::new(0), second]));
    let first_pass = report.turns[0].pass_id;
    assert!(report.turns.iter().any(|turn| {
        turn.pass_id == first_pass
            && turn.group_id == second
            && turn
                .items
                .iter()
                .any(|item| item.work_id == cold.work_id && item.class == WorkClass::Bulk)
    }));
    let snapshot_items = report
        .turns
        .iter()
        .flat_map(|turn| &turn.items)
        .filter(|item| item.class == WorkClass::Snapshot)
        .count();
    assert_eq!(snapshot_items, 32);
    let metrics = cluster.metrics();
    assert_eq!(metrics.admitted, metrics.serviced + metrics.failed);
    println!(
        "snapshot_items={snapshot_items} passes={} opportunities={}",
        report.plans.len(),
        report.opportunities
    );
}
