use std::num::NonZeroUsize;

use rafter_multiraft::managed::{AdmissionRejection as ManagedRejection, ManagedConfig};
use rafter_reference_sharded_counter::{
    adapter::{
        CounterAdmissionRejection, CounterSubmitOutcome, ManagedCounterCluster, NetworkConfig,
        ProposalReceipt, SessionSubmitOutcome,
    },
    ClientId, CounterCommand, CounterResult, Delta, GroupId, GroupIncarnation, GroupLifecycle,
    LifecycleOutcome, LifecycleRejection, LifecycleRequest, RequestFingerprint, RequestIdentity,
    Sequence, SessionEpoch, SystemClass, WorkQuota,
};

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test bounds are nonzero")
}

fn quota(value: u32) -> WorkQuota {
    WorkQuota::new(value).expect("test quota is nonzero")
}

fn cluster(
    group_queue: usize,
    global_queue: usize,
    network: usize,
    sessions: usize,
) -> ManagedCounterCluster {
    ManagedCounterCluster::new(
        ManagedConfig::new(
            nonzero(1),
            nonzero(group_queue),
            nonzero(global_queue),
            nonzero(1),
        )
        .expect("managed bounds are valid"),
        NetworkConfig {
            max_pending_messages: nonzero(network),
            max_sessions_per_group: nonzero(sessions),
        },
    )
}

fn create_serving(cluster: &mut ManagedCounterCluster, group_id: GroupId) {
    cluster
        .lifecycle(group_id, LifecycleRequest::Create { quota: quota(1) })
        .expect("group opens");
    cluster
        .lifecycle(group_id, LifecycleRequest::Recover)
        .expect("recovery starts");
    cluster.drive_until_idle(256).expect("election quiesces");
    cluster
        .lifecycle(group_id, LifecycleRequest::Serve)
        .expect("group serves");
}

fn request(sequence: u64, command: CounterCommand) -> RequestIdentity {
    RequestIdentity {
        client_id: ClientId::new(0),
        session_epoch: SessionEpoch::new(1).expect("epoch is nonzero"),
        sequence: Sequence::new(sequence).expect("sequence is nonzero"),
        fingerprint: RequestFingerprint::of(&command),
    }
}

fn open_session(cluster: &mut ManagedCounterCluster, group_id: GroupId) {
    assert!(matches!(
        cluster
            .open_session_for(
                group_id,
                GroupIncarnation::first(),
                ClientId::new(0),
                SessionEpoch::new(1).expect("epoch is nonzero"),
            )
            .expect("session admission succeeds"),
        SessionSubmitOutcome::Queued(_)
    ));
    cluster
        .drive_until_idle(256)
        .expect("session establishment commits");
}

fn queued(outcome: CounterSubmitOutcome) -> ProposalReceipt {
    let CounterSubmitOutcome::Queued(receipt) = outcome else {
        panic!("a new request must take one queue slot");
    };
    receipt
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LifecycleCell {
    Changed,
    Idempotent,
    Conflict,
    Unknown,
    Tombstoned,
}

fn cluster_at(lifecycle: Option<GroupLifecycle>) -> ManagedCounterCluster {
    let mut cluster = cluster(4, 8, 128, 1);
    let group_id = GroupId::new(0);
    let Some(lifecycle) = lifecycle else {
        return cluster;
    };
    cluster
        .lifecycle(group_id, LifecycleRequest::Create { quota: quota(1) })
        .expect("fixture group opens");
    match lifecycle {
        GroupLifecycle::Creating => {}
        GroupLifecycle::Recovering => {
            cluster
                .lifecycle(group_id, LifecycleRequest::Recover)
                .expect("fixture starts recovery");
        }
        GroupLifecycle::Serving => {
            cluster
                .lifecycle(group_id, LifecycleRequest::Recover)
                .expect("fixture starts recovery");
            cluster.drive_until_idle(256).expect("election quiesces");
            cluster
                .lifecycle(group_id, LifecycleRequest::Serve)
                .expect("fixture serves");
        }
        GroupLifecycle::Draining => {
            cluster
                .lifecycle(group_id, LifecycleRequest::Drain)
                .expect("fixture drains");
        }
        GroupLifecycle::Removed | GroupLifecycle::Tombstoned => {
            cluster
                .lifecycle(group_id, LifecycleRequest::Drain)
                .expect("fixture drains");
            cluster
                .lifecycle(group_id, LifecycleRequest::Remove)
                .expect("fixture removes");
            if lifecycle == GroupLifecycle::Tombstoned {
                cluster
                    .lifecycle(group_id, LifecycleRequest::Tombstone)
                    .expect("fixture tombstones");
            }
        }
    }
    cluster
}

fn expected_lifecycle_cell(
    current: Option<GroupLifecycle>,
    requested: GroupLifecycle,
) -> LifecycleCell {
    let Some(current) = current else {
        return if requested == GroupLifecycle::Creating {
            LifecycleCell::Changed
        } else {
            LifecycleCell::Unknown
        };
    };
    if current == requested {
        return LifecycleCell::Idempotent;
    }
    if current == GroupLifecycle::Tombstoned {
        return LifecycleCell::Tombstoned;
    }
    if matches!(
        (current, requested),
        (GroupLifecycle::Creating, GroupLifecycle::Recovering)
            | (GroupLifecycle::Recovering, GroupLifecycle::Serving)
            | (
                GroupLifecycle::Creating | GroupLifecycle::Recovering | GroupLifecycle::Serving,
                GroupLifecycle::Draining
            )
            | (GroupLifecycle::Draining, GroupLifecycle::Removed)
            | (
                GroupLifecycle::Removed,
                GroupLifecycle::Creating | GroupLifecycle::Tombstoned
            )
    ) {
        LifecycleCell::Changed
    } else {
        LifecycleCell::Conflict
    }
}

#[test]
fn every_real_lifecycle_table_cell_is_typed() {
    let states = [
        None,
        Some(GroupLifecycle::Creating),
        Some(GroupLifecycle::Recovering),
        Some(GroupLifecycle::Serving),
        Some(GroupLifecycle::Draining),
        Some(GroupLifecycle::Removed),
        Some(GroupLifecycle::Tombstoned),
    ];
    let requests = [
        LifecycleRequest::Create { quota: quota(1) },
        LifecycleRequest::Recover,
        LifecycleRequest::Serve,
        LifecycleRequest::Drain,
        LifecycleRequest::Remove,
        LifecycleRequest::Tombstone,
    ];
    for current in states {
        for request in requests {
            let outcome = cluster_at(current)
                .lifecycle(GroupId::new(0), request)
                .expect("policy cell is a value")
                .outcome;
            let actual = match outcome {
                LifecycleOutcome::Created { .. } | LifecycleOutcome::Applied { .. } => {
                    LifecycleCell::Changed
                }
                LifecycleOutcome::Idempotent { .. } => LifecycleCell::Idempotent,
                LifecycleOutcome::Rejected(LifecycleRejection::GroupUnknown) => {
                    LifecycleCell::Unknown
                }
                LifecycleOutcome::Rejected(LifecycleRejection::GroupTombstoned) => {
                    LifecycleCell::Tombstoned
                }
                LifecycleOutcome::Rejected(LifecycleRejection::Conflict { .. }) => {
                    LifecycleCell::Conflict
                }
                LifecycleOutcome::Rejected(other) => {
                    panic!("unexpected lifecycle refusal in table: {other:?}")
                }
            };
            assert_eq!(actual, expected_lifecycle_cell(current, request.target()));
        }
    }
}

#[test]
fn restored_maximum_incarnation_refuses_reopen_without_wrapping() {
    let group_id = GroupId::new(9);
    let mut source = cluster(2, 4, 32, 1);
    source
        .lifecycle(group_id, LifecycleRequest::Create { quota: quota(1) })
        .expect("group opens");
    source
        .lifecycle(group_id, LifecycleRequest::Drain)
        .expect("creating group drains");
    source
        .lifecycle(group_id, LifecycleRequest::Remove)
        .expect("empty group removes");
    let mut checkpoint = source
        .checkpoint_group(group_id)
        .expect("inactive identity checkpoints");
    checkpoint.incarnation =
        GroupIncarnation::new(u32::MAX).expect("maximum incarnation remains nonzero");

    let mut restored = cluster(2, 4, 32, 1);
    restored
        .restore_inactive_checkpoint(checkpoint, 1)
        .expect("inactive identity restores without a physical group");
    assert!(matches!(
        restored
            .lifecycle(group_id, LifecycleRequest::Create { quota: quota(1) })
            .expect("exhaustion is a policy value")
            .outcome,
        LifecycleOutcome::Rejected(LifecycleRejection::IncarnationExhausted)
    ));
    assert_eq!(
        restored.group_identity(group_id),
        Some((
            GroupIncarnation::new(u32::MAX).expect("maximum is nonzero"),
            GroupLifecycle::Removed,
        ))
    );
}

#[test]
fn per_group_pressure_preserves_completed_and_outstanding_retries() {
    let group_id = GroupId::new(0);
    let mut cluster = cluster(2, 4, 128, 1);
    create_serving(&mut cluster, group_id);
    open_session(&mut cluster, group_id);
    let add = CounterCommand::Add {
        delta: Delta::new(5).expect("delta is nonzero"),
    };
    let completed_request = request(1, add);
    queued(
        cluster
            .submit_for(group_id, GroupIncarnation::first(), completed_request, add)
            .expect("first command queues"),
    );
    cluster
        .drive_until_idle(256)
        .expect("first command completes");

    let pending_request = request(2, CounterCommand::Read);
    let pending = queued(
        cluster
            .submit_for(
                group_id,
                GroupIncarnation::first(),
                pending_request,
                CounterCommand::Read,
            )
            .expect("second command queues"),
    );
    cluster
        .submit_system(group_id, GroupIncarnation::first(), SystemClass::Bulk)
        .expect("second per-group slot fills");
    let overflow = cluster
        .submit_system(group_id, GroupIncarnation::first(), SystemClass::Snapshot)
        .expect_err("new work reaches the per-group bound");
    assert!(matches!(
        overflow.reason,
        CounterAdmissionRejection::Managed(ManagedRejection::GroupQueueFull {
            group_id: rejected,
            bound: 2,
        }) if rejected == group_id
    ));
    assert_eq!(
        cluster
            .submit_for(
                group_id,
                GroupIncarnation::first(),
                pending_request,
                CounterCommand::Read,
            )
            .expect("outstanding retry keeps its one queue slot"),
        CounterSubmitOutcome::AlreadyQueued(pending)
    );
    assert_eq!(
        cluster
            .submit_for(group_id, GroupIncarnation::first(), completed_request, add,)
            .expect("completed retry bypasses the full group queue"),
        CounterSubmitOutcome::Replayed(CounterResult::Added { value: 5 })
    );
    cluster
        .drive_until_idle(256)
        .expect("pressure clears without loss");
    let metrics = cluster.metrics();
    assert_eq!(metrics.admitted, metrics.serviced + metrics.failed);
}

#[test]
fn sustained_control_work_may_starve_bulk_only_until_control_stops() {
    let group_id = GroupId::new(0);
    let mut cluster = cluster(32, 32, 512, 1);
    create_serving(&mut cluster, group_id);
    let bulk = cluster
        .submit_system(group_id, GroupIncarnation::first(), SystemClass::Bulk)
        .expect("bulk queues first");

    for _ in 0..4 {
        cluster
            .submit_system(group_id, GroupIncarnation::first(), SystemClass::Control)
            .expect("sustained control work queues");
        let report = cluster.drive_round().expect("one pass executes");
        assert!(
            report
                .turns
                .iter()
                .flat_map(|turn| &turn.items)
                .all(|item| item.work_id != bulk.work_id),
            "bulk remains queued while higher-class work is sustained"
        );
    }
    let drained = cluster
        .drive_until_idle(256)
        .expect("bulk completes once control traffic stops");
    assert!(drained
        .turns
        .iter()
        .flat_map(|turn| &turn.items)
        .any(|item| item.work_id == bulk.work_id));
}
