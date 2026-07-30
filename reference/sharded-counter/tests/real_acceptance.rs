use std::num::NonZeroUsize;

use rafter_multiraft::managed::{ManagedConfig, WorkClass as ManagedClass};
use rafter_reference_sharded_counter::{
    adapter::{
        audit_acceptance, AcceptanceExpectation, CounterAdmissionRejection, CounterSubmitOutcome,
        ExpectedWork, ManagedCounterCluster, NetworkConfig, ProposalReceipt, SessionSubmitOutcome,
    },
    AdmissionRejection, ClientId, CounterCommand, CounterResult, Delta, GroupId, GroupIncarnation,
    GroupLifecycle, LifecycleOutcome, LifecycleRejection, LifecycleRequest, RequestFingerprint,
    RequestIdentity, Sequence, SessionEpoch, SystemClass, WorkQuota,
};

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test bounds are nonzero")
}

fn quota(value: u32) -> WorkQuota {
    WorkQuota::new(value).expect("test quota is nonzero")
}

fn epoch(value: u64) -> SessionEpoch {
    SessionEpoch::new(value).expect("test epoch is nonzero")
}

fn sequence(value: u64) -> Sequence {
    Sequence::new(value).expect("test sequence is nonzero")
}

fn request(
    client: u32,
    epoch_value: u64,
    sequence_value: u64,
    command: CounterCommand,
) -> RequestIdentity {
    RequestIdentity {
        client_id: ClientId::new(client),
        session_epoch: epoch(epoch_value),
        sequence: sequence(sequence_value),
        fingerprint: RequestFingerprint::of(&command),
    }
}

fn cluster(
    workers: usize,
    group_queue: usize,
    global_queue: usize,
    default_quota: usize,
    network: usize,
    sessions: usize,
) -> ManagedCounterCluster {
    ManagedCounterCluster::new(
        ManagedConfig::new(
            nonzero(workers),
            nonzero(group_queue),
            nonzero(global_queue),
            nonzero(default_quota),
        )
        .expect("managed bounds are valid"),
        NetworkConfig {
            max_pending_messages: nonzero(network),
            max_sessions_per_group: nonzero(sessions),
        },
    )
}

fn create_serving(cluster: &mut ManagedCounterCluster, group_id: GroupId, work_quota: u32) {
    assert!(matches!(
        cluster
            .lifecycle(
                group_id,
                LifecycleRequest::Create {
                    quota: quota(work_quota),
                },
            )
            .expect("physical group opens")
            .outcome,
        LifecycleOutcome::Created { .. }
    ));
    assert!(matches!(
        cluster
            .lifecycle(group_id, LifecycleRequest::Recover)
            .expect("recovery tick queues")
            .outcome,
        LifecycleOutcome::Applied {
            to: GroupLifecycle::Recovering,
            ..
        }
    ));
}

fn finish_recovery(cluster: &mut ManagedCounterCluster, groups: &[GroupId]) {
    cluster
        .drive_until_idle(512)
        .expect("deterministic elections quiesce");
    for group_id in groups {
        assert!(matches!(
            cluster
                .lifecycle(*group_id, LifecycleRequest::Serve)
                .expect("recovered group serves")
                .outcome,
            LifecycleOutcome::Applied {
                to: GroupLifecycle::Serving,
                ..
            }
        ));
    }
}

fn open_session(cluster: &mut ManagedCounterCluster, group_id: GroupId, client: u32) {
    assert!(matches!(
        cluster
            .open_session_for(
                group_id,
                GroupIncarnation::first(),
                ClientId::new(client),
                epoch(1),
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

fn start_recovery_and_capture_peer(
    cluster: &mut ManagedCounterCluster,
    group_id: GroupId,
) -> rafter_reference_sharded_counter::adapter::RoutedPeerEnvelope {
    assert!(matches!(
        cluster
            .lifecycle(group_id, LifecycleRequest::Create { quota: quota(2) },)
            .expect("physical group opens")
            .outcome,
        LifecycleOutcome::Created { .. }
    ));

    assert!(matches!(
        cluster
            .lifecycle(group_id, LifecycleRequest::Create { quota: quota(2) },)
            .expect("idempotent create is a value")
            .outcome,
        LifecycleOutcome::Idempotent {
            state: GroupLifecycle::Creating,
            ..
        }
    ));
    assert!(matches!(
        cluster
            .lifecycle(
                group_id,
                LifecycleRequest::Create { quota: quota(3) },
            )
            .expect("quota conflict is a value")
            .outcome,
        LifecycleOutcome::Rejected(LifecycleRejection::QuotaConflict { current })
            if current == quota(2)
    ));
    assert!(matches!(
        cluster
            .lifecycle(group_id, LifecycleRequest::Serve)
            .expect("invalid edge is a value")
            .outcome,
        LifecycleOutcome::Rejected(LifecycleRejection::Conflict {
            current: GroupLifecycle::Creating,
            requested: GroupLifecycle::Serving,
        })
    ));
    assert!(matches!(
        cluster
            .lifecycle(group_id, LifecycleRequest::Recover)
            .expect("recovery starts")
            .outcome,
        LifecycleOutcome::Applied {
            to: GroupLifecycle::Recovering,
            ..
        }
    ));
    cluster
        .drive_round()
        .expect("one recovery round emits peer traffic");
    let stale_peer = cluster
        .take_pending_peer()
        .expect("three-node election emits more than one peer envelope");
    finish_recovery(cluster, &[group_id]);
    assert!(matches!(
        cluster
            .lifecycle(group_id, LifecycleRequest::Serve)
            .expect("serve retry is idempotent")
            .outcome,
        LifecycleOutcome::Idempotent {
            state: GroupLifecycle::Serving,
            ..
        }
    ));
    stale_peer
}

fn drain_first_incarnation(cluster: &mut ManagedCounterCluster, group_id: GroupId) {
    open_session(cluster, group_id, 0);
    let command = CounterCommand::Add {
        delta: Delta::new(9).expect("delta is nonzero"),
    };
    queued(
        cluster
            .submit_for(
                group_id,
                GroupIncarnation::first(),
                request(0, 1, 1, command),
                command,
            )
            .expect("command queues"),
    );
    assert!(matches!(
        cluster
            .lifecycle(group_id, LifecycleRequest::Drain)
            .expect("drain applies")
            .outcome,
        LifecycleOutcome::Applied {
            to: GroupLifecycle::Draining,
            ..
        }
    ));
    assert!(matches!(
        cluster
            .lifecycle(group_id, LifecycleRequest::Remove)
            .expect("remove refusal is a value")
            .outcome,
        LifecycleOutcome::Rejected(LifecycleRejection::QueueNotDrained { pending: 1 })
    ));
    cluster
        .drive_until_idle(256)
        .expect("healthy accepted work drains by service");
    assert!(matches!(
        cluster
            .lifecycle(group_id, LifecycleRequest::Remove)
            .expect("empty group removes")
            .outcome,
        LifecycleOutcome::Applied {
            to: GroupLifecycle::Removed,
            ..
        }
    ));
}

fn assert_reopened_incarnation_is_fenced(
    cluster: &mut ManagedCounterCluster,
    group_id: GroupId,
    stale_peer: rafter_reference_sharded_counter::adapter::RoutedPeerEnvelope,
) {
    assert!(matches!(
        cluster
            .lifecycle(
                group_id,
                LifecycleRequest::Create { quota: quota(1) },
            )
            .expect("removed slot reopens")
            .outcome,
        LifecycleOutcome::Created { incarnation } if incarnation.get() == 2
    ));
    cluster
        .enqueue_peer(stale_peer)
        .expect("one captured envelope fits the bounded network");
    let late_peer = cluster.drive_round().expect("late peer traffic is a value");
    assert!(matches!(
        late_peer.refused_peer_traffic.as_slice(),
        [refusal]
            if refusal.group_id == group_id
                && refusal.incarnation == GroupIncarnation::first()
                && matches!(
                    refusal.reason,
                    AdmissionRejection::StaleIncarnation { current } if current.get() == 2
                )
    ));
    let stale = cluster
        .submit_for(
            group_id,
            GroupIncarnation::first(),
            request(0, 1, 2, CounterCommand::Read),
            CounterCommand::Read,
        )
        .expect_err("old-incarnation client traffic is fenced");
    assert!(matches!(
        stale.reason,
        CounterAdmissionRejection::Policy(AdmissionRejection::StaleIncarnation { current })
            if current.get() == 2
    ));
    let future = cluster
        .submit_for(
            group_id,
            GroupIncarnation::new(3).expect("future incarnation is nonzero"),
            request(0, 1, 2, CounterCommand::Read),
            CounterCommand::Read,
        )
        .expect_err("future client traffic is distinct");
    assert!(matches!(
        future.reason,
        CounterAdmissionRejection::Policy(AdmissionRejection::FutureIncarnation { current })
            if current.get() == 2
    ));
}

fn tombstone_reopened_group(cluster: &mut ManagedCounterCluster, group_id: GroupId) {
    assert!(matches!(
        cluster
            .lifecycle(group_id, LifecycleRequest::Drain)
            .expect("creating group may drain")
            .outcome,
        LifecycleOutcome::Applied { .. }
    ));
    assert!(matches!(
        cluster
            .lifecycle(group_id, LifecycleRequest::Remove)
            .expect("empty reopened group removes")
            .outcome,
        LifecycleOutcome::Applied { .. }
    ));
    assert!(matches!(
        cluster
            .lifecycle(group_id, LifecycleRequest::Tombstone)
            .expect("removed slot tombstones")
            .outcome,
        LifecycleOutcome::Applied {
            to: GroupLifecycle::Tombstoned,
            ..
        }
    ));
    assert!(matches!(
        cluster
            .lifecycle(group_id, LifecycleRequest::Create { quota: quota(1) },)
            .expect("tombstone refusal is a value")
            .outcome,
        LifecycleOutcome::Rejected(LifecycleRejection::GroupTombstoned)
    ));
}

#[test]
fn real_lifecycle_is_complete_idempotent_and_incarnation_safe() {
    let mut cluster = cluster(2, 8, 16, 2, 256, 4);
    let group_id = GroupId::new(7);
    let stale_peer = start_recovery_and_capture_peer(&mut cluster, group_id);
    drain_first_incarnation(&mut cluster, group_id);
    assert_reopened_incarnation_is_fenced(&mut cluster, group_id, stale_peer);
    tombstone_reopened_group(&mut cluster, group_id);
}

#[test]
fn retries_are_lossless_before_queue_bounds() {
    let mut cluster = cluster(1, 2, 2, 1, 128, 2);
    let groups = [GroupId::new(0), GroupId::new(1)];
    for group_id in groups {
        create_serving(&mut cluster, group_id, 1);
    }
    finish_recovery(&mut cluster, &groups);
    for group_id in groups {
        open_session(&mut cluster, group_id, 0);
    }

    let add = CounterCommand::Add {
        delta: Delta::new(4).expect("delta is nonzero"),
    };
    let first_request = request(0, 1, 1, add);
    let first = queued(
        cluster
            .submit_for(groups[0], GroupIncarnation::first(), first_request, add)
            .expect("first request queues"),
    );
    assert_eq!(
        cluster
            .submit_for(groups[0], GroupIncarnation::first(), first_request, add)
            .expect("outstanding retry is recognized"),
        CounterSubmitOutcome::AlreadyQueued(first)
    );
    queued(
        cluster
            .submit_for(
                groups[1],
                GroupIncarnation::first(),
                request(0, 1, 1, CounterCommand::Read),
                CounterCommand::Read,
            )
            .expect("second group fills the global queue"),
    );
    assert_eq!(
        cluster
            .submit_for(groups[0], GroupIncarnation::first(), first_request, add)
            .expect("outstanding retry bypasses the full global queue"),
        CounterSubmitOutcome::AlreadyQueued(first)
    );
    let overflow = cluster
        .submit_system(groups[0], GroupIncarnation::first(), SystemClass::Bulk)
        .expect_err("new work reaches the global bound");
    assert!(matches!(
        overflow.reason,
        CounterAdmissionRejection::Managed(
            rafter_multiraft::managed::AdmissionRejection::GlobalQueueFull { bound: 2 }
        )
    ));

    cluster
        .drive_until_idle(256)
        .expect("accepted requests complete");
    assert_eq!(
        cluster
            .submit_for(groups[0], GroupIncarnation::first(), first_request, add)
            .expect("completed retry bypasses queue bounds"),
        CounterSubmitOutcome::Replayed(CounterResult::Added { value: 4 })
    );
    let conflicting = CounterCommand::Add {
        delta: Delta::new(8).expect("delta is nonzero"),
    };
    let conflict = cluster
        .submit_for(
            groups[0],
            GroupIncarnation::first(),
            request(0, 1, 1, conflicting),
            conflicting,
        )
        .expect_err("same identity with another command conflicts");
    assert!(matches!(
        conflict.reason,
        CounterAdmissionRejection::Policy(AdmissionRejection::ConflictingRetry)
    ));
    let gap = cluster
        .submit_for(
            groups[0],
            GroupIncarnation::first(),
            request(0, 1, 3, CounterCommand::Read),
            CounterCommand::Read,
        )
        .expect_err("sequence gap fails closed");
    assert!(matches!(
        gap.reason,
        CounterAdmissionRejection::Policy(AdmissionRejection::SequenceGap { expected })
            if expected == sequence(2)
    ));
    let metrics = cluster.metrics();
    assert_eq!(
        metrics.admitted,
        metrics.serviced
            + metrics.failed
            + u64::try_from(metrics.queued + metrics.in_flight_work).expect("test counts fit")
    );
}

fn three_serving_groups(cluster: &mut ManagedCounterCluster, work_quota: u32) -> [GroupId; 3] {
    let groups = [GroupId::new(0), GroupId::new(1), GroupId::new(2)];
    for group_id in groups {
        create_serving(cluster, group_id, work_quota);
    }
    finish_recovery(cluster, &groups);
    groups
}

#[test]
fn class_priority_and_slow_worker_occupancy_are_exact() {
    let mut cluster = cluster(2, 8, 32, 4, 512, 2);
    let groups = three_serving_groups(&mut cluster, 6);
    open_session(&mut cluster, groups[0], 0);

    let first_bulk = cluster
        .submit_system(groups[0], GroupIncarnation::first(), SystemClass::Bulk)
        .expect("first bulk queues");
    let second_bulk = cluster
        .submit_system(groups[0], GroupIncarnation::first(), SystemClass::Bulk)
        .expect("second bulk queues");
    let snapshot = cluster
        .submit_system(groups[0], GroupIncarnation::first(), SystemClass::Snapshot)
        .expect("snapshot queues");
    let command = queued(
        cluster
            .submit_for(
                groups[0],
                GroupIncarnation::first(),
                request(0, 1, 1, CounterCommand::Read),
                CounterCommand::Read,
            )
            .expect("command queues"),
    );
    let first_control = cluster
        .submit_system(groups[0], GroupIncarnation::first(), SystemClass::Control)
        .expect("first control queues");
    let second_control = cluster
        .submit_system(groups[0], GroupIncarnation::first(), SystemClass::Control)
        .expect("second control queues");
    cluster
        .submit_system(groups[1], GroupIncarnation::first(), SystemClass::Bulk)
        .expect("slow group work queues");
    cluster
        .submit_system(groups[2], GroupIncarnation::first(), SystemClass::Bulk)
        .expect("healthy group work queues");
    cluster.set_service_delay(groups[1], 4);

    let report = cluster
        .drive_until_idle(256)
        .expect("one delayed worker cannot stop the other");
    let group_zero = report
        .turns
        .iter()
        .find(|turn| turn.group_id == groups[0])
        .expect("group zero receives a turn");
    assert_eq!(
        group_zero
            .items
            .iter()
            .map(|item| item.class)
            .collect::<Vec<_>>(),
        vec![
            ManagedClass::Control,
            ManagedClass::Control,
            ManagedClass::Command,
            ManagedClass::Snapshot,
            ManagedClass::Bulk,
            ManagedClass::Bulk,
        ]
    );
    assert_eq!(
        group_zero
            .items
            .iter()
            .map(|item| item.work_id)
            .collect::<Vec<_>>(),
        vec![
            first_control.work_id,
            second_control.work_id,
            command.admission.work_id,
            snapshot.work_id,
            first_bulk.work_id,
            second_bulk.work_id,
        ]
    );
    let healthy_position = report
        .turns
        .iter()
        .position(|turn| turn.group_id == groups[2])
        .expect("healthy peer receives a turn");
    let slow_position = report
        .turns
        .iter()
        .position(|turn| turn.group_id == groups[1])
        .expect("slow peer eventually completes");
    assert!(healthy_position < slow_position);
}

#[test]
fn quota_changes_throughput_share_without_changing_opportunity_share() {
    let mut cluster = cluster(2, 16, 32, 4, 512, 1);
    let narrow = GroupId::new(0);
    let wide = GroupId::new(1);
    create_serving(&mut cluster, narrow, 1);
    create_serving(&mut cluster, wide, 4);
    finish_recovery(&mut cluster, &[narrow, wide]);
    for _ in 0..4 {
        cluster
            .submit_system(narrow, GroupIncarnation::first(), SystemClass::Bulk)
            .expect("narrow work queues");
        cluster
            .submit_system(wide, GroupIncarnation::first(), SystemClass::Control)
            .expect("wide work queues");
    }

    let report = cluster
        .drive_until_idle(256)
        .expect("quota profile quiesces");
    assert_eq!(report.plans.first(), Some(&vec![narrow, wide]));
    let first_pass = report.turns[0].pass_id;
    let first_turns = report
        .turns
        .iter()
        .filter(|turn| turn.pass_id == first_pass)
        .collect::<Vec<_>>();
    assert_eq!(
        first_turns
            .iter()
            .map(|turn| turn.group_id)
            .collect::<Vec<_>>(),
        vec![narrow, wide],
        "higher-class work cannot reorder the ready-set pass"
    );
    assert_eq!(first_turns[0].items.len(), 1);
    assert_eq!(first_turns[1].items.len(), 4);
}

#[test]
fn poison_failure_is_isolated_and_conserved() {
    let mut cluster = cluster(2, 8, 32, 4, 512, 2);
    let groups = three_serving_groups(&mut cluster, 1);
    cluster
        .submit_fault(groups[1], SystemClass::Control)
        .expect("fault is ordinary admitted work");
    let behind_faults = (0..7)
        .map(|_| {
            cluster
                .submit_fault(groups[1], SystemClass::Bulk)
                .expect("proposals queue behind the poisoning dispatch")
        })
        .collect::<Vec<_>>();
    cluster
        .drive_round()
        .expect("the injected fault reaches the real group");
    for _ in 0..16 {
        if cluster.is_poisoned(groups[1]) {
            break;
        }
        cluster
            .drive_round()
            .expect("replication advances toward the injected fault");
    }
    assert!(cluster.is_poisoned(groups[1]));
    let queued_at_poison = cluster.metrics().queued;
    assert_ne!(queued_at_poison, 0, "the regression needs poisoned backlog");

    let healthy = cluster
        .submit_system(groups[2], GroupIncarnation::first(), SystemClass::Control)
        .expect("healthy work queues beside poison");
    let ordinary = cluster
        .drive_round()
        .expect("ordinary driving keeps healthy groups moving");
    assert!(cluster.is_poisoned(groups[1]));
    assert!(
        ordinary
            .turns
            .iter()
            .flat_map(|turn| &turn.items)
            .all(|item| item.work_id != behind_faults[0].admission.work_id),
        "ordinary driving must not auto-retire a poisoned queue: {ordinary:#?}"
    );
    assert!(ordinary
        .turns
        .iter()
        .flat_map(|turn| &turn.items)
        .any(|item| item.work_id == healthy.work_id
            && matches!(
                item.disposition,
                rafter_reference_sharded_counter::adapter::DrivenDisposition::Serviced
            )));
    assert_eq!(
        cluster.metrics().queued,
        queued_at_poison,
        "ordinary driving neither dispatches nor fails poisoned backlog"
    );

    let drained = cluster
        .lifecycle(groups[1], LifecycleRequest::Drain)
        .expect("explicit drain owns poisoned retirement");
    assert_eq!(drained.failed.len(), queued_at_poison);
    let explicitly_failed = behind_faults
        .iter()
        .filter(|receipt| {
            drained
                .failed
                .iter()
                .any(|failure| failure.work.get() == receipt.admission.work_id.get())
        })
        .collect::<Vec<_>>();
    assert!(!explicitly_failed.is_empty());
    assert!(explicitly_failed
        .iter()
        .all(|receipt| cluster.proposal_failure(receipt.proposal_id).is_some()));
    assert!(
        cluster
            .lifecycle(groups[1], LifecycleRequest::Drain)
            .expect("repeated drain is lossless")
            .failed
            .is_empty(),
        "every queued failure is returned exactly once"
    );
    let metrics = cluster.metrics();
    assert_eq!(metrics.admitted, metrics.serviced + metrics.failed);
}

#[test]
fn one_thousand_twenty_four_real_groups_have_zero_opportunity_gap() {
    const GROUPS: u32 = 1_024;
    let mut cluster = cluster(64, 2, 8_192, 1, 32_768, 1);
    let groups = (0..GROUPS).map(GroupId::new).collect::<Vec<_>>();
    for group_id in &groups {
        create_serving(&mut cluster, *group_id, 1);
    }
    finish_recovery(&mut cluster, &groups);

    let accepted = groups
        .iter()
        .map(|group_id| {
            let receipt = cluster
                .submit_system(*group_id, GroupIncarnation::first(), SystemClass::Bulk)
                .expect("one item per group fits");
            ExpectedWork {
                work_id: receipt.work_id,
                group_id: *group_id,
                class: ManagedClass::Bulk,
            }
        })
        .collect::<Vec<_>>();
    let report = cluster
        .drive_until_idle(1_024)
        .expect("large deterministic profile quiesces");
    let expectation = AcceptanceExpectation {
        ready: groups.clone(),
        accepted,
        quotas: groups.iter().map(|group_id| (*group_id, 1)).collect(),
    };
    let audit = audit_acceptance(&expectation, &report, &cluster.metrics())
        .expect("independent audit accepts the real schedule");
    assert_eq!(
        audit.ready_width,
        usize::try_from(GROUPS).expect("group count fits")
    );
    assert_eq!(audit.opportunities, audit.ready_width);
    assert_eq!(audit.widest_gap, 0);
    assert_eq!(audit.queued + audit.in_flight, 0);
    assert_eq!(audit.admitted, audit.serviced + audit.failed);
    println!(
        "groups={GROUPS} passes={} opportunities={} widest_gap={} admitted={} serviced={} failed={}",
        audit.passes,
        audit.opportunities,
        audit.widest_gap,
        audit.admitted,
        audit.serviced,
        audit.failed
    );
}
