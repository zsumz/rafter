use std::{collections::BTreeMap, num::NonZeroUsize};

use rafter_multiraft::managed::{ManagedConfig, WorkClass};
use rafter_reference_sharded_counter::{
    adapter::{
        audit_acceptance, AcceptanceExpectation, AcceptanceViolation, ExpectedWork,
        ManagedCounterCluster, NetworkConfig,
    },
    GroupId, GroupIncarnation, GroupLifecycle, LifecycleOutcome, LifecycleRequest, SystemClass,
    WorkQuota,
};

fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test bound is nonzero")
}

fn fixture() -> (
    AcceptanceExpectation,
    rafter_reference_sharded_counter::adapter::DriveReport,
    rafter_multiraft::managed::ManagedMetrics,
) {
    let mut cluster = ManagedCounterCluster::new(
        ManagedConfig::new(nonzero(2), nonzero(4), nonzero(16), nonzero(2))
            .expect("managed bounds are valid"),
        NetworkConfig {
            max_pending_messages: nonzero(256),
            max_sessions_per_group: nonzero(1),
        },
    );
    let groups = [GroupId::new(0), GroupId::new(1), GroupId::new(2)];
    for group_id in groups {
        assert!(matches!(
            cluster
                .lifecycle(
                    group_id,
                    LifecycleRequest::Create {
                        quota: WorkQuota::new(2).expect("quota is nonzero"),
                    },
                )
                .expect("group opens")
                .outcome,
            LifecycleOutcome::Created { .. }
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
    }
    cluster.drive_until_idle(256).expect("elections quiesce");
    for group_id in groups {
        cluster
            .lifecycle(group_id, LifecycleRequest::Serve)
            .expect("group serves");
    }

    let mut accepted = Vec::new();
    for (group_id, class) in [
        (groups[0], SystemClass::Bulk),
        (groups[0], SystemClass::Control),
        (groups[1], SystemClass::Bulk),
        (groups[2], SystemClass::Bulk),
    ] {
        let receipt = cluster
            .submit_system(group_id, GroupIncarnation::first(), class)
            .expect("fixture work queues");
        accepted.push(ExpectedWork {
            work_id: receipt.work_id,
            group_id,
            class: match class {
                SystemClass::Control => WorkClass::Control,
                SystemClass::Snapshot => WorkClass::Snapshot,
                SystemClass::Bulk => WorkClass::Bulk,
            },
        });
    }
    let report = cluster.drive_until_idle(256).expect("fixture run quiesces");
    let expectation = AcceptanceExpectation {
        ready: groups.to_vec(),
        accepted,
        quotas: BTreeMap::from([(groups[0], 2), (groups[1], 2), (groups[2], 2)]),
    };
    let metrics = cluster.metrics();
    audit_acceptance(&expectation, &report, &metrics).expect("fixture is a valid schedule");
    (expectation, report, metrics)
}

#[test]
fn the_real_audit_rejects_every_load_bearing_scheduler_mutation() {
    let (expectation, report, metrics) = fixture();

    let mut reordered = report.clone();
    reordered.plans[0].swap(0, 1);
    assert!(matches!(
        audit_acceptance(&expectation, &reordered, &metrics),
        Err(AcceptanceViolation::PlanMismatch { .. })
    ));

    let mut omitted_plan = report.clone();
    omitted_plan.plans[0].pop();
    assert!(matches!(
        audit_acceptance(&expectation, &omitted_plan, &metrics),
        Err(AcceptanceViolation::PlanMismatch { .. })
    ));

    let mut second_turn = report.clone();
    let duplicate = second_turn.turns[0].clone();
    second_turn.turns.insert(1, duplicate);
    assert!(matches!(
        audit_acceptance(&expectation, &second_turn, &metrics),
        Err(AcceptanceViolation::DuplicateOpportunity { .. })
    ));

    let mut over_quota_expectation = expectation.clone();
    over_quota_expectation
        .quotas
        .insert(expectation.ready[0], 1);
    assert!(matches!(
        audit_acceptance(&over_quota_expectation, &report, &metrics),
        Err(AcceptanceViolation::QuotaExceeded { .. })
    ));

    let mut unknown_expectation = expectation.clone();
    unknown_expectation.accepted.remove(0);
    assert!(matches!(
        audit_acceptance(&unknown_expectation, &report, &metrics),
        Err(AcceptanceViolation::UnknownWork { .. })
    ));

    let mut missing = report.clone();
    let missing_work = missing.turns[0].items.pop().expect("turn has work").work_id;
    assert!(matches!(
        audit_acceptance(&expectation, &missing, &metrics),
        Err(AcceptanceViolation::MissingDisposition { work_id }) if work_id == missing_work
    ));

    let mut class_reordered = report.clone();
    class_reordered.turns[0].items.swap(0, 1);
    assert!(matches!(
        audit_acceptance(&expectation, &class_reordered, &metrics),
        Err(AcceptanceViolation::ClassOutOfOrder { .. })
    ));

    let mut route_changed = expectation.clone();
    route_changed.accepted[0].class = WorkClass::Snapshot;
    assert!(matches!(
        audit_acceptance(&route_changed, &report, &metrics),
        Err(AcceptanceViolation::WorkRouteChanged { .. })
    ));

    let mut occupied = metrics.clone();
    occupied.occupied_workers = 1;
    assert!(matches!(
        audit_acceptance(&expectation, &report, &occupied),
        Err(AcceptanceViolation::WorkerStillOccupied { workers: 1 })
    ));

    let mut unbalanced = metrics.clone();
    unbalanced.admitted += 1;
    assert!(matches!(
        audit_acceptance(&expectation, &report, &unbalanced),
        Err(AcceptanceViolation::Conservation { .. })
    ));

    let omitted_group = expectation.ready[2];
    let mut opportunity_gap = report.clone();
    opportunity_gap
        .turns
        .retain(|turn| turn.group_id != omitted_group);
    let mut gap_expectation = expectation.clone();
    gap_expectation
        .accepted
        .retain(|work| work.group_id != omitted_group);
    assert!(matches!(
        audit_acceptance(&gap_expectation, &opportunity_gap, &metrics),
        Err(AcceptanceViolation::OpportunityGap { ref missing })
            if missing == &[omitted_group]
    ));

    let mut span_scan = report.clone();
    span_scan.plans[0].push(GroupId::new(99_999));
    assert!(matches!(
        audit_acceptance(&expectation, &span_scan, &metrics),
        Err(AcceptanceViolation::PlanMismatch { .. })
    ));
}
