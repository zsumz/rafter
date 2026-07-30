use std::num::NonZeroUsize;

use rafter::{LocalProposalId, LogIndex, MembershipConfig, MembershipSet, NodeId, Role, Term};
use rafter_app::{
    group::{GroupFatalState, GroupInput, GroupStepReport},
    metrics::RaftGroupMetrics,
    state_machine::ApplyResult,
};
use rafter_multiraft::{
    managed::{
        AdmissionRejection, ArmPass, BeginDispatch, CompletionError, ManagedConfig,
        ManagedScheduler, ManagedTypedMultiRaftHost, WorkClass, WorkDisposition,
    },
    DriverError, DriverErrorKind, ErrorCause, TypedGroupDriver,
};

fn config(workers: usize, group_queue: usize, global_queue: usize, quota: usize) -> ManagedConfig {
    ManagedConfig::new(
        NonZeroUsize::new(workers).expect("workers are nonzero"),
        NonZeroUsize::new(group_queue).expect("group queue is nonzero"),
        NonZeroUsize::new(global_queue).expect("global queue is nonzero"),
        NonZeroUsize::new(quota).expect("quota is nonzero"),
    )
    .expect("test bounds are valid")
}

fn ready_with_work(
    scheduler: &mut ManagedScheduler<u64, &'static str>,
    group_id: u64,
    payload: &'static str,
) {
    scheduler
        .register_group(group_id, None)
        .expect("group registers");
    scheduler
        .admit(&group_id, WorkClass::Command, payload)
        .expect("work is admitted");
    scheduler
        .set_available(&group_id, true)
        .expect("group becomes available");
}

#[test]
fn a_pass_uses_deterministic_group_order_and_one_opportunity_each() {
    let mut scheduler = ManagedScheduler::new(config(3, 8, 24, 1));
    for group_id in [30, 10, 20] {
        ready_with_work(&mut scheduler, group_id, "work");
    }

    let ArmPass::Armed(plan) = scheduler.arm_pass().expect("pass identity remains") else {
        panic!("ready groups arm a pass");
    };
    assert_eq!(plan.groups, vec![10, 20, 30]);

    let mut visited = Vec::new();
    for _ in 0..3 {
        let BeginDispatch::Dispatched(dispatch) = scheduler
            .begin_dispatch()
            .expect("dispatch identity remains")
        else {
            panic!("each ready group receives a dispatch");
        };
        visited.push(dispatch.group_id);
        let permit = dispatch.completion_permit();
        let disposition = [WorkDisposition::Serviced(dispatch.items[0].work_id)];
        scheduler
            .complete_dispatch(&permit, &disposition)
            .expect("exact completion releases occupancy");
    }
    assert_eq!(visited, plan.groups);
    let BeginDispatch::PassComplete(completion) = scheduler
        .begin_dispatch()
        .expect("completion needs no identity")
    else {
        panic!("the pass completes after all opportunities");
    };
    assert_eq!(completion.planned, 3);
    assert_eq!(completion.dispatched, 3);
    assert_eq!(completion.skipped, 0);
}

#[test]
fn quota_changes_turn_size_and_class_priority_stays_inside_the_turn() {
    let mut scheduler = ManagedScheduler::new(config(1, 8, 8, 3));
    scheduler.register_group(7, None).expect("group registers");
    for (class, payload) in [
        (WorkClass::Bulk, "bulk"),
        (WorkClass::Command, "command"),
        (WorkClass::Control, "control"),
        (WorkClass::Snapshot, "snapshot"),
    ] {
        scheduler
            .admit(&7, class, payload)
            .expect("work is admitted");
    }
    scheduler
        .set_available(&7, true)
        .expect("group becomes available");
    scheduler.arm_pass().expect("pass arms");

    let BeginDispatch::Dispatched(dispatch) = scheduler.begin_dispatch().expect("dispatch opens")
    else {
        panic!("ready work dispatches");
    };
    assert_eq!(
        dispatch
            .items
            .iter()
            .map(|item| (item.class, item.payload))
            .collect::<Vec<_>>(),
        vec![
            (WorkClass::Control, "control"),
            (WorkClass::Command, "command"),
            (WorkClass::Snapshot, "snapshot"),
        ]
    );
    assert_eq!(scheduler.metrics().queued, 1);
}

#[test]
fn group_and_global_queue_bounds_return_unaccepted_payloads() {
    let mut scheduler = ManagedScheduler::new(config(1, 2, 3, 1));
    scheduler.register_group(1, None).expect("group registers");
    scheduler.register_group(2, None).expect("group registers");
    scheduler
        .admit(&1, WorkClass::Command, "one")
        .expect("first group item fits");
    scheduler
        .admit(&1, WorkClass::Command, "two")
        .expect("second group item fits");

    let rejected = scheduler
        .admit(&1, WorkClass::Command, "group overflow")
        .expect_err("group bound is closed");
    assert_eq!(rejected.payload, "group overflow");
    assert!(matches!(
        rejected.reason,
        AdmissionRejection::GroupQueueFull {
            group_id: 1,
            bound: 2
        }
    ));

    scheduler
        .admit(&2, WorkClass::Command, "three")
        .expect("global final slot fits");
    let rejected = scheduler
        .admit(&2, WorkClass::Command, "global overflow")
        .expect_err("global bound is closed");
    assert_eq!(rejected.payload, "global overflow");
    assert!(matches!(
        rejected.reason,
        AdmissionRejection::GlobalQueueFull { bound: 3 }
    ));
    assert_eq!(scheduler.metrics().admitted, 3);
    assert_eq!(scheduler.metrics().queued, 3);
}

#[test]
fn a_failed_group_does_not_end_the_pass_or_drop_later_work() {
    let mut scheduler = ManagedScheduler::new(config(2, 4, 8, 1));
    ready_with_work(&mut scheduler, 1, "fails");
    ready_with_work(&mut scheduler, 2, "succeeds");
    scheduler.arm_pass().expect("pass arms");

    let BeginDispatch::Dispatched(failed) =
        scheduler.begin_dispatch().expect("first dispatch opens")
    else {
        panic!("first group dispatches");
    };
    let failed_permit = failed.completion_permit();
    scheduler
        .complete_dispatch(
            &failed_permit,
            &[WorkDisposition::Failed(failed.items[0].work_id)],
        )
        .expect("failure is explicit");

    let BeginDispatch::Dispatched(healthy) =
        scheduler.begin_dispatch().expect("second dispatch opens")
    else {
        panic!("later group still dispatches");
    };
    assert_eq!(healthy.group_id, 2);
    let healthy_permit = healthy.completion_permit();
    scheduler
        .complete_dispatch(
            &healthy_permit,
            &[WorkDisposition::Serviced(healthy.items[0].work_id)],
        )
        .expect("healthy completion is retained");
    let metrics = scheduler.metrics();
    assert_eq!(metrics.admitted, 2);
    assert_eq!(metrics.failed, 1);
    assert_eq!(metrics.serviced, 1);
    assert_eq!(metrics.queued + metrics.in_flight_work, 0);
}

#[test]
fn occupancy_releases_only_on_an_exact_once_completion() {
    let mut scheduler = ManagedScheduler::new(config(1, 4, 8, 1));
    ready_with_work(&mut scheduler, 1, "first");
    scheduler.register_group(2, None).expect("group registers");
    let unrelated = scheduler
        .admit(&2, WorkClass::Command, "unrelated")
        .expect("unrelated work is admitted");
    scheduler
        .set_available(&2, true)
        .expect("second group becomes available");
    scheduler.arm_pass().expect("pass arms");

    let BeginDispatch::Dispatched(dispatch) = scheduler.begin_dispatch().expect("dispatch opens")
    else {
        panic!("first group dispatches");
    };
    let permit = dispatch.completion_permit();
    assert_eq!(scheduler.metrics().occupied_workers, 1);
    assert!(matches!(
        scheduler.begin_dispatch().expect("no identity is needed"),
        BeginDispatch::WorkersOccupied
    ));
    assert!(matches!(
        scheduler.complete_dispatch(&permit, &[]),
        Err(CompletionError::WrongItemCount { .. })
    ));
    assert!(matches!(
        scheduler.complete_dispatch(&permit, &[WorkDisposition::Serviced(unrelated.work_id)]),
        Err(CompletionError::WrongWork { .. })
    ));
    assert_eq!(scheduler.metrics().occupied_workers, 1);

    scheduler
        .complete_dispatch(
            &permit,
            &[WorkDisposition::Serviced(dispatch.items[0].work_id)],
        )
        .expect("exact completion releases");
    assert_eq!(scheduler.metrics().occupied_workers, 0);
    assert!(matches!(
        scheduler.complete_dispatch(
            &permit,
            &[WorkDisposition::Serviced(dispatch.items[0].work_id)]
        ),
        Err(CompletionError::UnknownDispatch(_))
    ));
}

#[test]
fn another_schedulers_matching_numeric_ids_cannot_release_occupancy() {
    let mut first = ManagedScheduler::new(config(1, 2, 2, 1));
    let mut second = ManagedScheduler::new(config(1, 2, 2, 1));
    ready_with_work(&mut first, 1, "first");
    ready_with_work(&mut second, 1, "second");
    first.arm_pass().expect("first pass arms");
    second.arm_pass().expect("second pass arms");
    let BeginDispatch::Dispatched(first_dispatch) =
        first.begin_dispatch().expect("first dispatch opens")
    else {
        panic!("first scheduler dispatches");
    };
    let BeginDispatch::Dispatched(second_dispatch) =
        second.begin_dispatch().expect("second dispatch opens")
    else {
        panic!("second scheduler dispatches");
    };
    assert_eq!(
        first_dispatch.dispatch_id, second_dispatch.dispatch_id,
        "the regression requires matching public numeric identities"
    );
    let first_permit = first_dispatch.completion_permit();
    let second_permit = second_dispatch.completion_permit();
    let first_disposition = [WorkDisposition::Serviced(first_dispatch.items[0].work_id)];

    assert!(matches!(
        first.complete_dispatch(&second_permit, &first_disposition),
        Err(CompletionError::ForeignDispatch(_))
    ));
    assert_eq!(first.metrics().occupied_workers, 1);
    first
        .complete_dispatch(&first_permit, &first_disposition)
        .expect("the scheduler's own permit releases occupancy");
    assert_eq!(first.metrics().occupied_workers, 0);
}

#[test]
fn planning_uses_the_ready_set_not_the_group_id_span() {
    let mut scheduler = ManagedScheduler::new(config(1, 4, 8, 1));
    ready_with_work(&mut scheduler, u64::MAX, "sparse");

    let ArmPass::Armed(plan) = scheduler.arm_pass().expect("pass identity remains") else {
        panic!("the sparse ready group arms a pass");
    };
    assert_eq!(plan.groups, vec![u64::MAX]);
    assert_eq!(scheduler.metrics().groups, 1);
    assert_eq!(scheduler.metrics().ready_groups, 1);
}

#[test]
fn explicit_queue_failure_preserves_in_flight_ownership_and_conservation() {
    let mut scheduler = ManagedScheduler::new(config(1, 4, 4, 1));
    ready_with_work(&mut scheduler, 1, "in flight");
    let queued = scheduler
        .admit(&1, WorkClass::Bulk, "queued")
        .expect("second item is admitted");
    scheduler.arm_pass().expect("pass arms");
    let BeginDispatch::Dispatched(dispatch) = scheduler.begin_dispatch().expect("dispatch opens")
    else {
        panic!("one item enters flight");
    };

    let retired = scheduler
        .fail_queued(&1)
        .expect("explicit failure drains only queued work");
    assert_eq!(retired.len(), 1);
    assert_eq!(retired[0].work_id, queued.work_id);
    assert_eq!(retired[0].payload, "queued");
    let metrics = scheduler.metrics();
    assert_eq!(metrics.admitted, 2);
    assert_eq!(metrics.failed, 1);
    assert_eq!(metrics.in_flight_work, 1);
    assert_eq!(metrics.queued, 0);

    let permit = dispatch.completion_permit();
    scheduler
        .complete_dispatch(
            &permit,
            &[WorkDisposition::Serviced(dispatch.items[0].work_id)],
        )
        .expect("in-flight work keeps its exact completion path");
    let metrics = scheduler.metrics();
    assert_eq!(metrics.admitted, metrics.serviced + metrics.failed);
    assert_eq!(metrics.in_flight_work + metrics.queued, 0);
}

#[derive(Debug)]
struct TypedTestDriver {
    group_id: u64,
    applied: u64,
    poisoned: bool,
}

impl TypedTestDriver {
    fn healthy(group_id: u64) -> Self {
        Self {
            group_id,
            applied: 0,
            poisoned: false,
        }
    }

    fn poisoned(group_id: u64) -> Self {
        Self {
            group_id,
            applied: 0,
            poisoned: true,
        }
    }
}

impl TypedGroupDriver<u64> for TypedTestDriver {
    type Command = Vec<u8>;
    type CommandResult = Vec<u8>;

    fn step(
        &mut self,
        _input: GroupInput<u64, Self::Command>,
    ) -> Result<GroupStepReport<u64, Self::CommandResult>, DriverError> {
        if self.poisoned {
            return Err(DriverError::new(
                DriverErrorKind::Poisoned,
                ErrorCause::new(std::io::Error::other("poisoned test group")),
            ));
        }
        self.applied += 1;
        Ok(GroupStepReport {
            group_id: self.group_id,
            peer_messages: Vec::new(),
            applied: vec![ApplyResult {
                index: LogIndex(self.applied),
                term: Term(1),
                result: b"applied".to_vec(),
                local_proposal_id: Some(LocalProposalId(self.applied)),
            }],
            proposal_events: Vec::new(),
            read_events: Vec::new(),
            leadership_transfer_events: Vec::new(),
            snapshot_events: Vec::new(),
            membership_events: Vec::new(),
            metrics: None,
        })
    }

    fn metrics(&self) -> RaftGroupMetrics<u64> {
        RaftGroupMetrics {
            group_id: self.group_id,
            node_id: NodeId(1),
            role: Role::Follower,
            term: Term(1),
            leader_hint: None,
            commit_index: LogIndex(self.applied),
            applied_index: LogIndex(self.applied),
            last_log_index: LogIndex(self.applied),
            snapshot_index: LogIndex::ZERO,
            membership: MembershipConfig::Stable(
                MembershipSet::new(vec![NodeId(1)], Vec::new()).expect("valid membership"),
            ),
            replication: Vec::new(),
            pending_proposals: 0,
            pending_read_barriers: 0,
            pending_query_reads: 0,
            completed_query_reads: 0,
            reserved_reads: 0,
            fatal_state: if self.poisoned {
                GroupFatalState::Poisoned {
                    reason: "test poison".to_string(),
                }
            } else {
                GroupFatalState::Healthy
            },
        }
    }
}

#[test]
fn typed_composition_retains_reports_and_isolates_a_poisoned_group() {
    let mut host = ManagedTypedMultiRaftHost::<u64, Vec<u8>, Vec<u8>>::new(config(1, 4, 12, 1));
    host.open_group(&1, TypedTestDriver::healthy(1), None)
        .expect("first group opens");
    host.open_group(&2, TypedTestDriver::poisoned(2), None)
        .expect("failing group opens");
    host.open_group(&3, TypedTestDriver::healthy(3), None)
        .expect("later group opens");
    for group_id in [1, 2, 3] {
        host.admit(&group_id, WorkClass::Command, GroupInput::Tick)
            .expect("tick is admitted");
        host.set_available(&group_id, true)
            .expect("group becomes available");
    }

    let ArmPass::Armed(plan) = host.arm_pass().expect("pass identity remains") else {
        panic!("three ready groups arm a pass");
    };
    assert_eq!(plan.groups, vec![1, 2, 3]);

    let mut reports = Vec::new();
    for expected_group in [1, 2, 3] {
        let BeginDispatch::Dispatched(dispatch) =
            host.begin_dispatch().expect("dispatch identity remains")
        else {
            panic!("each group receives its opportunity");
        };
        assert_eq!(dispatch.group_id, expected_group);
        reports.push(
            host.execute_dispatch(dispatch)
                .expect("owned dispatch executes losslessly"),
        );
    }
    assert!(reports[0].items[0].result.is_ok());
    assert!(reports[1].items[0].result.is_err());
    assert!(reports[2].items[0].result.is_ok());
    assert_eq!(
        reports[0].items[0].result.as_ref().unwrap().applied.len(),
        1
    );
    assert_eq!(
        reports[2].items[0].result.as_ref().unwrap().applied.len(),
        1
    );
    assert!(reports.iter().all(|report| report.completion.is_ok()));

    let metrics = host.managed_metrics();
    assert_eq!(metrics.admitted, 3);
    assert_eq!(metrics.serviced, 2);
    assert_eq!(metrics.failed, 1);
    assert_eq!(metrics.queued + metrics.in_flight_work, 0);

    let queued_after_poison = host
        .admit(&2, WorkClass::Command, GroupInput::Tick)
        .expect("poisoned group admission remains explicit");
    assert!(matches!(
        host.arm_pass().expect("no new identity is needed"),
        ArmPass::AlreadyArmed(_)
    ));
    assert!(matches!(
        host.begin_dispatch().expect("pass completes"),
        BeginDispatch::PassComplete(_)
    ));
    assert!(matches!(
        host.arm_pass().expect("poisoned group stays unavailable"),
        ArmPass::Idle
    ));
    assert!(
        host.remove_group(&2).is_err(),
        "accepted poisoned backlog fences removal"
    );
    let retired = host
        .fail_queued(&2)
        .expect("explicit drain owns poisoned backlog retirement");
    assert_eq!(retired.len(), 1);
    assert_eq!(retired[0].work_id, queued_after_poison.work_id);
    assert_eq!(retired[0].class, WorkClass::Command);
    assert!(matches!(retired[0].payload, GroupInput::Tick));
    assert!(
        host.fail_queued(&2)
            .expect("repeated drain is lossless and idempotent")
            .is_empty(),
        "each failure is returned exactly once"
    );
    let metrics = host.managed_metrics();
    assert_eq!(metrics.admitted, 4);
    assert_eq!(metrics.serviced, 2);
    assert_eq!(metrics.failed, 2);
    assert_eq!(metrics.queued + metrics.in_flight_work, 0);
    assert!(host
        .remove_group(&2)
        .expect("drained group is removable")
        .is_some());
}
