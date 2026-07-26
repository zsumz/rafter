//! Regression suite from the cold audit of `rafter-multiraft`.
//!
//! Every test here failed against the tree that shipped before it. They are
//! kept as one file rather than folded into the unit tests because they are a
//! record of what this crate got wrong, and each one names the finding it
//! pins.

mod support;

use rafter::{LocalProposalId, LogIndex, Term};
use rafter_app::{
    group::{GroupInput, GroupStepReport},
    metrics::RaftGroupMetrics,
    state_machine::ApplyResult,
};
use rafter_multiraft::{
    DriverError, DriverErrorKind, ErrorCause, MultiRaftError, MultiRaftErrorKind, MultiRaftHost,
    TypedGroupDriver, TypedMultiRaftHost,
};

use support::{metrics, ApplyingDriver, FailingDriver, ShardFailure, StepCounter};

// ---------------------------------------------------------------------------
// M1 -- `tick_all` destroyed the committed apply results of every earlier group
// ---------------------------------------------------------------------------

#[test]
fn a_tick_pass_carries_the_apply_results_of_groups_before_a_failing_one() {
    let mut host = MultiRaftHost::new();
    host.open_group(1, ApplyingDriver::new(1))
        .expect("open group 1");
    host.open_group(2, FailingDriver::new(2, "group 2 is poisoned"))
        .expect("open group 2");

    let pass = host.tick_all();

    assert_eq!(
        host.metrics().expect("metrics").groups[0].applied_index,
        LogIndex(1),
        "group 1 committed and applied one entry"
    );
    assert!(
        !pass.is_complete(),
        "a pass with one healthy group and one failing group is not complete"
    );
    let applied = pass
        .reports()
        .flat_map(|report| report.applied.iter())
        .count();
    assert_eq!(
        applied, 1,
        "group 1's apply result must reach the caller: `applied` is the only proof a write took \
         effect and nothing re-emits it"
    );
    assert_eq!(
        pass.failures()
            .map(|(group_id, _)| *group_id)
            .collect::<Vec<_>>(),
        vec![2],
        "group 2's failure is reported per group, not as a pass-level error"
    );
}

#[test]
fn a_typed_tick_pass_carries_the_apply_results_of_groups_before_a_failing_one() {
    let mut host = TypedMultiRaftHost::<u64, TypedCommand, TypedResult>::new();
    host.open_group(1, TypedApplyingDriver::new(1))
        .expect("open group 1");
    host.open_group(2, TypedFailingDriver::new(2))
        .expect("open group 2");

    let pass = host.tick_all();

    assert_eq!(
        host.metrics().expect("metrics").groups[0].applied_index,
        LogIndex(1),
        "group 1 committed and applied one entry"
    );
    let applied = pass
        .reports()
        .flat_map(|report| report.applied.iter())
        .count();
    assert_eq!(applied, 1, "group 1's apply result must reach the caller");
    assert_eq!(
        pass.failures()
            .map(|(group_id, _)| *group_id)
            .collect::<Vec<_>>(),
        vec![2]
    );
}

// ---------------------------------------------------------------------------
// M2 -- one failing group starved every higher-keyed group, with no way out
// ---------------------------------------------------------------------------

#[test]
fn a_failing_group_does_not_starve_a_higher_keyed_one() {
    let broken_steps = StepCounter::default();
    let healthy_steps = StepCounter::default();
    let mut host = MultiRaftHost::new();
    host.open_group(
        1,
        FailingDriver::with_counter(1, "poisoned", StepCounter::clone(&broken_steps)),
    )
    .expect("open group 1");
    host.open_group(
        2,
        ApplyingDriver::with_counter(2, StepCounter::clone(&healthy_steps)),
    )
    .expect("open group 2");

    for _ in 0..100 {
        let pass = host.tick_all();
        assert_eq!(
            pass.visited(),
            2,
            "every pass visits every group, whatever any group did"
        );
    }

    assert_eq!(broken_steps.get(), 100, "the broken group was stepped");
    assert_eq!(
        healthy_steps.get(),
        100,
        "a poisoned group cannot stop unrelated groups: elections, heartbeats, and replication all \
         travel on the tick group 2 was denied"
    );
}

#[test]
fn a_pass_in_which_every_group_fails_still_visits_every_group() {
    let mut host = MultiRaftHost::new();
    host.open_group(1, FailingDriver::new(1, "poisoned"))
        .expect("open group 1");
    host.open_group(2, FailingDriver::new(2, "poisoned"))
        .expect("open group 2");

    let pass = host.tick_all();

    assert_eq!(pass.visited(), host.len());
    assert!(!pass.is_complete());
    assert_eq!(pass.failures().count(), 2);
    assert_eq!(pass.reports().count(), 0);
}

#[test]
fn a_retired_group_gives_its_driver_back_and_stops_being_stepped() {
    let broken_steps = StepCounter::default();
    let healthy_steps = StepCounter::default();
    let mut host = MultiRaftHost::new();
    host.open_group(
        1,
        FailingDriver::with_counter(1, "poisoned", StepCounter::clone(&broken_steps)),
    )
    .expect("open group 1");
    host.open_group(
        2,
        ApplyingDriver::with_counter(2, StepCounter::clone(&healthy_steps)),
    )
    .expect("open group 2");
    let _ = host.tick_all();

    let mut retired = host.remove_group(&1).expect("group 1 retires");

    assert!(!host.contains_group(&1));
    assert_eq!(host.len(), 1);
    let pass = host.tick_all();
    assert!(pass.is_complete(), "only the healthy group is left");
    assert_eq!(pass.visited(), 1);
    assert_eq!(
        broken_steps.get(),
        1,
        "a retired group consumes no further scheduling opportunity"
    );

    // The driver is still usable, which is what draining a group needs.
    assert_eq!(retired.metrics().group_id, 1);
    assert!(retired.step(GroupInput::Tick).is_err());
    assert_eq!(broken_steps.get(), 2, "the caller drove it, not the host");
}

#[test]
fn retiring_a_group_is_idempotent_and_the_key_reopens() {
    let mut host = MultiRaftHost::new();
    host.open_group(1, FailingDriver::new(1, "poisoned"))
        .expect("open group 1");

    assert!(host.remove_group(&1).is_some());
    assert!(host.remove_group(&1).is_none());
    assert!(host.remove_group(&1).is_none());
    assert!(host.is_empty());

    host.open_group(1, ApplyingDriver::new(1))
        .expect("the key reopens under a fresh driver");
    let pass = host.tick_all();
    assert!(pass.is_complete());
}

#[test]
fn traffic_for_a_retired_group_is_unknown_rather_than_reopening_it() {
    let mut host = MultiRaftHost::new();
    host.open_group(1, ApplyingDriver::new(1))
        .expect("open group 1");
    host.remove_group(&1).expect("group 1 retires");

    let error = host
        .step_group(
            &1,
            GroupInput::PeerMessage {
                envelope: support::envelope(1),
            },
        )
        .expect_err("late traffic does not resurrect a retired group");

    // The tombstone boundary, asserted in the direction this crate behaves: a
    // retired key and a key that never existed are the same answer, because
    // the retention horizon that would separate them is not this host's to
    // pick. A caller that must fence late traffic holds that tombstone itself.
    assert!(
        matches!(error, MultiRaftError::UnknownGroup { group_id: 1 }),
        "late traffic names the retired key: {error:?}"
    );
    let never_existed = host
        .step_group(&404, GroupInput::Tick)
        .expect_err("a key that never existed answers identically");
    assert_eq!(
        error.kind(),
        never_existed.kind(),
        "a retired key and a key that never existed project to one category"
    );
    assert_eq!(error.kind(), MultiRaftErrorKind::UnknownGroup);
    assert!(host.is_empty(), "the refused input opened nothing");
}

#[test]
fn a_typed_group_retires_the_same_way() {
    // The two hosts are line-for-line mirrors, and M1 was present in both
    // because the code was present in both. Every lifecycle guard is asserted
    // on each host rather than on the untyped one alone.
    let mut host = TypedMultiRaftHost::<u64, TypedCommand, TypedResult>::new();
    host.open_group(1, TypedFailingDriver::new(1))
        .expect("open group 1");
    host.open_group(2, TypedApplyingDriver::new(2))
        .expect("open group 2");

    let mut retired = host.remove_group(&1).expect("group 1 retires");

    assert!(!host.contains_group(&1));
    assert_eq!(host.len(), 1);
    assert!(host.remove_group(&1).is_none(), "retirement is idempotent");
    assert_eq!(retired.metrics().group_id, 1);
    assert!(retired.step(GroupInput::Tick).is_err());

    let pass = host.tick_all();
    assert!(pass.is_complete());
    assert_eq!(pass.visited(), 1);
}

#[test]
fn group_ids_are_the_order_a_pass_visits() {
    let mut host = MultiRaftHost::new();
    host.open_group(3, ApplyingDriver::new(3)).expect("open 3");
    host.open_group(1, ApplyingDriver::new(1)).expect("open 1");
    host.open_group(2, ApplyingDriver::new(2)).expect("open 2");

    let announced = host.group_ids().copied().collect::<Vec<_>>();
    let pass = host.tick_all();
    let visited = pass
        .outcomes()
        .iter()
        .map(|outcome| outcome.group_id)
        .collect::<Vec<_>>();

    assert_eq!(announced, vec![1, 2, 3], "keys are announced in key order");
    assert_eq!(
        visited, announced,
        "a pass visits exactly the keys `group_ids` announced, in that order"
    );
}

// ---------------------------------------------------------------------------
// M3 -- the host error implemented neither `Display` nor `Error`, and the
// driver surface collapsed every typed failure into a `Debug` string
// ---------------------------------------------------------------------------

#[test]
fn a_driver_failure_keeps_its_permanence_and_its_typed_cause() {
    let mut host = MultiRaftHost::new();
    host.open_group(1, FailingDriver::new(1, "log fsync lost its device"))
        .expect("open group 1");
    host.open_group(2, FailingDriver::transient(2, "peer connection reset"))
        .expect("open group 2");

    let pass = host.tick_all();
    let kinds = pass
        .failures()
        .map(|(_, error)| error.kind())
        .collect::<Vec<_>>();

    assert_eq!(
        kinds,
        vec![
            MultiRaftErrorKind::DriverPoisoned,
            MultiRaftErrorKind::DriverTransient
        ],
        "a permanent failure and a transient one are different facts: one says retire the group"
    );

    let (_, poisoned) = pass.failures().next().expect("group 1 failed");
    let MultiRaftError::Driver { kind, cause, .. } = poisoned else {
        panic!("expected a driver failure, got {poisoned:?}");
    };
    assert!(kind.is_permanent());
    let recovered = cause
        .downcast_ref::<ShardFailure>()
        .expect("the driver's own error type survives the host boundary");
    assert_eq!(
        recovered,
        &ShardFailure {
            shard: 1,
            detail: "log fsync lost its device",
        }
    );
}

#[test]
fn the_host_error_renders_and_chains_to_the_preserved_cause() {
    let mut host = MultiRaftHost::new();
    host.open_group(1, FailingDriver::new(1, "log fsync lost its device"))
        .expect("open group 1");

    let error = host
        .step_group(&1, GroupInput::Tick)
        .expect_err("the driver refuses");

    // `Display`, which this type had no implementation of at all.
    let rendered = render(&error);
    assert!(rendered.contains('1'), "renders the group: {rendered}");
    assert!(
        rendered.contains("log fsync lost its device"),
        "renders the preserved cause: {rendered}"
    );

    // `source()`, one link per real failure rather than one per boundary.
    let source = std::error::Error::source(&error).expect("the cause is reachable");
    assert!(
        source.downcast_ref::<ShardFailure>().is_some(),
        "the chain reaches the driver's own error, not a wrapper"
    );

    // Every variant renders, including the ones with no cause to chain to.
    for variant in [
        MultiRaftError::GroupAlreadyOpen { group_id: 1_u64 },
        MultiRaftError::UnknownGroup { group_id: 1 },
        MultiRaftError::WrongGroup {
            expected: 1,
            actual: 2,
        },
        MultiRaftError::InvalidReport {
            group_id: 1,
            field: "peer_messages",
            reported: 2,
        },
        MultiRaftError::UnrecognizedEvent {
            group_id: 1,
            field: "read_events",
        },
    ] {
        assert!(!render(&variant).is_empty(), "{variant:?} renders");
        assert!(
            std::error::Error::source(&variant).is_none(),
            "{variant:?} has no cause to chain to"
        );
    }
}

/// Requires `E: Error`, which `MultiRaftError` did not implement.
fn render<E: std::error::Error>(error: &E) -> String {
    error.to_string()
}

// --------------------------------------------------------------- typed fixtures

#[derive(Clone, Debug, Eq, PartialEq)]
struct TypedCommand(u64);

#[derive(Clone, Debug, Eq, PartialEq)]
struct TypedResult(u64);

#[derive(Debug)]
struct TypedApplyingDriver {
    group_id: u64,
    applied: u64,
}

impl TypedApplyingDriver {
    fn new(group_id: u64) -> Self {
        Self {
            group_id,
            applied: 0,
        }
    }
}

impl TypedGroupDriver<u64> for TypedApplyingDriver {
    type Command = TypedCommand;
    type CommandResult = TypedResult;

    fn step(
        &mut self,
        _input: GroupInput<u64, Self::Command>,
    ) -> Result<GroupStepReport<u64, Self::CommandResult>, DriverError> {
        self.applied += 1;
        let mut report = typed_report(self.group_id);
        report.applied.push(ApplyResult {
            index: LogIndex(self.applied),
            term: Term(1),
            result: TypedResult(self.applied),
            local_proposal_id: Some(LocalProposalId(self.applied)),
        });
        Ok(report)
    }

    fn metrics(&self) -> RaftGroupMetrics<u64> {
        metrics(self.group_id, self.applied)
    }
}

#[derive(Debug)]
struct TypedFailingDriver {
    group_id: u64,
}

impl TypedFailingDriver {
    fn new(group_id: u64) -> Self {
        Self { group_id }
    }
}

impl TypedGroupDriver<u64> for TypedFailingDriver {
    type Command = TypedCommand;
    type CommandResult = TypedResult;

    fn step(
        &mut self,
        _input: GroupInput<u64, Self::Command>,
    ) -> Result<GroupStepReport<u64, Self::CommandResult>, DriverError> {
        Err(DriverError::new(
            DriverErrorKind::Poisoned,
            ErrorCause::new(ShardFailure {
                shard: self.group_id,
                detail: "typed shard driver failed",
            }),
        ))
    }

    fn metrics(&self) -> RaftGroupMetrics<u64> {
        metrics(self.group_id, 0)
    }
}

fn typed_report(group_id: u64) -> GroupStepReport<u64, TypedResult> {
    GroupStepReport {
        group_id,
        peer_messages: Vec::new(),
        applied: Vec::new(),
        proposal_events: Vec::new(),
        read_events: Vec::new(),
        leadership_transfer_events: Vec::new(),
        snapshot_events: Vec::new(),
        membership_events: Vec::new(),
        metrics: None,
    }
}
