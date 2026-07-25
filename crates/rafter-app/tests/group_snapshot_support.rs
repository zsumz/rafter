//! A state machine's snapshot declaration, and what a group does with it.
//!
//! A required const with no default is the only shape in which "this
//! application has no snapshot format" is a sentence the type system can read.
//! These tests pin the three answers a group can reach: refuse before touching
//! an `Unsupported` state machine, install into a `Supported` one, and name the
//! contradiction when a `Supported` one refuses anyway.

#![allow(clippy::wildcard_imports)]

mod support;

use support::*;

/// A state machine with no snapshot format that records every call it receives.
///
/// It inherits both provided snapshot bodies, which is exactly what an
/// `Unsupported` declaration is for.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct UnsupportedStateMachine {
    applied_index: LogIndex,
    install_calls: usize,
}

impl ReplicatedStateMachine for UnsupportedStateMachine {
    type Command = Vec<u8>;
    type CommandResult = Vec<u8>;
    type Query = Vec<u8>;
    type QueryResult = Option<Vec<u8>>;
    type Error = RecordingStateMachineError;

    const SNAPSHOT_SUPPORT: SnapshotSupport = SnapshotSupport::Unsupported;

    fn applied_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.applied_index)
    }

    fn encode_command(&self, command: &Self::Command) -> Result<Vec<u8>, Self::Error> {
        Ok(command.clone())
    }

    fn decode_command(&self, payload: &[u8]) -> Result<Self::Command, Self::Error> {
        Ok(payload.to_vec())
    }

    fn apply_batch(
        &mut self,
        batch: ApplyBatch<Self::Command>,
    ) -> Result<Vec<ApplyResult<Self::CommandResult>>, Self::Error> {
        Ok(batch
            .entries
            .into_iter()
            .map(|entry| {
                self.applied_index = entry.index;
                ApplyResult {
                    index: entry.index,
                    term: entry.term,
                    result: entry.command,
                    local_proposal_id: entry.local_proposal_id,
                }
            })
            .collect())
    }

    fn read(
        &self,
        query: Self::Query,
        _barrier: ReadBarrier,
    ) -> Result<Self::QueryResult, Self::Error> {
        Ok(Some(query))
    }

    /// Written only so a test can prove the group never reaches it. A real
    /// `Unsupported` state machine writes no body at all and inherits this
    /// refusal, which is what [`MisdeclaringStateMachine`] exercises.
    fn install_snapshot(
        &mut self,
        snapshot: ApplicationSnapshot,
    ) -> Result<(), ApplicationSnapshotError<Self::Error>> {
        let _ = snapshot;
        self.install_calls += 1;
        Err(ApplicationSnapshotError::Unsupported)
    }
}

/// A state machine that declares support and then inherits the provided bodies.
///
/// This is the loophole the provided bodies would open without the const: the
/// declaration says one thing and the implementation says another, and only a
/// group can catch it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct MisdeclaringStateMachine {
    applied_index: LogIndex,
}

impl ReplicatedStateMachine for MisdeclaringStateMachine {
    type Command = Vec<u8>;
    type CommandResult = Vec<u8>;
    type Query = Vec<u8>;
    type QueryResult = Option<Vec<u8>>;
    type Error = RecordingStateMachineError;

    const SNAPSHOT_SUPPORT: SnapshotSupport = SnapshotSupport::Supported;

    fn applied_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.applied_index)
    }

    fn encode_command(&self, command: &Self::Command) -> Result<Vec<u8>, Self::Error> {
        Ok(command.clone())
    }

    fn decode_command(&self, payload: &[u8]) -> Result<Self::Command, Self::Error> {
        Ok(payload.to_vec())
    }

    fn apply_batch(
        &mut self,
        _batch: ApplyBatch<Self::Command>,
    ) -> Result<Vec<ApplyResult<Self::CommandResult>>, Self::Error> {
        Ok(Vec::new())
    }

    fn read(
        &self,
        query: Self::Query,
        _barrier: ReadBarrier,
    ) -> Result<Self::QueryResult, Self::Error> {
        Ok(Some(query))
    }
}

fn scripted_runtime() -> ScriptedRuntime {
    ScriptedRuntime::with_terms([(LogIndex(2), Term(1)), (LogIndex(3), Term(1))])
}

#[test]
fn an_unsupported_state_machine_refuses_a_raft_driven_install() {
    let snapshot = test_snapshot(8);
    let mut group = RaftGroup::new(
        7,
        NodeId(1),
        scripted_runtime(),
        UnsupportedStateMachine::default(),
    );

    let error = group
        .apply_raft_outputs(vec![RaftOutput::ApplySnapshot { snapshot }])
        .expect_err("a replica that cannot install a snapshot has no way forward");

    assert!(matches!(
        error,
        GroupError::SnapshotsUnsupported {
            snapshot_index: LogIndex(8),
        }
    ));
    assert!(matches!(
        group.fatal_state(),
        GroupFatalState::Poisoned { .. }
    ));
}

/// The load-bearing negative. Refusing *after* the call would leave the
/// application having seen a payload it declared it cannot interpret, which is
/// the failure mode the declaration exists to prevent.
#[test]
fn an_unsupported_state_machine_is_not_called_before_the_refusal() {
    let snapshot = test_snapshot(8);
    let mut group = RaftGroup::new(
        7,
        NodeId(1),
        scripted_runtime(),
        UnsupportedStateMachine::default(),
    );

    let _ = group
        .apply_raft_outputs(vec![RaftOutput::ApplySnapshot { snapshot }])
        .expect_err("the install is refused");

    assert_eq!(group.state_machine().install_calls, 0);
    assert_eq!(group.state_machine().applied_index, LogIndex::ZERO);
}

/// The refusal has no underlying error, so it invents none: the state machine
/// was never called and there is nothing of its to preserve.
#[test]
fn an_unsupported_refusal_records_no_poison_cause() {
    let snapshot = test_snapshot(8);
    let mut group = RaftGroup::new(
        7,
        NodeId(1),
        scripted_runtime(),
        UnsupportedStateMachine::default(),
    );

    let _ = group
        .apply_raft_outputs(vec![RaftOutput::ApplySnapshot { snapshot }])
        .expect_err("the install is refused");

    assert!(group.poison_cause().is_none());
}

#[test]
fn a_supported_state_machine_installs_as_before() {
    let snapshot = test_snapshot(8);
    let mut group = scripted_group(RecordingStateMachine::default());

    let report = group
        .apply_raft_outputs(vec![RaftOutput::ApplySnapshot {
            snapshot: snapshot.clone(),
        }])
        .expect("a declared-supported state machine installs");

    assert_eq!(
        report.snapshot_events,
        vec![SnapshotEvent::Apply {
            group_id: 7,
            snapshot,
        }]
    );
    assert_eq!(group.state_machine().installed_snapshots.len(), 1);
    assert_eq!(group.state_machine().applied_index, LogIndex(8));
    assert!(matches!(group.fatal_state(), GroupFatalState::Healthy));
}

/// The loophole check, and the reason the provided bodies are safe to ship: a
/// state machine that declares support and forgets to implement it still fails,
/// but it fails with an error that names the mistake instead of a generic
/// install failure a reader has to interpret.
#[test]
fn a_state_machine_that_declares_support_and_inherits_the_default_is_misdeclared() {
    let snapshot = test_snapshot(8);
    let mut group = RaftGroup::new(
        7,
        NodeId(1),
        scripted_runtime(),
        MisdeclaringStateMachine::default(),
    );

    let error = group
        .apply_raft_outputs(vec![RaftOutput::ApplySnapshot { snapshot }])
        .expect_err("an inherited body contradicts a `Supported` declaration");

    assert!(matches!(
        error,
        GroupError::SnapshotSupportMisdeclared {
            snapshot_index: LogIndex(8),
        }
    ));
    assert!(
        !matches!(error, GroupError::SnapshotsUnsupported { .. }),
        "a misdeclaration is a different fault from a declared limitation"
    );
    assert!(matches!(
        group.fatal_state(),
        GroupFatalState::Poisoned { .. }
    ));
}

/// A declared-supported state machine that genuinely fails still reports its
/// own error, and the group keeps that error as the cause every later refusal
/// reports.
#[test]
fn an_install_failure_still_poisons_with_the_state_machine_error() {
    let snapshot = test_snapshot(9);
    let mut group = scripted_group(RecordingStateMachine {
        fail_install_snapshot: true,
        ..RecordingStateMachine::default()
    });

    let error = group
        .apply_raft_outputs(vec![RaftOutput::ApplySnapshot { snapshot }])
        .expect_err("a failed install is fatal");

    assert!(matches!(
        error,
        GroupError::StateMachine {
            operation: StateMachineOperation::InstallSnapshot,
            ref source,
        } if **source == RecordingStateMachineError::InstallSnapshot
    ));
    assert_eq!(
        group
            .poison_cause()
            .and_then(ErrorCause::downcast_ref::<RecordingStateMachineError>),
        Some(&RecordingStateMachineError::InstallSnapshot)
    );
}

/// The property that makes the reference program's durability rule checkable: a
/// release gate can read the declaration at compile time, without an instance
/// and without running anything.
#[test]
fn snapshot_support_is_readable_without_an_instance() {
    const RECORDING: SnapshotSupport =
        <RecordingStateMachine as ReplicatedStateMachine>::SNAPSHOT_SUPPORT;
    const UNSUPPORTED: SnapshotSupport =
        <UnsupportedStateMachine as ReplicatedStateMachine>::SNAPSHOT_SUPPORT;

    assert_eq!(RECORDING, SnapshotSupport::Supported);
    assert_eq!(UNSUPPORTED, SnapshotSupport::Unsupported);
}
