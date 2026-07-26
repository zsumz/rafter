//! The shipped blanket driver, on the path every real embedder takes.
//!
//! `impl TypedGroupDriver for RaftGroup` is the only driver implementation
//! this crate ships. It used to produce its error with
//! `map_err(|error| format!("{error:?}"))`, which threw away twenty typed
//! `GroupError` variants, a `source()` chain, and an `ErrorCause` a caller
//! could downcast — so a caller could not tell a poison, which is permanent,
//! from a failure that might not recur.
//!
//! These tests drive a real `RaftGroup` over a real durable runtime.

use std::{error::Error, fmt};

use rafter::{LocalProposalId, LogIndex, NodeConfig, NodeId, Role};
use rafter_app::{
    error::{GroupError, StateMachineOperation},
    group::{GroupInput, RaftGroup},
    proposal::Proposal,
    state_machine::{
        ApplyBatch, ApplyResult, ReadBarrier, ReplicatedStateMachine, SnapshotSupport,
    },
};
use rafter_multiraft::{DriverErrorKind, MultiRaftError, MultiRaftErrorKind, TypedMultiRaftHost};
use rafter_runtime::{DurableRaftNode, RaftRuntimeError};
use rafter_storage::InMemoryRaftHardStateStore;

type Host = TypedMultiRaftHost<u64, Command, u64>;
type ShardGroupError = GroupError<ShardFault, RaftRuntimeError>;

#[test]
fn a_failure_that_does_not_poison_is_transient_and_keeps_its_typed_cause() {
    let mut host = Host::new();
    host.open_group(1, group(1)).expect("open group 1");
    elect(&mut host);

    // `encode_command` fails before anything reaches the log, and `rafter-app`
    // does not poison for it: the group is still usable afterwards.
    let error =
        propose(&mut host, Command::Unencodable, 1).expect_err("the command has no encoding");

    assert_eq!(error.kind(), MultiRaftErrorKind::DriverTransient);
    assert!(
        !driver_kind(&error).is_permanent(),
        "a group that did not poison must not be reported as finished"
    );
    let cause = driver_cause(&error);
    assert!(
        matches!(
            cause,
            ShardGroupError::StateMachine {
                operation: StateMachineOperation::EncodeCommand,
                ..
            }
        ),
        "the typed variant survives rather than a rendering of it: {cause:?}"
    );

    // And the claim the kind makes is true: the group still works.
    propose(&mut host, Command::Add(7), 2).expect("the group is not poisoned");
}

#[test]
fn the_failure_that_poisons_is_reported_as_permanent_not_the_one_after_it() {
    let mut host = Host::new();
    host.open_group(1, group(1)).expect("open group 1");
    elect(&mut host);

    // `apply_batch` fails on a committed entry, which poisons the group. The
    // error returned is `StateMachine`, not `Poisoned` -- classifying by
    // variant would call this first, group-ending failure transient.
    let error = propose(&mut host, Command::Unappliable, 1).expect_err("the apply fails");

    assert_eq!(
        error.kind(),
        MultiRaftErrorKind::DriverPoisoned,
        "the driver reports the permanence it observed, not what the variant implies"
    );
    assert!(
        driver_kind(&error).is_permanent(),
        "the failure that poisoned the group retires it"
    );
    let cause = driver_cause(&error);
    assert!(
        matches!(
            cause,
            ShardGroupError::StateMachine {
                operation: StateMachineOperation::ApplyBatch,
                ..
            }
        ),
        "the poisoning failure reports what actually broke: {cause:?}"
    );

    // Every later step is refused, permanently, and still says so.
    let later = propose(&mut host, Command::Add(1), 2).expect_err("a poisoned group refuses");
    assert_eq!(later.kind(), MultiRaftErrorKind::DriverPoisoned);
    assert!(
        matches!(driver_cause(&later), ShardGroupError::Poisoned { .. }),
        "the later refusal is a poison refusal: {:?}",
        driver_cause(&later)
    );

    let tick = host.tick_all();
    assert_eq!(
        tick.visited(),
        1,
        "a poisoned group still gets its own pass"
    );
    assert!(tick
        .failures()
        .all(|(_, error)| error.kind() == MultiRaftErrorKind::DriverPoisoned));

    // Which is what the retirement API is for.
    assert!(host.remove_group(&1).is_some());
    assert!(host.tick_all().is_complete());
}

#[test]
fn a_poisoned_group_renders_and_chains_to_the_application_error() {
    let mut host = Host::new();
    host.open_group(1, group(1)).expect("open group 1");
    elect(&mut host);
    let error = propose(&mut host, Command::Unappliable, 1).expect_err("the apply fails");

    let rendered = error.to_string();
    assert!(
        rendered.contains("cannot apply this command"),
        "the host error renders the application's own message: {rendered}"
    );

    let mut chain: Vec<String> = Vec::new();
    let mut link: Option<&(dyn Error + 'static)> = error.source();
    while let Some(current) = link {
        chain.push(current.to_string());
        link = current.source();
    }
    assert!(
        chain
            .last()
            .is_some_and(|last| last.contains("cannot apply this command")),
        "the chain ends at the state machine's own error: {chain:?}"
    );
}

// ------------------------------------------------------------------ fixtures

fn group(group_id: u64) -> RaftGroup<u64, ShardStateMachine, DurableRaftNode> {
    let config = NodeConfig::new(NodeId(1), Vec::new(), 1)
        .expect("single-voter config is valid")
        .with_pre_vote(false);
    let raft = DurableRaftNode::new(config, InMemoryRaftHardStateStore::new())
        .expect("in-memory durable node opens");
    RaftGroup::new(group_id, NodeId(1), raft, ShardStateMachine::default())
}

fn elect(host: &mut Host) {
    for _ in 0..4 {
        host.step_group(&1, GroupInput::Tick)
            .expect("a single-voter group elects itself");
        if host
            .metrics()
            .groups
            .iter()
            .any(|metrics| metrics.role == Role::Leader)
        {
            return;
        }
    }
    panic!("the single-voter group never became leader");
}

fn propose(host: &mut Host, command: Command, id: u64) -> Result<(), MultiRaftError<u64>> {
    host.step_group(
        &1,
        GroupInput::Proposal {
            proposal: Proposal {
                local_proposal_id: LocalProposalId(id),
                client_request_id: None,
                command,
            },
        },
    )
    .map(|_| ())
}

fn driver_kind(error: &MultiRaftError<u64>) -> DriverErrorKind {
    let MultiRaftError::Driver { kind, .. } = error else {
        panic!("expected a driver failure, got {error:?}");
    };
    *kind
}

fn driver_cause(error: &MultiRaftError<u64>) -> &ShardGroupError {
    let MultiRaftError::Driver { cause, .. } = error else {
        panic!("expected a driver failure, got {error:?}");
    };
    cause
        .downcast_ref::<ShardGroupError>()
        .expect("the app layer's typed error survives the host boundary")
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Command {
    Add(u64),
    /// Refused by `encode_command`, which does not poison the group.
    Unencodable,
    /// Refused by `apply_batch` after it commits, which does poison it.
    Unappliable,
}

#[derive(Debug, Eq, PartialEq)]
struct ShardFault(&'static str);

impl fmt::Display for ShardFault {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for ShardFault {}

#[derive(Debug, Default)]
struct ShardStateMachine {
    applied_index: LogIndex,
    total: u64,
}

impl ReplicatedStateMachine for ShardStateMachine {
    type Command = Command;
    type CommandResult = u64;
    type Query = ();
    type QueryResult = u64;
    type Error = ShardFault;

    const SNAPSHOT_SUPPORT: SnapshotSupport = SnapshotSupport::Unsupported;

    fn applied_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(self.applied_index)
    }

    fn encode_command(&self, command: &Self::Command) -> Result<Vec<u8>, Self::Error> {
        match command {
            Command::Add(amount) => Ok(amount.to_be_bytes().to_vec()),
            Command::Unappliable => Ok(b"unappliable".to_vec()),
            Command::Unencodable => Err(ShardFault("this command has no encoding")),
        }
    }

    fn decode_command(&self, payload: &[u8]) -> Result<Self::Command, Self::Error> {
        if payload == b"unappliable" {
            return Ok(Command::Unappliable);
        }
        let bytes: [u8; 8] = payload
            .try_into()
            .map_err(|_| ShardFault("payload is not an eight-byte amount"))?;
        Ok(Command::Add(u64::from_be_bytes(bytes)))
    }

    fn apply_batch(
        &mut self,
        batch: ApplyBatch<Self::Command>,
    ) -> Result<Vec<ApplyResult<Self::CommandResult>>, Self::Error> {
        let mut results = Vec::with_capacity(batch.entries.len());
        for entry in batch.entries {
            let Command::Add(amount) = entry.command else {
                return Err(ShardFault("this state machine cannot apply this command"));
            };
            self.total += amount;
            self.applied_index = entry.index;
            results.push(ApplyResult {
                index: entry.index,
                term: entry.term,
                result: self.total,
                local_proposal_id: entry.local_proposal_id,
            });
        }
        Ok(results)
    }

    fn read(&self, _query: Self::Query, _barrier: ReadBarrier) -> Result<u64, Self::Error> {
        Ok(self.total)
    }
}
