//! What the driver says about a write it could not finish, and about a group
//! that died holding one.
//!
//! Every scenario here comes from the adversarial review of
//! `TransportRaftDriver`, kept with its fixture and inverted where the review
//! recorded a defect.

#![allow(clippy::wildcard_imports)]

mod support;

use std::collections::{BTreeMap, BTreeSet};

use rafter_runtime::DurableRaftNode;
use rafter_runtime_api::PersistedRaftRuntime;
use rafter_service::{AuthenticatedPeerEnvelope, TransportDriverOptions, TransportRaftDriver};
use support::transport::*;
use support::*;

// ---------------------------------------------------------------------------
// A driver reports what it observed, and says "unknown" for everything else.
//
// Every scenario below comes from the adversarial review of this driver, kept
// with its fixture and inverted where the review recorded a defect.
// ---------------------------------------------------------------------------

/// Builds one group over a state machine the caller chose.
fn group_with_app(node_id: u64, peers: &[u64], app: KvStateMachine) -> NumberedGroup {
    let config = NodeConfig::new(
        NodeId(node_id),
        peers.iter().copied().map(NodeId).collect(),
        3,
    )
    .expect("static Raft config is valid");
    let raft = DurableRaftNode::new(config, rafter_storage::InMemoryRaftHardStateStore::new())
        .expect("in-memory durable node opens");
    RaftGroup::new(GROUP, NodeId(node_id), raft, app)
}

fn driver_over_app(node_id: u64, peers: &[u64], app: KvStateMachine) -> (Driver, QueueTransport) {
    let transport = QueueTransport::default();
    let validator = Validator {
        transport: transport.clone(),
        authorized: peers.iter().copied().map(NodeId).collect(),
        nameable: None,
    };
    let driver = TransportRaftDriver::new(
        group_with_app(node_id, peers, app),
        Vec::new(),
        transport.clone(),
        validator,
        TransportDriverOptions::default(),
    )
    .expect("a quiescent group is adoptable");
    (driver, transport)
}

fn failing_apply() -> KvStateMachine {
    KvStateMachine {
        fail_apply: true,
        ..KvStateMachine::default()
    }
}

fn elect_single_voter(driver: &Driver) {
    for _ in 0..16 {
        if driver.handle().metrics().expect("metrics").current().role == Role::Leader {
            return;
        }
        driver.tick().expect("a tick advances the protocol");
    }
    panic!("the single-voter replica never took leadership");
}

fn write_fate(error: &WriteError) -> WriteFate {
    error.fate()
}

/// A single voter commits and applies inside the very step that proposes, so a
/// refused apply poisons a group whose entry is already durable and committed.
/// The driver used to answer `WriteFate::NotAppended` — "it cannot commit, now
/// or later, and its request identity is still unused" — for that entry, which
/// invites a caller to retry under a fresh identity and apply it twice.
#[test]
fn a_poisoning_apply_reports_unknown_for_an_entry_that_is_committed() {
    let (driver, _transport) = driver_over_app(1, &[], failing_apply());
    elect_single_voter(&driver);

    let handle = driver.handle();
    let mut write = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    let outcome = poll_once(&mut write).expect("the failing step resolves the waiter in place");
    let error = outcome.expect_err("a poisoning apply cannot produce a receipt");

    // Read the log through the driver's own observation seam, so the test is
    // about fate rather than about instrumentation.
    let (last_log_index, commit_index) = driver
        .with_group(|group| {
            (
                group.runtime().last_log_index(),
                group.runtime().commit_index(),
            )
        })
        .expect("the driver still holds its group");
    assert!(
        last_log_index >= LogIndex(2) && commit_index >= LogIndex(2),
        "the fixture needs the proposal appended and committed: \
         last_log_index={last_log_index}, commit_index={commit_index}"
    );

    assert!(
        matches!(
            error,
            WriteError::UnknownOutcome {
                reason: UnknownOutcomeReason::GroupPoisoned,
                ..
            }
        ),
        "got {error:?}"
    );
    assert!(
        write_fate(&error).may_commit(),
        "a committed entry may still take effect"
    );
}

/// One group, one fault, two drivers, one answer. The review ran this pair to
/// show the two disagreeing; it is kept to show them agreeing.
#[test]
fn both_drivers_call_the_same_poisoning_apply_unknown() {
    let (driver, _transport) = driver_over_app(1, &[], failing_apply());
    elect_single_voter(&driver);
    let transport_error = poll_once(&mut Box::pin(
        driver
            .handle()
            .write(("alpha".to_owned(), "one".to_owned())),
    ))
    .expect("the failing step resolves in place")
    .expect_err("a poisoning apply cannot produce a receipt");

    let in_memory = KvDriver::new_elected(
        NodeId(1),
        vec![group_with_app_for_in_memory(failing_apply())],
    )
    .expect("primary elects");
    let in_memory_error = block_on(
        in_memory
            .handle()
            .write(("alpha".to_owned(), "one".to_owned())),
    )
    .expect_err("a poisoning apply cannot produce a receipt");

    for (label, error) in [
        ("transport", &transport_error),
        ("in-memory", &in_memory_error),
    ] {
        assert!(
            matches!(
                error,
                WriteError::UnknownOutcome {
                    reason: UnknownOutcomeReason::GroupPoisoned,
                    ..
                }
            ),
            "{label}: {error:?}"
        );
    }
}

fn group_with_app_for_in_memory(app: KvStateMachine) -> KvGroup {
    let config = NodeConfig::new(NodeId(1), Vec::new(), 3).expect("static Raft config is valid");
    let raft = DurableRaftNode::new(config, rafter_storage::InMemoryRaftHardStateStore::new())
        .expect("in-memory durable node opens");
    RaftGroup::new((), NodeId(1), raft, app)
}

/// A poison hands the group's pending waiters to `poisoned_waiters` and emits
/// nothing further for them. The driver drains that table, so a client that was
/// mid-flight when the group died is told so instead of waiting forever.
#[test]
fn a_poison_resolves_every_in_flight_waiter() {
    let (leader, leader_transport) = driver_over_app(1, &[2], failing_apply());
    let (follower, follower_transport) = driver_over_app(2, &[1], KvStateMachine::default());
    let nodes = BTreeMap::from([
        (NodeId(1), (leader.clone(), leader_transport)),
        (NodeId(2), (follower, follower_transport)),
    ]);

    for _ in 0..64 {
        if leader.handle().metrics().expect("metrics").current().role == Role::Leader {
            break;
        }
        leader.tick().expect("a tick advances the protocol");
        exchange_fallibly(&nodes);
    }
    assert_eq!(
        leader.handle().metrics().expect("metrics").current().role,
        Role::Leader,
        "the fixture needs an elected leader"
    );

    let handle = leader.handle();
    let mut write = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    assert!(
        poll_once(&mut write).is_none(),
        "the write cannot complete before the follower acknowledges"
    );
    assert_eq!(leader.pending_writes().len(), 1);

    // Drive until the leader's apply fails: that is the poison.
    let mut poisoned = false;
    for _ in 0..64 {
        if exchange_fallibly(&nodes) || leader.tick().is_err() {
            poisoned = true;
            break;
        }
    }
    assert!(poisoned, "the fixture needs the leader's apply to fail");

    let outcome = poll_once(&mut write).expect("the poisoned group resolves its client");
    let error = outcome.expect_err("a poisoned group cannot produce a receipt");
    assert!(
        matches!(
            error,
            WriteError::UnknownOutcome {
                reason: UnknownOutcomeReason::GroupPoisoned,
                ..
            }
        ),
        "got {error:?}"
    );
    assert!(
        leader.pending_writes().is_empty(),
        "the driver holds no unresolved write"
    );
    assert!(
        leader
            .with_group(|group| group.poisoned_waiters().is_empty())
            .expect("the driver still holds its poisoned group"),
        "the group's poisoned-waiter table was drained"
    );
}

/// Delivers what each transport accepted, reporting whether any delivery failed
/// — which is how a poison surfaces to this fixture.
fn exchange_fallibly(nodes: &BTreeMap<NodeId, (Driver, QueueTransport)>) -> bool {
    let mut failed = false;
    let frames = nodes
        .values()
        .flat_map(|(_, transport)| transport.take_deliverable())
        .collect::<Vec<_>>();
    for envelope in frames {
        let Some((driver, _)) = nodes.get(&envelope.to) else {
            continue;
        };
        let authenticated = AuthenticatedPeerEnvelope {
            group_id: envelope.group_id,
            authenticated_peer: Principal::for_node(envelope.from),
            raft_from: envelope.from,
            raft_to: envelope.to,
            message: envelope.message,
        };
        if driver.deliver(authenticated).is_err() {
            failed = true;
        }
    }
    failed
}

/// Group failures reach clients through the same category mapping the
/// in-memory driver uses, so `Poisoned` is a category rather than a transport
/// fault wrapping a wrapped error.
#[test]
fn a_poisoned_group_reports_poisoned_rather_than_transport() {
    let (driver, _transport) = driver_over_app(1, &[], failing_apply());
    elect_single_voter(&driver);
    let handle = driver.handle();

    let mut first = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    let _ = poll_once(&mut first).expect("the poisoning write resolves in place");

    let mut second = Box::pin(handle.write(("beta".to_owned(), "two".to_owned())));
    let write_error = poll_once(&mut second)
        .expect("the refusal resolves in place")
        .expect_err("a poisoned group cannot produce a receipt");
    let mut read = Box::pin(handle.read("alpha".to_owned(), ReadConsistency::Linearizable));
    let read_error = poll_once(&mut read)
        .expect("the refusal resolves in place")
        .expect_err("a poisoned group cannot produce a receipt");

    assert!(
        matches!(write_error, WriteError::Poisoned { .. }),
        "got {write_error:?}"
    );
    assert!(
        matches!(read_error, ReadError::Poisoned { .. }),
        "got {read_error:?}"
    );
}

// ---------------------------------------------------------------------------
// The fate rule applied to the two group errors that prove their own refusal.
//
// `NotAppended` is reported only where the refusal is the whole event. These
// two are, and each test asserts the evidence that makes it provable rather
// than the fate alone.
// ---------------------------------------------------------------------------

/// `reject_if_poisoned` is `step_with_options`'s first statement, so a write
/// submitted to an already-poisoned group is refused before the group does
/// anything at all. This is the most common failing-write path a poisoned
/// replica has — every write after the first — and `Unresolved` told those
/// callers their request identity might be spent.
#[test]
fn a_write_to_an_already_poisoned_group_is_not_appended() {
    let (driver, _transport) = driver_over_app(1, &[], failing_apply());
    elect_single_voter(&driver);
    let handle = driver.handle();

    // The first write is the one that poisons; it is correctly unknown.
    let mut first = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    let _ = poll_once(&mut first).expect("the poisoning write resolves in place");
    let before = driver
        .with_group(|group| group.runtime().last_log_index())
        .expect("the driver still holds its group");

    let mut second = Box::pin(handle.write(("beta".to_owned(), "two".to_owned())));
    let error = poll_once(&mut second)
        .expect("the refusal resolves in place")
        .expect_err("a poisoned group cannot produce a receipt");

    assert_eq!(
        driver
            .with_group(|group| group.runtime().last_log_index())
            .expect("the driver still holds its group"),
        before,
        "the refused write reached no log, which is what makes the fate provable"
    );
    assert_eq!(
        write_fate(&error),
        WriteFate::NotAppended,
        "the group refused before it proposed: {error:?}"
    );
    assert!(
        !write_fate(&error).may_commit(),
        "nothing was proposed, so nothing can commit later"
    );
}

/// `step_proposal` encodes the command before it records the proposal and
/// before it hands anything to the runtime, so an encoder that refuses is a
/// refusal the driver watched happen.
#[test]
fn an_encode_failure_is_not_appended() {
    let (driver, _transport) = driver_over_app(
        1,
        &[],
        KvStateMachine {
            fail_encode: true,
            ..KvStateMachine::default()
        },
    );
    elect_single_voter(&driver);
    let handle = driver.handle();

    let mut write = Box::pin(handle.write(("key".to_owned(), "value".to_owned())));
    let error = poll_once(&mut write)
        .expect("the failing step resolves the waiter in place")
        .expect_err("an encode failure is not a successful write");

    assert_eq!(
        driver
            .with_group(|group| group.metrics().pending_proposals)
            .expect("the driver still holds its group"),
        0,
        "the group tracks no proposal, which is what makes the fate provable"
    );
    assert!(
        matches!(
            error,
            WriteError::StateMachine {
                operation: StateMachineOperation::EncodeCommand,
                ..
            }
        ),
        "the operation that surfaced it is preserved: {error:?}"
    );
    assert_eq!(write_fate(&error), WriteFate::NotAppended);
}

/// The apply side of the same rule, kept beside it so the boundary is visible:
/// an apply runs after the append, on an entry the log already holds, so its
/// state-machine failure stays unresolved.
#[test]
fn an_apply_failure_is_still_unresolved() {
    let (driver, _transport) = driver_over_app(1, &[], failing_apply());
    elect_single_voter(&driver);
    let handle = driver.handle();

    let mut write = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    let error = poll_once(&mut write)
        .expect("the failing step resolves the waiter in place")
        .expect_err("a poisoning apply cannot produce a receipt");

    assert!(
        write_fate(&error).may_commit(),
        "the entry is in the log and may still take effect: {error:?}"
    );
}

// ---------------------------------------------------------------------------
// The poison drain on the leadership-transfer step.
//
// The drain's own design chose its call sites by "can this poison" and listed
// the transfer step among them; the transfer stepped the group directly and
// drained on neither path.
// ---------------------------------------------------------------------------

/// Appends a tracked proposal and never commits it, then commits and applies on
/// the leadership-transfer step — which is where the state machine refuses and
/// the group poisons, capturing the pending proposal on its way down.
#[derive(Clone, Debug, Eq, PartialEq)]
struct TransferPoisonRuntime {
    commit_index: LogIndex,
}

impl PersistedRaftRuntime for TransferPoisonRuntime {
    type Error = rafter_runtime::RaftRuntimeError;

    fn id(&self) -> NodeId {
        NodeId(1)
    }
    fn leader_hint(&self) -> Option<NodeId> {
        Some(NodeId(1))
    }
    fn role(&self) -> Role {
        Role::Leader
    }
    fn current_term(&self) -> Term {
        Term(1)
    }
    fn commit_index(&self) -> LogIndex {
        self.commit_index
    }
    fn last_log_index(&self) -> LogIndex {
        LogIndex(1)
    }
    fn snapshot_index(&self) -> LogIndex {
        LogIndex::ZERO
    }
    fn committed_application_index_through(&self, index: LogIndex) -> LogIndex {
        std::cmp::min(index, self.commit_index)
    }
    fn membership(&self) -> MembershipConfig {
        MembershipConfig::stable(
            MembershipSet::new(vec![NodeId(1), NodeId(2)], Vec::new())
                .expect("scripted membership is valid"),
        )
    }
    /// Never mid-change, so the two memberships are one. Asserted rather than
    /// inherited: only this fixture can make that claim about itself.
    fn committed_membership(&self) -> MembershipConfig {
        self.membership()
    }
    fn replication(&self) -> Vec<ReplicationProgress> {
        Vec::new()
    }
    fn term_at_index(&self, index: LogIndex) -> Option<Term> {
        (index <= LogIndex(1)).then_some(Term(1))
    }

    fn step(&mut self, input: RaftInput) -> Result<Vec<RaftOutput>, Self::Error> {
        match input {
            RaftInput::TrackedClientProposal { proposal_id, .. } => {
                Ok(vec![RaftOutput::LocalProposalAppended {
                    proposal_id,
                    index: LogIndex(1),
                    term: Term(1),
                }])
            }
            RaftInput::TransferLeadership { .. } => {
                self.commit_index = LogIndex(1);
                Ok(vec![RaftOutput::Apply {
                    index: LogIndex(1),
                    term: Term(1),
                    payload: SharedPayload::from(&b"poison\nvalue"[..]),
                    local_proposal_id: None,
                }])
            }
            _ => Ok(Vec::new()),
        }
    }

    fn step_proposal_batch(
        &mut self,
        _proposals: Vec<ClientProposalInput>,
    ) -> Result<Vec<RaftOutput>, Self::Error> {
        Ok(Vec::new())
    }

    fn step_batch(&mut self, inputs: Vec<RaftInput>) -> Result<Vec<RaftOutput>, Self::Error> {
        let mut outputs = Vec::new();
        for input in inputs {
            outputs.extend(self.step(input)?);
        }
        Ok(outputs)
    }
}

type TransferDriver =
    TransportRaftDriver<u64, KvStateMachine, TransferPoisonRuntime, QueueTransport, Validator>;

fn transfer_poison_driver() -> TransferDriver {
    let transport = QueueTransport::default();
    let validator = Validator {
        transport: transport.clone(),
        authorized: BTreeSet::from([NodeId(2)]),
        nameable: None,
    };
    TransportRaftDriver::new(
        RaftGroup::new(
            GROUP,
            NodeId(1),
            TransferPoisonRuntime {
                commit_index: LogIndex::ZERO,
            },
            failing_apply(),
        ),
        Vec::new(),
        transport,
        validator,
        TransportDriverOptions::default(),
    )
    .expect("a quiescent group is adoptable")
}

/// A poison raised by the leadership-transfer step captures the in-flight write.
/// The drain now runs at that site, so the client holds its answer when the
/// call returns rather than waiting for an unrelated later call to rescue it.
#[test]
fn a_poison_on_the_leadership_transfer_step_resolves_its_client() {
    let driver = transfer_poison_driver();
    let handle = driver.handle();

    let mut write = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    assert!(
        poll_once(&mut write).is_none(),
        "the write is appended and pending"
    );

    let mut transfer = Box::pin(handle.transfer_leadership(NodeId(2)));
    let transferred = poll_once(&mut transfer).expect("the transfer resolves in place");
    assert!(
        transferred.is_err(),
        "the apply failed, so the transfer step failed"
    );

    let error = poll_once(&mut write)
        .expect("the drain ran at the transfer step, so the client is resolved")
        .expect_err("a poisoned group cannot produce a receipt");
    assert!(
        matches!(
            error,
            WriteError::UnknownOutcome {
                reason: UnknownOutcomeReason::GroupPoisoned,
                ..
            }
        ),
        "got {error:?}"
    );
    assert!(driver.pending_writes().is_empty());
}

/// The same fixture, at the observable that names the site: the group's
/// captured-waiter table is empty once the transfer has returned.
#[test]
fn the_leadership_transfer_step_drains_the_groups_poisoned_waiters() {
    let driver = transfer_poison_driver();
    let handle = driver.handle();

    let mut write = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    assert!(poll_once(&mut write).is_none());
    let mut transfer = Box::pin(handle.transfer_leadership(NodeId(2)));
    let _ = poll_once(&mut transfer).expect("the transfer resolves in place");

    assert!(
        driver
            .with_group(|group| group.poisoned_waiters().is_empty())
            .expect("the driver still holds its group"),
        "the drain runs after the leadership-transfer step on both paths"
    );
}

/// The supervisor reaction the entry documents. A later tick would also drain,
/// but the rescue is incidental: a supervisor that releases instead of ticking
/// used to be told the driver retired the incarnation, for a client whose group
/// had poisoned under it.
#[test]
fn releasing_after_a_transfer_poison_reports_the_poison() {
    let driver = transfer_poison_driver();
    let handle = driver.handle();

    let mut write = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    assert!(poll_once(&mut write).is_none());
    let mut transfer = Box::pin(handle.transfer_leadership(NodeId(2)));
    let _ = poll_once(&mut transfer).expect("the transfer resolves in place");

    let _group = driver.release_group().expect("the driver held its group");

    let error = poll_once(&mut write)
        .expect("the waiter holds an answer")
        .expect_err("a poisoned group cannot produce a receipt");
    assert!(
        matches!(
            error,
            WriteError::UnknownOutcome {
                reason: UnknownOutcomeReason::GroupPoisoned,
                ..
            }
        ),
        "the group poisoned during the transfer, so that is the fact the driver \
         observed first; got {error:?}"
    );
}

/// The success path through the same entry point, so routing the rejection is
/// not lost to the drain: a transfer to a target the group refuses still
/// resolves its own future with the refusal.
#[test]
fn a_rejected_transfer_still_resolves_its_own_future() {
    let (driver, _transport) = driver_over_app(1, &[2], KvStateMachine::default());
    let handle = driver.handle();

    // A follower cannot transfer leadership, which is a rejection the step
    // reports in its report rather than as a step failure.
    let mut transfer = Box::pin(handle.transfer_leadership(NodeId(2)));
    let outcome = poll_once(&mut transfer).expect("the transfer resolves in place");

    assert!(
        matches!(outcome, Err(TransferLeadershipError::Rejected { .. })),
        "got {outcome:?}"
    );
}
