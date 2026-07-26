//! What the driver says about a write it could not finish, and about a group
//! that died holding one.
//!
//! Every scenario here comes from the adversarial review of
//! `TransportRaftDriver`, kept with its fixture and inverted where the review
//! recorded a defect.

#![allow(clippy::wildcard_imports)]

mod support;

use std::collections::BTreeMap;

use rafter_runtime::DurableRaftNode;
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
