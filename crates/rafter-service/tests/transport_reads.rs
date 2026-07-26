//! What the transport driver answers a read, at each consistency level it is
//! asked for.
//!
//! Separate from `transport_waiters.rs`, which is about the tables a read
//! leaves behind. Everything here is about the answer itself: which levels are
//! served, what bounds a served one, and what the two shipped drivers say when
//! handed the same request.

#![allow(clippy::wildcard_imports)]

mod support;

use rafter_service::{ReadOptions, TransportDriverOptions};
use support::transport::*;
use support::*;

/// A key this replica has applied, written through the leader and settled.
fn applied_cluster() -> std::collections::BTreeMap<NodeId, (Driver, QueueTransport)> {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let handle = nodes[&NodeId(1)].0.handle();
    let mut write = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    assert!(start(&mut write).is_none());
    settle(&nodes);
    let _ = poll_once(&mut write)
        .expect("the write resolves")
        .expect("the write commits and applies");
    nodes
}

/// The entry. A deployment asking its own replica what it holds gets an answer
/// through the read path, rather than having to borrow the group.
///
/// The follower is the interesting side: it is the replica whose staleness the
/// old refusal was written about, and it is the one a deployment asks when it
/// wants to know what *this* node has.
#[test]
fn a_local_read_answers_from_this_replica_without_a_barrier() {
    let nodes = applied_cluster();
    let (follower, _transport) = &nodes[&NodeId(2)];
    let reserved_before = follower
        .handle()
        .metrics()
        .expect("metrics")
        .current()
        .reserved_reads;

    let receipt = block_on(
        follower
            .handle()
            .read("alpha".to_owned(), ReadConsistency::Local),
    )
    .expect("a local read is answered from this replica's applied state");

    assert_eq!(receipt.result, Some("one".to_owned()));
    assert!(
        receipt.proof.is_none(),
        "a local read submits no read-index round, so the absent proof is the honest report"
    );
    assert_eq!(
        follower
            .handle()
            .metrics()
            .expect("metrics")
            .current()
            .reserved_reads,
        reserved_before,
        "no barrier was reserved"
    );
    assert!(
        follower.pending_reads().is_empty(),
        "and no waiter was registered, so there is nothing to abandon"
    );
}

/// The claim the old doc paragraph denied was possible: a local read says how
/// far behind it is, in both directions, when it cannot meet the caller's floor.
#[test]
fn a_local_read_below_its_floor_reports_both_indices() {
    let nodes = applied_cluster();
    let (follower, _transport) = &nodes[&NodeId(2)];
    let applied = follower
        .with_group(|group| {
            group
                .state_machine()
                .applied_index()
                .expect("this state machine always reports one")
        })
        .expect("the follower holds its group");

    let error = block_on(follower.handle().read_with_options(
        "alpha".to_owned(),
        ReadConsistency::Local,
        ReadOptions::default().with_min_applied_index(LogIndex(applied.0 + 5)),
    ))
    .expect_err("a floor this replica has not reached is not answered from behind it");

    assert!(
        matches!(
            error,
            ReadError::FreshnessUnavailable {
                read_id: None,
                required_applied_index,
                local_applied_index,
            } if required_applied_index == LogIndex(applied.0 + 5)
                && local_applied_index == applied
        ),
        "the refusal names both indices, which is what a caller acts on, got {error:?}"
    );
}

/// A read that registers no waiter cannot contribute to the condition
/// `max_pending_waiters` refuses for, so it is not refused by it.
#[test]
fn a_local_read_is_served_at_the_waiter_limit() {
    let nodes = cluster_with_options(
        &[1, 2],
        &[(
            1,
            TransportDriverOptions::default().with_max_pending_waiters(1),
        )]
        .into_iter()
        .collect(),
    );
    elect(&nodes, NodeId(1));
    let (leader, _transport) = &nodes[&NodeId(1)];
    let handle = leader.handle();

    // One unresolved barrier fills the driver's whole waiter budget.
    let mut barrier = Box::pin(handle.read("alpha".to_owned(), ReadConsistency::Linearizable));
    assert!(
        start(&mut barrier).is_none(),
        "the barrier is waiting on its quorum round"
    );
    let refused = block_on(handle.read("alpha".to_owned(), ReadConsistency::Linearizable))
        .expect_err("a second barrier is over the bound");
    assert!(
        matches!(refused, ReadError::Transport { .. }),
        "got {refused:?}"
    );

    let receipt = block_on(handle.read("alpha".to_owned(), ReadConsistency::Local))
        .expect("a local read holds no waiter and is not bounded by the waiter count");

    assert!(receipt.proof.is_none());
}

/// The guard the borrowed-group route did not have.
///
/// A local read goes through `RaftGroup::read`, which refuses a poisoned group
/// before it reads anything. `with_group` borrows the group without checking
/// poison at all — deliberately, so a test can ask a failed replica what it
/// durably holds — so the path this entry replaces would have answered here.
#[test]
fn a_local_read_is_refused_on_a_poisoned_group() {
    let (driver, _transport) = driver_over_app(1, &[], failing_apply());
    elect_single_voter(&driver);
    let handle = driver.handle();
    // A single voter commits and applies inside the step that proposes, so the
    // refused apply poisons the group in place.
    let _ = block_on(handle.write(("alpha".to_owned(), "one".to_owned())))
        .expect_err("the apply fails");

    let error = block_on(handle.read("alpha".to_owned(), ReadConsistency::Local))
        .expect_err("a poisoned group answers no read at any consistency level");

    assert!(matches!(error, ReadError::Poisoned { .. }), "got {error:?}");
}

/// The `NoGroup` check precedes the consistency branch: a local read has no
/// state machine to read either.
#[test]
fn a_local_read_is_refused_after_release() {
    let (driver, _transport) = driver_for(1, &[2]);
    let handle = driver.handle();
    let _ = driver.release_group().expect("the driver holds a group");

    let error = block_on(handle.read("alpha".to_owned(), ReadConsistency::Local))
        .expect_err("a released driver holds no state machine");

    assert!(
        matches!(error, ReadError::Transport { .. }),
        "got {error:?}"
    );
}

/// Boundary one. Widening to `Local` is a widening to `Local`.
///
/// `LeaseRead` stays refused because the app layer refuses it, which is why the
/// variant is hidden from generated docs rather than merely unimplemented here.
#[test]
fn a_lease_read_is_still_refused() {
    let (driver, _transport) = driver_for(1, &[2]);

    let error = block_on(
        driver
            .handle()
            .read("alpha".to_owned(), ReadConsistency::LeaseRead),
    )
    .expect_err("no shipped driver serves lease reads");

    assert!(
        matches!(
            error,
            ReadError::UnsupportedConsistency {
                consistency: ReadConsistency::LeaseRead
            }
        ),
        "got {error:?}"
    );
}

/// Boundary two, and the agreement this entry claims. The two drivers served
/// different sets of consistency levels, which was the defect; this is the
/// executable form of the claim that they no longer do.
///
/// Each replica is given a value of its own to find first, so the comparison is
/// of two answers rather than of two empty state machines: a driver that served
/// the level and returned nothing would pass a test that only compared `None` to
/// `None`.
#[test]
fn both_drivers_answer_a_local_read_the_same_way() {
    let in_memory = elected_driver();
    let in_memory_handle = in_memory.handle();
    block_on(in_memory_handle.write(("alpha".to_owned(), "one".to_owned())))
        .expect("the in-memory driver commits and applies in place");
    let in_memory_receipt =
        block_on(in_memory_handle.read("alpha".to_owned(), ReadConsistency::Local))
            .expect("the in-memory driver serves local reads");

    let (transport, _queue) = driver_for(1, &[]);
    elect_single_voter(&transport);
    let transport_handle = transport.handle();
    block_on(transport_handle.write(("alpha".to_owned(), "one".to_owned())))
        .expect("a single voter commits and applies inside the step that proposes");
    let transport_receipt =
        block_on(transport_handle.read("alpha".to_owned(), ReadConsistency::Local))
            .expect("and the transport driver serves them too");

    assert_eq!(
        in_memory_receipt.result,
        Some("one".to_owned()),
        "the fixture must have something to find, or the comparison is vacuous"
    );
    assert_eq!(in_memory_receipt.result, transport_receipt.result);
    assert!(in_memory_receipt.proof.is_none());
    assert!(
        transport_receipt.proof.is_none(),
        "neither submits a read-index round, so neither produces a proof"
    );
}
