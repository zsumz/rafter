//! Who a waiter belongs to, and what happens when nobody is listening.
//!
//! Two mechanisms have to compose here rather than race: a client that dropped
//! its future has its waiter reclaimed, and a caller that abandoned a waiter
//! whose future is still held gets its answer on the next poll.

#![allow(clippy::wildcard_imports)]

mod support;

use std::collections::BTreeMap;

use rafter_service::{ReadOptions, TransportDriverOptions};
use support::transport::*;
use support::*;

// ---------------------------------------------------------------------------
// Waiter reclamation, and the two shapes the composition has to keep apart:
// a client that stopped listening, and a caller that stopped waiting for one
// that is still listening.
// ---------------------------------------------------------------------------

/// The documented supervisor drain is a client timing out and dropping its
/// future. The review filled a bounded driver four times over that way and
/// found four resolved waiters nothing would ever poll, each holding its cloned
/// request, permanently. The future's own drop is now the reclamation.
#[test]
fn a_dropped_read_future_reclaims_its_waiter_and_its_barrier() {
    let overrides = BTreeMap::from([(
        1,
        TransportDriverOptions::default().with_max_pending_waiters(4),
    )]);
    let nodes = cluster_with_options(&[1, 2], &overrides);
    elect(&nodes, NodeId(1));
    let (driver, _transport) = &nodes[&NodeId(1)];
    let handle = driver.handle();
    let reserved_before = driver
        .with_group(|group| group.metrics().reserved_reads)
        .expect("the driver holds a group");

    for index in 0..4_u32 {
        let mut read =
            Box::pin(handle.read(format!("query-{index}"), ReadConsistency::Linearizable));
        assert!(
            poll_once(&mut read).is_none(),
            "a barrier awaiting its quorum round cannot resolve immediately"
        );
        drop(read); // the client timed out and dropped its future
    }

    assert!(
        driver.pending_reads().is_empty(),
        "nothing is holding a waiter for a client that left"
    );
    assert_eq!(
        driver
            .with_group(|group| group.metrics().reserved_reads)
            .expect("the driver holds a group"),
        reserved_before,
        "every dropped read gave its barrier back to the group"
    );

    // The full budget is available again, which is the observable consequence.
    let mut reads = (0..4_u32)
        .map(|index| Box::pin(handle.read(format!("again-{index}"), ReadConsistency::Linearizable)))
        .collect::<Vec<_>>();
    for read in &mut reads {
        assert!(poll_once(read).is_none(), "the freed slots admit four more");
    }
    assert_eq!(driver.pending_reads().len(), 4);
}

/// Abandonment resolves rather than removes, so a caller that abandons and
/// still holds its future gets the answer it asked to stop waiting for. That is
/// what makes the two mechanisms compose instead of racing.
#[test]
fn an_abandoned_waiter_still_answers_a_late_poll() {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let (driver, _transport) = &nodes[&NodeId(1)];
    let handle = driver.handle();

    let mut read = Box::pin(handle.read("a".to_owned(), ReadConsistency::Linearizable));
    assert!(poll_once(&mut read).is_none());
    let read_id = driver.pending_reads()[0];
    assert!(driver.abandon_read(read_id), "the waiter is retired");
    assert!(driver.pending_reads().is_empty());

    let mut write = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    assert!(poll_once(&mut write).is_none());
    let local_proposal_id = driver.pending_writes()[0].local_proposal_id;
    assert!(driver.abandon_write(local_proposal_id));

    // Polled after abandonment, from futures the caller kept.
    let read_outcome = poll_once(&mut read).expect("the abandoned read still answers");
    assert!(
        matches!(
            read_outcome,
            Err(ReadError::Abandoned {
                reason: ReadAbandonReason::DriveBoundReached,
                ..
            })
        ),
        "got {read_outcome:?}"
    );
    let write_outcome = poll_once(&mut write).expect("the abandoned write still answers");
    assert!(
        matches!(
            write_outcome,
            Err(WriteError::UnknownOutcome {
                reason: UnknownOutcomeReason::DriveBoundReached,
                ..
            })
        ),
        "got {write_outcome:?}"
    );
}

/// The one behavioural difference reclamation introduces, stated as a test:
/// abandonment resolves a client, and a client whose future was dropped is not
/// there to resolve.
#[test]
fn abandoning_a_waiter_whose_future_was_dropped_retires_nothing() {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let (driver, _transport) = &nodes[&NodeId(1)];
    let handle = driver.handle();

    let mut read = Box::pin(handle.read("a".to_owned(), ReadConsistency::Linearizable));
    assert!(poll_once(&mut read).is_none());
    let read_id = driver.pending_reads()[0];
    drop(read);

    assert!(
        !driver.abandon_read(read_id),
        "the dropped future already reclaimed its waiter"
    );
    assert!(driver.pending_reads().is_empty());
}

/// A caller that just observed a write can require a read at least that fresh.
/// The driver used to hardcode `min_applied_index: None`, which discarded the
/// request silently.
#[test]
fn a_read_floor_is_honored_verbatim() {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let (driver, _transport) = &nodes[&NodeId(1)];
    let handle = driver.handle();
    let mut write = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    assert!(start(&mut write).is_none());
    settle(&nodes);
    let receipt = poll_once(&mut write)
        .expect("the write resolves")
        .expect("the write commits and applies");

    // The floor a caller has: the index of the write it just observed. A read
    // at that floor answers, because this replica applied it.
    let mut at_floor = Box::pin(handle.read_with_options(
        "alpha".to_owned(),
        ReadConsistency::Linearizable,
        ReadOptions::default().with_min_applied_index(receipt.index),
    ));
    assert!(start(&mut at_floor).is_none());
    settle(&nodes);
    let answered = poll_once(&mut at_floor)
        .expect("the barrier resolves")
        .expect("a floor this replica has reached is satisfiable");
    assert_eq!(answered.result, Some("one".to_owned()));

    // A floor above anything this replica will ever apply is honored verbatim
    // rather than capped at the read index, so the read never answers. A driver
    // that discarded the floor would answer this one too.
    let mut above_floor = Box::pin(handle.read_with_options(
        "alpha".to_owned(),
        ReadConsistency::Linearizable,
        ReadOptions::default().with_min_applied_index(LogIndex(9_999)),
    ));
    assert!(start(&mut above_floor).is_none());
    settle(&nodes);
    assert!(
        poll_once(&mut above_floor).is_none(),
        "a floor the replica has not reached leaves the read waiting"
    );
    assert_eq!(driver.pending_reads().len(), 1);
}
