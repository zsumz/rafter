//! Who a waiter belongs to, and what happens when nobody is listening.
//!
//! Three mechanisms have to compose here rather than race: a client that dropped
//! its future has its waiter reclaimed, a caller that abandoned a waiter whose
//! future is still held gets its answer on the next poll, and reclaiming never
//! waits for the driver's lock — because a future can be dropped by code the
//! driver is running under that lock, and a `Drop` that waited there would stop
//! the thread it ran on.

#![allow(clippy::wildcard_imports)]

mod support;

use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    sync::{mpsc, Arc, Barrier, Mutex},
    time::Duration,
};

use rafter::{PreVoteResponse, RequestVoteResponse};
use rafter_service::{
    AuthenticatedPeerEnvelope, PeerEnvelope, PeerSet, RaftTransport, ReadOptions,
    SnapshotChunkEnvelope, TransportDriverOptions, TransportRaftDriver, WriteOptions,
};
use support::transport::*;
use support::*;

/// How long a re-entrancy test waits before calling a hang a hang.
///
/// The tests below reproduce deadlocks. They run their fixture on a worker
/// thread and watch it from the test thread, so a regression reports which drop
/// never returned instead of hanging the whole suite.
const WATCHDOG: Duration = Duration::from_secs(5);

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

// ---------------------------------------------------------------------------
// Reclamation under the driver's own lock.
//
// A client future can be dropped by code the driver invoked while holding its
// lock, and the reclamation the drop performs must not ask for that lock back.
// `with_group` is the documented site; a transport call is the one an embedder
// reaches without reading `with_group`'s doc at all.
// ---------------------------------------------------------------------------

/// An elected two-voter leader whose follower never answers, so a write stays
/// appended-and-unacknowledged and a read stays reserved. Every future dropped
/// below is genuinely unresolved, which is the only state a guard has work in.
fn leader_with_a_silent_follower() -> Driver {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let (driver, _transport) = &nodes[&NodeId(1)];
    driver.clone()
}

/// Runs `body` on a worker thread and fails with `stuck` if it does not finish.
fn watched(stuck: &'static str, body: impl FnOnce() + Send + 'static) {
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        body();
        let _ = sender.send(());
    });
    receiver.recv_timeout(WATCHDOG).unwrap_or_else(|_| {
        panic!("{stuck}");
    });
}

/// `with_group` runs its closure with the driver locked and tells the caller
/// not to call back into the driver. Dropping a client future is not a call —
/// it needs no borrow and names no method — but the reclamation behind it used
/// to take the driver's lock, so a closure that dropped one stopped its thread.
#[test]
fn dropping_an_unresolved_write_future_inside_with_group_reclaims_it() {
    watched(
        "WaiterGuard::drop re-entered the driver lock that with_group holds",
        || {
            let driver = leader_with_a_silent_follower();
            let handle = driver.handle();
            let mut write = Box::pin(handle.write(("key".to_owned(), "value".to_owned())));
            assert!(
                poll_once(&mut write).is_none(),
                "the write is in flight and unresolved"
            );

            let still_pending = driver
                .with_group(move |group| {
                    drop(write);
                    // Observed from inside the lock: the reclamation was
                    // deferred rather than performed, which is what makes it
                    // non-blocking.
                    group.metrics().pending_proposals
                })
                .expect("the driver still holds its group");
            assert_eq!(still_pending, 1);

            assert!(
                driver.pending_writes().is_empty(),
                "the next lock acquisition took the deferred reclamation"
            );
        },
    );
}

/// The read side of the same re-entrancy, which has more to do: an unresolved
/// read's reclamation cancels its barrier through the group.
#[test]
fn dropping_an_unresolved_read_future_inside_with_group_reclaims_it() {
    watched("WaiterGuard::drop re-entered the driver lock", || {
        let driver = leader_with_a_silent_follower();
        let handle = driver.handle();
        let mut read = Box::pin(handle.read("key".to_owned(), ReadConsistency::Linearizable));
        assert!(
            poll_once(&mut read).is_none(),
            "the barrier needs a quorum round, so it is unresolved"
        );

        let reserved_inside = driver
            .with_group(move |group| {
                drop(read);
                group.metrics().reserved_reads
            })
            .expect("the driver still holds its group");
        assert_eq!(
            reserved_inside, 1,
            "the barrier is still reserved inside the closure: the drop deferred"
        );

        assert!(driver.pending_reads().is_empty());
        assert_eq!(
            driver
                .with_group(|group| group.metrics().reserved_reads)
                .expect("the driver still holds its group"),
            0,
            "the deferred reclamation gave the barrier back"
        );
    });
}

/// The deferral, asserted rather than inferred, and on both waiter kinds at
/// once: nothing is reclaimed while the lock is held, and everything is
/// reclaimed by the next acquisition.
#[test]
fn a_deferred_reclamation_is_taken_by_the_next_lock_acquisition() {
    watched("a deferred reclamation never ran", || {
        let driver = leader_with_a_silent_follower();
        let handle = driver.handle();
        let mut write = Box::pin(handle.write(("key".to_owned(), "value".to_owned())));
        assert!(poll_once(&mut write).is_none());
        let mut read = Box::pin(handle.read("key".to_owned(), ReadConsistency::Linearizable));
        assert!(poll_once(&mut read).is_none());

        let inside = driver
            .with_group(move |group| {
                drop(write);
                drop(read);
                (
                    group.metrics().pending_proposals,
                    group.metrics().reserved_reads,
                )
            })
            .expect("the driver still holds its group");
        assert_eq!(inside, (1, 1), "neither reclamation ran under the lock");

        // Any acquisition drains: this one is an ordinary public method.
        assert!(driver.pending_writes().is_empty());
        assert!(driver.pending_reads().is_empty());
        assert_eq!(
            driver
                .with_group(|group| group.metrics().reserved_reads)
                .expect("the driver still holds its group"),
            0
        );
    });
}

/// A transport whose `send` drops a client future somebody stashed in it.
///
/// This is the hazard away from `with_group`: the driver calls a transport
/// while routing a report, under its own lock, and an embedder's transport may
/// own a client future — one it kept to retry, or one belonging to a task it
/// resumes. Nothing warns it, because it never reads `with_group`'s contract.
type StashedWrite =
    Pin<Box<dyn Future<Output = Result<WriteReceipt<Option<String>>, WriteError>> + Send>>;

#[derive(Clone, Default)]
struct DropOnSendTransport {
    link: QueueTransport,
    stashed: Arc<Mutex<Option<StashedWrite>>>,
}

impl DropOnSendTransport {
    fn stash(&self, future: StashedWrite) {
        *self
            .stashed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(future);
    }
}

impl RaftTransport<u64> for DropOnSendTransport {
    type PeerPrincipal = Principal;
    type Error = TransportError;

    fn send(&self, envelope: PeerEnvelope<u64>) -> Result<(), Self::Error> {
        drop(
            self.stashed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take(),
        );
        self.link.send(envelope)
    }

    fn send_snapshot_chunk(&self, envelope: SnapshotChunkEnvelope<u64>) -> Result<(), Self::Error> {
        self.link.send_snapshot_chunk(envelope)
    }

    fn update_peers(
        &self,
        group_id: &u64,
        peers: PeerSet<Self::PeerPrincipal>,
    ) -> Result<(), Self::Error> {
        self.link.update_peers(group_id, peers)
    }

    fn fence_peer(&self, group_id: &u64, peer: Self::PeerPrincipal) -> Result<(), Self::Error> {
        self.link.fence_peer(group_id, peer)
    }
}

type DropOnSendDriver = TransportRaftDriver<
    u64,
    KvStateMachine,
    rafter_runtime::DurableRaftNode,
    DropOnSendTransport,
    Validator,
>;

/// Elects a lone replica by answering its own election frames with grants.
///
/// The support cluster cannot do this one: it is typed over `QueueTransport`,
/// and the whole point of this fixture is a different transport. The follower
/// exists only as a voter, so once the election is over it never answers again
/// and a write stays appended-and-unacknowledged — which is the state a live
/// waiter needs.
fn elect_by_granting_its_own_votes(driver: &DropOnSendDriver, link: &QueueTransport) {
    for _ in 0..32 {
        if driver.handle().metrics().expect("metrics").current().role == Role::Leader {
            return;
        }
        driver.tick().expect("a tick advances the protocol");
        for envelope in link.take_deliverable() {
            let granted = match envelope.message {
                Message::PreVote(vote) => Message::PreVoteResponse(PreVoteResponse {
                    term: vote.term,
                    voter_id: NodeId(2),
                    vote_granted: true,
                }),
                Message::RequestVote(vote) => Message::RequestVoteResponse(RequestVoteResponse {
                    term: vote.term,
                    voter_id: NodeId(2),
                    vote_granted: true,
                }),
                _ => continue,
            };
            driver
                .deliver(AuthenticatedPeerEnvelope {
                    group_id: GROUP,
                    authenticated_peer: Principal::for_node(NodeId(2)),
                    raft_from: NodeId(2),
                    raft_to: NodeId(1),
                    message: granted,
                })
                .expect("a grant from an authorized voter is accepted");
        }
    }
    panic!("the replica never took leadership within the tick budget");
}

/// The guarantee `with_group` now states, at a site no caller of `with_group`
/// is involved in: a transport that drops a client future while the driver is
/// routing a report reclaims it instead of stopping the tick.
#[test]
fn dropping_a_future_inside_a_transport_call_reclaims_it() {
    watched(
        "WaiterGuard::drop re-entered the driver lock from a send",
        || {
            let transport = DropOnSendTransport::default();
            let validator = Validator {
                transport: transport.link.clone(),
                authorized: BTreeSet::from([NodeId(2)]),
                nameable: None,
            };
            let driver: DropOnSendDriver = TransportRaftDriver::new(
                numbered_group(GROUP, 1, &[2], 3),
                Vec::new(),
                transport.clone(),
                validator,
                TransportDriverOptions::default(),
            )
            .expect("a quiescent group is adoptable");
            elect_by_granting_its_own_votes(&driver, &transport.link);

            let handle = driver.handle();
            // The handle moves into the future, so what the transport drops is the
            // whole client operation, exactly as an embedder's would be.
            let mut write: StashedWrite =
                Box::pin(async move { handle.write(("key".to_owned(), "v".to_owned())).await });
            assert!(
                poll_once(&mut write).is_none(),
                "the follower never acknowledges, so the waiter stays live"
            );
            assert_eq!(driver.pending_writes().len(), 1);
            transport.stash(write);

            // The tick's outbound frames reach `send`, which drops the future.
            driver.tick().expect("a tick advances the protocol");

            assert!(
                driver.pending_writes().is_empty(),
                "the transport's drop reclaimed the waiter"
            );
        },
    );
}

// ---------------------------------------------------------------------------
// Held under attack. These pass before and after the reclamation change; their
// job is to fail if it moved something it was not meant to move.
// ---------------------------------------------------------------------------

/// Abandon-then-drop and drop-then-abandon, in both orders.
#[test]
fn probe_abandon_and_drop_orderings() {
    {
        let driver = leader_with_a_silent_follower();
        let handle = driver.handle();
        let mut write = Box::pin(handle.write(("a".to_owned(), "1".to_owned())));
        assert!(poll_once(&mut write).is_none());
        let pending = driver.pending_writes();
        assert_eq!(pending.len(), 1);
        assert!(driver.abandon_write(pending[0].local_proposal_id));
        let answered = poll_once(&mut write).expect("the abandoned waiter answers");
        assert!(matches!(
            answered,
            Err(WriteError::UnknownOutcome {
                reason: UnknownOutcomeReason::DriveBoundReached,
                ..
            })
        ));
        drop(write);
        assert!(driver.pending_writes().is_empty());
    }
    {
        let driver = leader_with_a_silent_follower();
        let handle = driver.handle();
        let mut write = Box::pin(handle.write(("a".to_owned(), "1".to_owned())));
        assert!(poll_once(&mut write).is_none());
        let pending = driver.pending_writes();
        assert_eq!(pending.len(), 1);
        drop(write);
        assert!(
            !driver.abandon_write(pending[0].local_proposal_id),
            "a dropped future's waiter is gone, so nothing is retired"
        );
        assert!(driver.pending_writes().is_empty());
    }
}

/// A forgotten future never drops, so its waiter keeps its slot — and stays
/// abandonable, because the table entry is still there.
#[test]
fn probe_a_forgotten_future_keeps_its_slot_but_stays_abandonable() {
    let overrides = BTreeMap::from([(
        1,
        TransportDriverOptions::default().with_max_pending_waiters(1),
    )]);
    let nodes = cluster_with_options(&[1, 2], &overrides);
    elect(&nodes, NodeId(1));
    let (driver, _transport) = &nodes[&NodeId(1)];
    let handle = driver.handle();

    let mut first = Box::pin(handle.read("a".to_owned(), ReadConsistency::Linearizable));
    assert!(poll_once(&mut first).is_none());
    std::mem::forget(first);

    let mut second = Box::pin(handle.read("b".to_owned(), ReadConsistency::Linearizable));
    let refused = poll_once(&mut second).expect("the bound refuses synchronously");
    assert!(
        refused.is_err(),
        "the forgotten waiter still holds the slot: {refused:?}"
    );
    let pending = driver.pending_reads();
    assert_eq!(pending.len(), 1);
    assert!(
        driver.abandon_read(pending[0]),
        "the entry is still in the table, so abandonment still frees the slot"
    );
}

// ---------------------------------------------------------------------------
// Concurrency probes. The reclamation path is now two locks and a queue, so
// the orderings it can be driven through are worth hammering directly.
// ---------------------------------------------------------------------------

/// Sanity: the fixture really does hold unresolved waiters, so the probes below
/// are not vacuous.
#[test]
fn probe_fixture_holds_unresolved_waiters() {
    let driver = leader_with_a_silent_follower();
    let handle = driver.handle();

    let mut write = Box::pin(handle.write(("k".to_owned(), "v".to_owned())));
    assert!(poll_once(&mut write).is_none());
    let mut read = Box::pin(handle.read("k".to_owned(), ReadConsistency::Linearizable));
    assert!(poll_once(&mut read).is_none());

    assert_eq!(driver.pending_writes().len(), 1);
    assert_eq!(driver.pending_reads().len(), 1);
}

/// Clients starting and dropping unresolved futures while another thread drives
/// reads. Every drop races a lock it may or may not get, which is the branch
/// the deferral queue exists for.
#[test]
fn probe_dropping_unresolved_futures_races_read_driving() {
    let driver = Arc::new(leader_with_a_silent_follower());
    let stop = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(5));
    let unresolved_seen = Arc::new(AtomicUsize::new(0));

    let mut workers = Vec::new();
    for worker in 0..4_u64 {
        let driver = Arc::clone(&driver);
        let start = Arc::clone(&start);
        let unresolved_seen = Arc::clone(&unresolved_seen);
        workers.push(std::thread::spawn(move || {
            let handle = driver.handle();
            start.wait();
            for round in 0..300_u64 {
                let mut write = Box::pin(handle.write((format!("k{worker}"), round.to_string())));
                if round % 3 == 0 {
                    drop(write);
                } else {
                    if poll_once(&mut write).is_none() {
                        unresolved_seen.fetch_add(1, Ordering::Relaxed);
                    }
                    drop(write);
                }
                let mut read =
                    Box::pin(handle.read(format!("k{worker}"), ReadConsistency::Linearizable));
                if round % 2 == 0 && poll_once(&mut read).is_none() {
                    unresolved_seen.fetch_add(1, Ordering::Relaxed);
                }
                drop(read);
            }
        }));
    }

    let driver_thread = {
        let driver = Arc::clone(&driver);
        let stop = Arc::clone(&stop);
        let start = Arc::clone(&start);
        std::thread::spawn(move || {
            start.wait();
            while !stop.load(Ordering::Relaxed) {
                // Deliberately no `tick()`: ticking an unreachable leader demotes
                // it, after which every future resolves in place and the probe
                // stops exercising unresolved waiters. `tick` is raced below.
                let _ = driver.drive_pending_reads();
            }
        })
    };

    for worker in workers {
        worker.join().expect("no worker panicked");
    }
    stop.store(true, Ordering::Relaxed);
    driver_thread
        .join()
        .expect("the driving thread did not panic");

    let unresolved = unresolved_seen.load(Ordering::Relaxed);
    assert!(
        unresolved > 500,
        "the probe must actually exercise unresolved waiters, saw {unresolved}"
    );
    assert!(
        driver.pending_writes().is_empty(),
        "every write future was dropped, so no waiter may remain: {:?}",
        driver.pending_writes()
    );
    assert!(driver.pending_reads().is_empty());
    assert_eq!(
        driver
            .with_group(RaftGroup::metrics)
            .expect("the driver still holds its group")
            .reserved_reads,
        0,
        "a dropped read gives its barrier back"
    );
}

/// Dropping unresolved futures while another thread abandons the same waiters
/// out from under them.
#[test]
fn probe_abandonment_races_drops() {
    let driver = Arc::new(leader_with_a_silent_follower());
    let stop = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(3));

    let clients = {
        let driver = Arc::clone(&driver);
        let start = Arc::clone(&start);
        std::thread::spawn(move || {
            let handle = driver.handle();
            start.wait();
            for round in 0..600_u64 {
                let mut write = Box::pin(handle.write(("k".to_owned(), round.to_string())));
                let _ = poll_once(&mut write);
                let mut read = Box::pin(handle.read("k".to_owned(), ReadConsistency::Linearizable));
                let _ = poll_once(&mut read);
                drop(read);
                drop(write);
            }
        })
    };

    let abandoner = {
        let driver = Arc::clone(&driver);
        let stop = Arc::clone(&stop);
        let start = Arc::clone(&start);
        std::thread::spawn(move || {
            start.wait();
            while !stop.load(Ordering::Relaxed) {
                for pending in driver.pending_writes() {
                    let _ = driver.abandon_write(pending.local_proposal_id);
                }
                for read_id in driver.pending_reads() {
                    let _ = driver.abandon_read(read_id);
                }
            }
        })
    };

    start.wait();
    clients.join().expect("no client panicked");
    stop.store(true, Ordering::Relaxed);
    abandoner.join().expect("the abandoner did not panic");

    assert!(driver.pending_writes().is_empty());
    assert!(driver.pending_reads().is_empty());
}

/// Dropping unresolved futures while another thread releases and re-adopts the
/// group, so a deferred reclamation can be taken by an incarnation that did not
/// create it.
#[test]
fn probe_dropping_futures_races_release_and_readopt() {
    let driver = Arc::new(leader_with_a_silent_follower());
    let stop = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(3));

    let clients = {
        let driver = Arc::clone(&driver);
        let start = Arc::clone(&start);
        std::thread::spawn(move || {
            let handle = driver.handle();
            start.wait();
            for round in 0..400_u64 {
                let mut write = Box::pin(handle.write(("k".to_owned(), round.to_string())));
                let _ = poll_once(&mut write);
                let mut read = Box::pin(handle.read("k".to_owned(), ReadConsistency::Linearizable));
                let _ = poll_once(&mut read);
                drop(read);
                drop(write);
            }
        })
    };

    let churn = {
        let driver = Arc::clone(&driver);
        let stop = Arc::clone(&stop);
        let start = Arc::clone(&start);
        std::thread::spawn(move || {
            start.wait();
            while !stop.load(Ordering::Relaxed) {
                if let Ok(group) = driver.release_group() {
                    let _ = driver.adopt_group(group, Vec::new());
                }
            }
        })
    };

    start.wait();
    clients.join().expect("no client panicked");
    stop.store(true, Ordering::Relaxed);
    churn.join().expect("the churn thread did not panic");

    assert!(driver.pending_writes().is_empty());
    assert!(driver.pending_reads().is_empty());
}

/// The same drops, raced against `tick()`. No non-vacuity counter: ticking an
/// unreachable leader demotes it, so how many futures are unresolved at any
/// moment is scheduling-dependent. The invariant is what is asserted.
#[test]
fn probe_dropping_futures_races_ticks() {
    let driver = Arc::new(leader_with_a_silent_follower());
    let stop = Arc::new(AtomicBool::new(false));
    let start = Arc::new(Barrier::new(5));

    let mut workers = Vec::new();
    for worker in 0..4_u64 {
        let driver = Arc::clone(&driver);
        let start = Arc::clone(&start);
        workers.push(std::thread::spawn(move || {
            let handle = driver.handle();
            start.wait();
            for round in 0..300_u64 {
                let mut write = Box::pin(handle.write((format!("k{worker}"), round.to_string())));
                if round % 3 != 0 {
                    let _ = poll_once(&mut write);
                }
                drop(write);
                let mut read =
                    Box::pin(handle.read(format!("k{worker}"), ReadConsistency::Linearizable));
                if round % 2 == 0 {
                    let _ = poll_once(&mut read);
                }
                drop(read);
            }
        }));
    }

    let ticker = {
        let driver = Arc::clone(&driver);
        let stop = Arc::clone(&stop);
        let start = Arc::clone(&start);
        std::thread::spawn(move || {
            start.wait();
            while !stop.load(Ordering::Relaxed) {
                let _ = driver.tick();
                let _ = driver.drive_pending_reads();
            }
        })
    };

    for worker in workers {
        worker.join().expect("no worker panicked");
    }
    stop.store(true, Ordering::Relaxed);
    ticker.join().expect("the ticker did not panic");

    assert!(driver.pending_writes().is_empty());
    assert!(driver.pending_reads().is_empty());
    assert_eq!(
        driver
            .with_group(RaftGroup::metrics)
            .expect("the driver still holds its group")
            .reserved_reads,
        0
    );
}

// ---------------------------------------------------------------------------
// Addressed operations. `pending_writes` answers "what is this driver still
// holding"; these answer "which one is mine", and the difference is only
// visible once two operations are in flight at once.
// ---------------------------------------------------------------------------

/// The ID is known before the future is polled, which is the whole point: the
/// only thing it is for is abandoning, and abandoning is something a caller
/// does *while* waiting.
#[test]
fn a_begun_write_is_named_before_it_resolves() {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let (driver, _transport) = &nodes[&NodeId(1)];

    let (local_proposal_id, mut future) = driver
        .begin_write(
            ("alpha".to_owned(), "one".to_owned()),
            WriteOptions::default(),
        )
        .expect("the driver admits the write");

    assert_eq!(
        driver
            .pending_writes()
            .into_iter()
            .map(|write| write.local_proposal_id)
            .collect::<Vec<_>>(),
        vec![local_proposal_id],
        "the returned ID names the waiter the driver registered"
    );
    settle(&nodes);
    let receipt = poll_once(&mut future)
        .expect("the write resolves once its entry commits")
        .expect("the write commits and applies");
    assert_eq!(receipt.result, None);
}

/// End to end, with no `pending_writes` lookup anywhere: begin, abandon under
/// the name it returned, and hear the driver's own terminal vocabulary.
#[test]
fn a_begun_write_can_be_abandoned_under_its_own_name() {
    let driver = leader_with_a_silent_follower();

    let (local_proposal_id, mut future) = driver
        .begin_write(("a".to_owned(), "1".to_owned()), WriteOptions::default())
        .expect("the driver admits the write");
    assert!(poll_once(&mut future).is_none(), "the follower is silent");

    assert!(driver.abandon_write(local_proposal_id));

    let answered = poll_once(&mut future).expect("the abandoned waiter answers in place");
    assert!(
        matches!(
            answered,
            Err(WriteError::UnknownOutcome {
                reason: UnknownOutcomeReason::DriveBoundReached,
                ..
            })
        ),
        "got {answered:?}"
    );
}

/// The finding, as a test. Two writes in flight, and the *first* one abandoned:
/// `pending_writes().max()` names the second, so the helper both consumers
/// wrote would have retired the wrong waiter and left the caller's own write
/// waiting.
#[test]
fn two_concurrent_writes_are_each_named_correctly() {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let (driver, _transport) = &nodes[&NodeId(1)];

    let (first_id, mut first) = driver
        .begin_write(("a".to_owned(), "1".to_owned()), WriteOptions::default())
        .expect("the driver admits the first write");
    let (second_id, mut second) = driver
        .begin_write(("b".to_owned(), "2".to_owned()), WriteOptions::default())
        .expect("the driver admits the second write");
    assert_ne!(first_id, second_id);
    assert_eq!(
        driver
            .pending_writes()
            .into_iter()
            .map(|write| write.local_proposal_id)
            .max(),
        Some(second_id),
        "the highest unresolved ID is the *second* write, which is why reading it \
         to find the first one is wrong"
    );

    assert!(driver.abandon_write(first_id));

    let abandoned = poll_once(&mut first).expect("the abandoned waiter answers in place");
    assert!(
        matches!(
            abandoned,
            Err(WriteError::UnknownOutcome {
                reason: UnknownOutcomeReason::DriveBoundReached,
                ..
            })
        ),
        "got {abandoned:?}"
    );
    settle(&nodes);
    let receipt = poll_once(&mut second)
        .expect("the second write is untouched and resolves")
        .expect("it commits and applies");
    assert_eq!(receipt.result, None);
}

/// The read counterpart, including the barrier accounting an abandoned read
/// gives back.
#[test]
fn a_begun_read_is_named_before_it_resolves() {
    let driver = leader_with_a_silent_follower();
    let reserved_before = driver
        .with_group(|group| group.metrics().reserved_reads)
        .expect("the driver holds a group");

    let (read_id, mut future) = driver
        .begin_read("alpha".to_owned(), ReadOptions::default())
        .expect("the driver reserves a barrier");
    assert!(poll_once(&mut future).is_none(), "the round cannot finish");
    assert_eq!(driver.pending_reads(), vec![read_id]);

    assert!(driver.abandon_read(read_id));

    let answered = poll_once(&mut future).expect("the abandoned barrier answers in place");
    assert!(
        matches!(
            answered,
            Err(ReadError::Abandoned {
                reason: ReadAbandonReason::DriveBoundReached,
                ..
            })
        ),
        "got {answered:?}"
    );
    assert_eq!(
        driver
            .with_group(|group| group.metrics().reserved_reads)
            .expect("the driver holds a group"),
        reserved_before,
        "the barrier was cancelled through the group before the client resolved"
    );
}

/// A refusal that allocated no ID has no pair to return, so it is an `Err`
/// rather than a future carrying one.
#[test]
fn a_refused_write_returns_its_error_rather_than_a_future() {
    let overrides = BTreeMap::from([(
        1,
        TransportDriverOptions::default().with_max_pending_waiters(1),
    )]);
    let nodes = cluster_with_options(&[1, 2], &overrides);
    elect(&nodes, NodeId(1));
    let (driver, _transport) = &nodes[&NodeId(1)];

    let (_id, _future) = driver
        .begin_write(("a".to_owned(), "1".to_owned()), WriteOptions::default())
        .expect("the first write fits the bound");

    let error = driver
        .begin_write(("b".to_owned(), "2".to_owned()), WriteOptions::default())
        .err()
        .expect("the second is over the bound");

    assert!(
        matches!(error, WriteError::Transport { .. }),
        "got {error:?}"
    );
    assert_eq!(
        driver.pending_writes().len(),
        1,
        "a refused write registers no waiter"
    );
}

/// The executable form of "one body, two entry points": the addressed and the
/// plain write path are the same registration, so they answer the same.
#[test]
fn the_addressed_and_plain_write_paths_agree() {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let (driver, _transport) = &nodes[&NodeId(1)];
    let handle = driver.handle();

    let (_id, mut addressed) = driver
        .begin_write(
            ("k".to_owned(), "first".to_owned()),
            WriteOptions::default(),
        )
        .expect("the driver admits the addressed write");
    let mut plain = Box::pin(handle.write(("k".to_owned(), "second".to_owned())));
    assert!(start(&mut plain).is_none());
    settle(&nodes);

    let addressed = poll_once(&mut addressed)
        .expect("the addressed write resolves")
        .expect("it commits and applies");
    let plain = poll_once(&mut plain)
        .expect("the plain write resolves")
        .expect("it commits and applies");

    assert_eq!(addressed.result, None, "nothing was there before it");
    assert_eq!(
        plain.result,
        Some("first".to_owned()),
        "and the second one saw the first, so both took the same path in order"
    );
}
