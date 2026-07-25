//! One local replica, one attached transport, and the frames in between.
//!
//! The test transport is a queue with an explicit `take_deliverable`, so no
//! test here depends on timing: a frame moves when the test says so, and a cut
//! link is a real refusal rather than a silently skipped queue.

#![allow(clippy::wildcard_imports)]

mod support;

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    error::Error,
    fmt,
    sync::{Arc, Mutex},
};

use rafter_app::proposal::ClientRequestId;
use rafter_runtime::DurableRaftNode;
use rafter_service::{
    AuthenticatedPeerEnvelope, AuthenticatedPeerEnvelopeError, AuthenticatedPeerValidator,
    InboundEnvelopeError, PeerEnvelope, PeerSet, RaftTransport, TransportDriverOptions,
    TransportRaftDriver, WriteOptions,
};
use support::*;

const GROUP: u64 = 7;

/// Authenticated transport identity of one replica.
///
/// Deliberately not a `NodeId`: the transport authenticates a principal, and
/// the validator decides which Raft replica that principal is allowed to be.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Principal(String);

impl Principal {
    fn for_node(node_id: NodeId) -> Self {
        Self(format!("replica-{}", node_id.0))
    }
}

#[derive(Debug)]
struct TransportError(&'static str);

impl fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for TransportError {}

#[derive(Default)]
struct QueueState {
    sent: VecDeque<PeerEnvelope<u64>>,
    observed: Vec<PeerEnvelope<u64>>,
    cut: bool,
    fenced: BTreeSet<NodeId>,
}

/// A transport that holds every frame until a test asks for it.
#[derive(Clone, Default)]
struct QueueTransport {
    inner: Arc<Mutex<QueueState>>,
}

impl QueueTransport {
    fn lock(&self) -> std::sync::MutexGuard<'_, QueueState> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Returns every frame the transport accepted since the last call.
    fn take_deliverable(&self) -> Vec<PeerEnvelope<u64>> {
        self.lock().sent.drain(..).collect()
    }

    /// Every frame the transport was ever handed, refused or not.
    fn observed(&self) -> Vec<PeerEnvelope<u64>> {
        self.lock().observed.clone()
    }

    fn cut(&self) {
        self.lock().cut = true;
    }

    fn is_fenced(&self, node_id: NodeId) -> bool {
        self.lock().fenced.contains(&node_id)
    }
}

impl RaftTransport<u64> for QueueTransport {
    type PeerPrincipal = Principal;
    type Error = TransportError;

    fn send(&self, envelope: PeerEnvelope<u64>) -> Result<(), Self::Error> {
        let mut state = self.lock();
        state.observed.push(envelope.clone());
        if state.cut {
            return Err(TransportError("link is cut"));
        }
        state.sent.push_back(envelope);
        Ok(())
    }

    fn update_peers(
        &self,
        _group_id: &u64,
        _peers: PeerSet<Self::PeerPrincipal>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn fence_peer(&self, _group_id: &u64, peer: Self::PeerPrincipal) -> Result<(), Self::Error> {
        let node_id = principal_node(&peer).ok_or(TransportError("unknown principal"))?;
        self.lock().fenced.insert(node_id);
        Ok(())
    }
}

fn principal_node(principal: &Principal) -> Option<NodeId> {
    principal
        .0
        .strip_prefix("replica-")?
        .parse()
        .ok()
        .map(NodeId)
}

/// Maps authenticated principals to replicas, and consults the transport for
/// fencing so a test can fence through the documented API.
#[derive(Clone)]
struct Validator {
    transport: QueueTransport,
    authorized: BTreeSet<NodeId>,
}

impl AuthenticatedPeerValidator<u64, Principal> for Validator {
    fn is_known_group(&self, group_id: &u64) -> bool {
        *group_id == GROUP
    }

    fn node_for_authenticated_peer(&self, _group_id: &u64, peer: &Principal) -> Option<NodeId> {
        principal_node(peer)
    }

    fn is_authorized_peer(&self, _group_id: &u64, node_id: NodeId) -> bool {
        self.authorized.contains(&node_id)
    }

    fn is_fenced_peer(&self, _group_id: &u64, node_id: NodeId) -> bool {
        self.transport.is_fenced(node_id)
    }
}

type Driver = TransportRaftDriver<u64, KvStateMachine, DurableRaftNode, QueueTransport, Validator>;

fn driver_for(node_id: u64, peers: &[u64]) -> (Driver, QueueTransport) {
    driver_with_options(node_id, peers, TransportDriverOptions::default())
}

fn driver_with_options(
    node_id: u64,
    peers: &[u64],
    options: TransportDriverOptions,
) -> (Driver, QueueTransport) {
    let transport = QueueTransport::default();
    let validator = Validator {
        transport: transport.clone(),
        authorized: peers.iter().copied().map(NodeId).collect(),
    };
    let driver = TransportRaftDriver::new(
        numbered_group(GROUP, node_id, peers, 3),
        Vec::new(),
        transport.clone(),
        validator,
        options,
    )
    .expect("a quiescent group is adoptable");
    (driver, transport)
}

/// Ticks past the election timeout this fixture configures.
fn tick_past_election_timeout(driver: &Driver) {
    for _ in 0..4 {
        driver.tick().expect("a tick advances the protocol");
    }
}

/// Starts a client future without waiting for it.
///
/// `RaftHandle` methods are `async fn`s, so the driver's `write`/`read` — and
/// with them the waiter registration — do not run until the returned future is
/// polled once. Every test here starts its operation explicitly, then asserts
/// on what a later `tick` or `deliver` did with it.
fn start<F: std::future::Future + Unpin>(future: &mut F) -> Option<F::Output> {
    poll_once(future)
}

/// Moves every frame the transports accepted to the driver it is addressed to,
/// then collects any barrier proofs the deliveries granted.
fn exchange(nodes: &BTreeMap<NodeId, (Driver, QueueTransport)>) -> usize {
    let mut delivered = 0;
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
        driver
            .deliver(authenticated)
            .expect("the frame is accepted");
        delivered += 1;
    }
    for (driver, _) in nodes.values() {
        driver
            .drive_pending_reads()
            .expect("collecting granted proofs never fails here");
    }
    delivered
}

fn cluster(node_ids: &[u64]) -> BTreeMap<NodeId, (Driver, QueueTransport)> {
    cluster_with_options(node_ids, &BTreeMap::new())
}

fn cluster_with_options(
    node_ids: &[u64],
    overrides: &BTreeMap<u64, TransportDriverOptions>,
) -> BTreeMap<NodeId, (Driver, QueueTransport)> {
    node_ids
        .iter()
        .map(|node_id| {
            let peers = node_ids
                .iter()
                .copied()
                .filter(|peer| peer != node_id)
                .collect::<Vec<_>>();
            let options = overrides.get(node_id).copied().unwrap_or_default();
            (
                NodeId(*node_id),
                driver_with_options(*node_id, &peers, options),
            )
        })
        .collect()
}

/// Ticks and exchanges until the primary is leader.
fn elect(nodes: &BTreeMap<NodeId, (Driver, QueueTransport)>, primary: NodeId) {
    let (driver, _) = nodes.get(&primary).expect("the primary is in the cluster");
    for _ in 0..64 {
        if driver.handle().metrics().expect("metrics").current().role == Role::Leader {
            return;
        }
        driver.tick().expect("a tick advances the protocol");
        for _ in 0..8 {
            if exchange(nodes) == 0 {
                break;
            }
        }
    }
    panic!("the primary did not become leader within the tick budget");
}

/// Drives every node until nothing is left in flight.
fn settle(nodes: &BTreeMap<NodeId, (Driver, QueueTransport)>) {
    for _ in 0..64 {
        if exchange(nodes) == 0 {
            for (driver, _) in nodes.values() {
                driver.tick().expect("a tick advances the protocol");
            }
            if exchange(nodes) == 0 {
                return;
            }
        }
    }
}

/// The base case the crate has never had: two drivers, frames moved by the
/// test, one write committing and applying.
#[test]
fn a_write_completes_through_two_drivers_over_a_transport() {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let handle = nodes[&NodeId(1)].0.handle();

    let mut write = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    assert!(
        start(&mut write).is_none(),
        "the write cannot complete before its entry replicates"
    );
    settle(&nodes);

    let receipt = poll_once(&mut write)
        .expect("the write resolves once its entry commits")
        .expect("the write commits and applies");
    assert_eq!(receipt.result, None);
}

/// The structural claim of the entry: outbound frames reach the attached
/// transport rather than a private queue nothing can see.
#[test]
fn an_outbound_frame_reaches_the_transport_rather_than_a_private_queue() {
    let (driver, transport) = driver_for(1, &[2]);
    assert!(transport.observed().is_empty());

    tick_past_election_timeout(&driver);

    let observed = transport.observed();
    assert!(
        !observed.is_empty(),
        "the tick's peer messages must reach the transport"
    );
    assert!(
        observed
            .iter()
            .all(|envelope| envelope.group_id == GROUP && envelope.from == NodeId(1)),
        "every frame is this replica's own, got {observed:?}"
    );
}

/// The waiter property: a write registered on one call completes on a later
/// `tick`, which is a call it has no other relationship to.
#[test]
fn a_client_future_resolves_inside_a_tick_it_did_not_start() {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let handle = nodes[&NodeId(1)].0.handle();

    let mut write = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    assert!(start(&mut write).is_none());

    settle(&nodes);

    assert!(
        poll_once(&mut write).is_some(),
        "a later tick resolved a waiter it did not create"
    );
}

/// A granted barrier is consumed by a later read call rather than announced by
/// an event, so the third entry point exists to collect it.
#[test]
fn a_read_barrier_resolves_through_drive_pending_reads() {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let handle = nodes[&NodeId(1)].0.handle();
    let mut write = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    assert!(start(&mut write).is_none());
    settle(&nodes);
    let _ = poll_once(&mut write)
        .expect("the write resolves")
        .expect("the write commits and applies");

    let mut read = Box::pin(handle.read("alpha".to_owned(), ReadConsistency::Linearizable));
    assert!(
        start(&mut read).is_none(),
        "a granted barrier is collected by a later call, not announced by an event"
    );
    settle(&nodes);

    let receipt = poll_once(&mut read)
        .expect("the barrier resolves once the round completes")
        .expect("the read answers");
    assert_eq!(receipt.result, Some("one".to_owned()));
}

/// A barrier ended by a step that was not a read call still belongs to a
/// client.
///
/// The app layer ends a barrier in whichever step observes the cause, which for
/// a leadership change is a tick or a delivery. A driver that only reads its own
/// read calls' outcomes never hears it: the group drops the barrier's state, the
/// client waits forever, and the driver's next retry asks the group to
/// re-reserve a spent `ReadId`.
#[test]
fn a_barrier_canceled_during_a_delivery_resolves_its_client() {
    let nodes = cluster(&[1, 2, 3]);
    elect(&nodes, NodeId(1));
    let (driver, _transport) = &nodes[&NodeId(1)];
    let handle = driver.handle();

    // Nothing is exchanged after this, so the quorum round cannot finish on its
    // own and the barrier is still reserved when the delivery below lands.
    let mut read = Box::pin(handle.read("alpha".to_owned(), ReadConsistency::Linearizable));
    assert!(
        start(&mut read).is_none(),
        "the barrier is waiting on its quorum round"
    );

    // A higher term on an inbound frame makes the leader step down, and the
    // delivery step that observes it cancels every barrier the leader held.
    driver
        .deliver(vote_envelope(NodeId(2), NodeId(1)))
        .expect("an authorized peer's frame is accepted");

    let error = poll_once(&mut read)
        .expect("the step that ended the barrier resolved its client")
        .expect_err("a canceled barrier carries no answer");
    assert!(
        matches!(error, ReadError::Canceled { .. }),
        "the cluster invalidated the barrier, so the client hears that, got {error:?}"
    );
    driver
        .drive_pending_reads()
        .expect("a resolved barrier leaves nothing to re-reserve");
}

#[test]
fn release_returns_the_group_the_driver_was_built_with() {
    let (driver, _transport) = driver_for(1, &[2]);

    let group = driver.release_group().expect("the driver holds a group");

    assert_eq!(*group.group_id(), GROUP);
    assert_eq!(group.node_id(), NodeId(1));
}

/// A released driver refuses; it does not panic. The typed empty state is the
/// difference between this slot and an `Option` with expecting accessors.
#[test]
fn a_released_driver_refuses_every_operation() {
    let (driver, _transport) = driver_for(1, &[2]);
    let handle = driver.handle();
    let _ = driver.release_group().expect("the driver holds a group");

    assert!(matches!(driver.tick(), Err(ManagedDriverError::NoGroup)));
    assert!(matches!(
        driver.drive_pending_reads(),
        Err(ManagedDriverError::NoGroup)
    ));
    assert!(matches!(
        driver.release_group().map(|_| ()),
        Err(ManagedDriverError::NoGroup)
    ));

    let write = block_on(handle.write(("alpha".to_owned(), "one".to_owned())))
        .expect_err("a released driver serves no writes");
    assert!(
        matches!(
            write,
            WriteError::UnknownOutcome {
                reason: UnknownOutcomeReason::DriverReleased,
                ..
            }
        ),
        "got {write:?}"
    );
}

/// The recovery report carries peer messages a restart depends on, which is why
/// the signature takes outputs rather than an already-applied group.
#[test]
fn adopt_routes_the_recovery_outputs_it_was_given() {
    let (driver, transport) = driver_for(1, &[2]);
    let group = driver.release_group().expect("the driver holds a group");
    let _ = transport.take_deliverable();
    let before = transport.observed().len();

    driver
        .adopt_group(group, Vec::new())
        .expect("an empty recovery installs");

    assert_eq!(transport.observed().len(), before);
    tick_past_election_timeout(&driver);
    assert!(
        transport.observed().len() > before,
        "the re-adopted incarnation reaches the same transport"
    );
}

/// A handle names a service rather than a node incarnation.
#[test]
fn a_handle_survives_release_and_re_adoption() {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let handle = nodes[&NodeId(1)].0.handle();

    let group = nodes[&NodeId(1)]
        .0
        .release_group()
        .expect("the driver holds a group");
    nodes[&NodeId(1)]
        .0
        .adopt_group(group, Vec::new())
        .expect("the same group is re-adopted");

    let mut write = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    assert!(start(&mut write).is_none());
    settle(&nodes);
    let receipt = poll_once(&mut write)
        .expect("the same handle writes against the new incarnation")
        .expect("the write commits and applies");
    assert_eq!(receipt.result, None);
}

/// Negative: validation that happens after the step is not validation, so the
/// group's metrics must be untouched by a refused frame.
#[test]
fn an_unauthorized_peer_is_refused_before_the_group_is_stepped() {
    let (driver, _transport) = driver_for(1, &[2]);
    let before = driver.handle().metrics().expect("metrics").current();

    let error = driver
        .deliver(vote_envelope(NodeId(9), NodeId(1)))
        .expect_err("node 9 is not an authorized peer");

    assert!(
        matches!(
            error,
            InboundEnvelopeError::Rejected {
                source: AuthenticatedPeerEnvelopeError::UnauthorizedPeer { node_id: NodeId(9) },
            }
        ),
        "got {error:?}"
    );
    assert_eq!(
        driver.handle().metrics().expect("metrics").current(),
        before
    );
}

/// Negative: the same frame accepted, then fenced through the documented
/// transport API, then refused.
#[test]
fn a_fenced_peer_is_refused_after_fencing() {
    let (driver, transport) = driver_for(1, &[2]);
    driver
        .deliver(vote_envelope(NodeId(2), NodeId(1)))
        .expect("an authorized peer is accepted");

    transport
        .fence_peer(&GROUP, Principal::for_node(NodeId(2)))
        .expect("fencing a known principal succeeds");

    let error = driver
        .deliver(vote_envelope(NodeId(2), NodeId(1)))
        .expect_err("a fenced peer is refused");
    assert!(
        matches!(
            error,
            InboundEnvelopeError::Rejected {
                source: AuthenticatedPeerEnvelopeError::FencedPeer { node_id: NodeId(2) },
            }
        ),
        "got {error:?}"
    );
}

/// Negative: a many-group host demultiplexes by group before calling `deliver`,
/// and a driver that serves one group refuses every other.
#[test]
fn a_frame_for_another_group_is_refused() {
    let (driver, _transport) = driver_for(1, &[2]);
    let mut envelope = vote_envelope(NodeId(2), NodeId(1));
    envelope.group_id = GROUP + 1;

    let error = driver
        .deliver(envelope)
        .expect_err("this driver serves one group");

    assert!(
        matches!(
            error,
            InboundEnvelopeError::Rejected {
                source: AuthenticatedPeerEnvelopeError::UnknownGroup,
            }
        ),
        "got {error:?}"
    );
}

/// Negative: a driver that propagated transport refusals would fail writes on
/// every heartbeat drop. Raft tolerates drops and re-sends, so the driver
/// counts them instead.
#[test]
fn a_refused_send_does_not_fail_the_write_that_produced_it() {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let (driver, transport) = &nodes[&NodeId(1)];
    let handle = driver.handle();
    let refused_before = driver.refused_sends();
    transport.cut();

    let mut write = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    assert!(start(&mut write).is_none());
    driver
        .tick()
        .expect("a tick still succeeds with a cut link");

    assert!(
        driver.refused_sends() > refused_before,
        "a cut link is visible as a refusal count, not as a failed write"
    );
    assert!(
        poll_once(&mut write).is_none(),
        "the write proceeds toward its own outcome rather than failing on a drop"
    );
}

/// Negative: filling to `max_pending_waiters` must refuse the next write rather
/// than grow, and the refusal is observed, so its identity is still unused.
#[test]
fn waiters_are_bounded() {
    let overrides = BTreeMap::from([(
        1,
        TransportDriverOptions::default().with_max_pending_waiters(1),
    )]);
    let nodes = cluster_with_options(&[1, 2], &overrides);
    elect(&nodes, NodeId(1));
    let handle = nodes[&NodeId(1)].0.handle();

    let mut first = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    assert!(start(&mut first).is_none(), "the first write is pending");

    let refused = block_on(handle.write(("beta".to_owned(), "two".to_owned())))
        .expect_err("the second write exceeds the waiter bound");

    assert_eq!(refused.kind(), WriteErrorKind::Transport);
    assert_eq!(
        refused.fate(),
        WriteFate::NotAppended,
        "nothing was proposed, so the request identity is still unused"
    );
}

/// Negative: `release_group` promises every outstanding waiter resolves before
/// it returns, and that the retired group is quiescent.
#[test]
fn release_resolves_outstanding_waiters_before_returning() {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let (driver, _transport) = &nodes[&NodeId(1)];
    let handle = driver.handle();
    let mut write = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    assert!(start(&mut write).is_none());

    let group = driver.release_group().expect("the driver holds a group");

    let error = poll_once(&mut write)
        .expect("the waiter resolved before release returned")
        .expect_err("a released write has no receipt");
    assert!(
        matches!(
            error,
            WriteError::UnknownOutcome {
                reason: UnknownOutcomeReason::DriverReleased,
                ..
            }
        ),
        "got {error:?}"
    );
    // The group still tracks the proposal, and that is the point: the entry is
    // in the durable log and may commit under the next incarnation, which is
    // what makes the client's outcome unknown rather than refused. Only the
    // reads are cancelled, because a read takes no effect.
    assert_eq!(group.metrics().reserved_reads, 0);
}

/// The misattribution this vocabulary removes: `RuntimeDroppedProposal` means
/// the app or runtime layer declared local tracking lost while the driver kept
/// running, and points an operator at the wrong layer after a restart.
#[test]
fn releasing_a_group_resolves_outstanding_writes_as_driver_released() {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let (driver, _transport) = &nodes[&NodeId(1)];
    let handle = driver.handle();
    let mut write = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    assert!(start(&mut write).is_none());

    let _ = driver.release_group().expect("the driver holds a group");

    let error = poll_once(&mut write)
        .expect("the waiter resolved")
        .expect_err("a released write has no receipt");
    let WriteError::UnknownOutcome { reason, .. } = error else {
        panic!("expected an unknown outcome, got {error:?}");
    };
    assert_eq!(reason, UnknownOutcomeReason::DriverReleased);
    assert_ne!(reason, UnknownOutcomeReason::RuntimeDroppedProposal);
}

#[test]
fn releasing_a_group_abandons_its_outstanding_reads() {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let (driver, _transport) = &nodes[&NodeId(1)];
    let handle = driver.handle();
    let mut read = Box::pin(handle.read("alpha".to_owned(), ReadConsistency::Linearizable));
    assert!(start(&mut read).is_none(), "the barrier is in flight");

    let group = driver.release_group().expect("the driver holds a group");

    let error = poll_once(&mut read)
        .expect("the waiter resolved before release returned")
        .expect_err("an abandoned read has no answer");
    assert!(
        matches!(
            error,
            ReadError::Abandoned {
                reason: ReadAbandonReason::DriverReleased,
                ..
            }
        ),
        "got {error:?}"
    );
    assert_eq!(
        group.metrics().reserved_reads,
        0,
        "the barrier was cancelled through the group before the error was returned"
    );
}

/// This is what makes `DriverReleased` an *unknown* outcome rather than a
/// failure: the entry is in the durable log, and the next incarnation over the
/// same storage can still commit and apply it.
#[test]
fn a_write_released_after_local_append_may_still_apply_under_the_next_incarnation() {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let (driver, _transport) = &nodes[&NodeId(1)];
    let handle = driver.handle();
    let mut write = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    assert!(start(&mut write).is_none());

    let group = driver.release_group().expect("the driver holds a group");
    let applied_before = group.metrics().applied_index;
    let error = poll_once(&mut write)
        .expect("the waiter resolved")
        .expect_err("the outcome is unknown, not failed");
    assert!(error.fate().may_commit());

    // The same durable group comes back and the entry it already held commits.
    driver
        .adopt_group(group, Vec::new())
        .expect("the retired group is re-adoptable");
    settle(&nodes);

    let applied_after = nodes[&NodeId(1)]
        .0
        .handle()
        .metrics()
        .expect("metrics")
        .current()
        .applied_index;
    assert!(
        applied_after > applied_before,
        "the appended entry committed under the next incarnation"
    );
}

/// Observation is what `release_group` cannot offer: it hands the group back by
/// resolving every outstanding waiter, so it retires a replica rather than
/// looking at one.
#[test]
fn with_group_reads_a_running_replica_without_releasing_it() {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let (driver, _transport) = &nodes[&NodeId(1)];
    let handle = driver.handle();
    let mut write = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    assert!(start(&mut write).is_none());
    settle(&nodes);
    let _ = poll_once(&mut write)
        .expect("the write resolves")
        .expect("the write commits and applies");

    let value = driver
        .with_group(|group| group.state_machine().values.get("alpha").cloned())
        .expect("a running driver holds its group");
    assert_eq!(value, Some("one".to_owned()));

    let applied = driver
        .with_group(|group| group.metrics().applied_index)
        .expect("a running driver holds its group");
    assert!(
        applied
            >= driver
                .committed_application_index()
                .expect("the group is here"),
        "this replica has applied everything it knows to be committed"
    );

    // The replica is still running, which is the whole point of the surface.
    let mut second = Box::pin(handle.write(("beta".to_owned(), "two".to_owned())));
    assert!(start(&mut second).is_none());
    settle(&nodes);
    let _ = poll_once(&mut second)
        .expect("the driver kept serving")
        .expect("the second write commits and applies");
}

#[test]
fn with_group_refuses_after_release() {
    let (driver, _transport) = driver_for(1, &[2]);
    let _ = driver.release_group().expect("the driver holds a group");

    assert!(matches!(
        driver.with_group(RaftGroup::node_id),
        Err(ManagedDriverError::NoGroup)
    ));
    assert!(matches!(
        driver.committed_application_index(),
        Err(ManagedDriverError::NoGroup)
    ));
}

/// A caller that stops waiting says so, and gets its slot back without waiting
/// for the client to poll.
#[test]
fn abandoning_a_write_resolves_its_client_and_frees_its_slot() {
    let overrides = BTreeMap::from([(
        1,
        TransportDriverOptions::default().with_max_pending_waiters(1),
    )]);
    let nodes = cluster_with_options(&[1, 2], &overrides);
    elect(&nodes, NodeId(1));
    let (driver, _transport) = &nodes[&NodeId(1)];
    let handle = driver.handle();

    let mut write = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    assert!(start(&mut write).is_none(), "the first write is pending");
    let pending = driver.pending_writes();
    assert_eq!(pending.len(), 1, "the driver names what it is holding");

    assert!(driver.abandon_write(pending[0].local_proposal_id));
    assert!(
        !driver.abandon_write(pending[0].local_proposal_id),
        "abandoning twice is a no-op rather than a fault"
    );

    let error = poll_once(&mut write)
        .expect("the abandoned client has an answer")
        .expect_err("an abandoned write has no receipt");
    assert!(
        matches!(
            error,
            WriteError::UnknownOutcome {
                reason: UnknownOutcomeReason::DriveBoundReached,
                ..
            }
        ),
        "got {error:?}"
    );
    assert!(
        error.fate().may_commit(),
        "an appended entry may still commit, which is what unknown means"
    );

    // The slot came back, so the bound admits the next write.
    let mut second = Box::pin(handle.write(("beta".to_owned(), "two".to_owned())));
    assert!(
        start(&mut second).is_none(),
        "the abandoned waiter stopped counting against the bound"
    );
}

/// Late events for an abandoned ID are harmless, and the direction is that the
/// first outcome wins: the client already holds a terminal answer.
#[test]
fn a_late_proposal_event_does_not_overwrite_an_abandoned_write() {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let (driver, _transport) = &nodes[&NodeId(1)];
    let handle = driver.handle();

    let mut write = Box::pin(handle.write(("alpha".to_owned(), "one".to_owned())));
    assert!(start(&mut write).is_none());
    let local_proposal_id = driver.pending_writes()[0].local_proposal_id;
    assert!(driver.abandon_write(local_proposal_id));

    // The proposal the caller walked away from goes on to commit and apply.
    settle(&nodes);

    let error = poll_once(&mut write)
        .expect("the client still holds what abandonment gave it")
        .expect_err("the later apply did not overwrite it");
    assert!(
        matches!(
            error,
            WriteError::UnknownOutcome {
                reason: UnknownOutcomeReason::DriveBoundReached,
                ..
            }
        ),
        "got {error:?}"
    );
    let applied = driver
        .with_group(|group| group.state_machine().values.get("alpha").cloned())
        .expect("the driver still holds its group");
    assert_eq!(
        applied,
        Some("one".to_owned()),
        "unknown means it may still commit, and here it did"
    );
    assert!(
        driver.pending_writes().is_empty(),
        "nothing is left unresolved"
    );
}

/// The barrier is always cancelled first, so `reserved_reads` returns to its
/// previous value rather than leaking a reservation.
#[test]
fn abandoning_a_read_cancels_its_barrier() {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let (driver, _transport) = &nodes[&NodeId(1)];
    let handle = driver.handle();
    let reserved_before = driver
        .with_group(|group| group.metrics().reserved_reads)
        .expect("the driver holds its group");

    let mut read = Box::pin(handle.read("alpha".to_owned(), ReadConsistency::Linearizable));
    assert!(start(&mut read).is_none(), "the barrier is in flight");
    let read_ids = driver.pending_reads();
    assert_eq!(read_ids.len(), 1);

    assert!(driver.abandon_read(read_ids[0]));

    let error = poll_once(&mut read)
        .expect("the abandoned client has an answer")
        .expect_err("an abandoned read has no answer");
    assert!(
        matches!(
            error,
            ReadError::Abandoned {
                reason: ReadAbandonReason::DriveBoundReached,
                ..
            }
        ),
        "got {error:?}"
    );
    assert_eq!(
        driver
            .with_group(|group| group.metrics().reserved_reads)
            .expect("the driver holds its group"),
        reserved_before,
        "the barrier was cancelled through the group before the error was returned"
    );
    driver
        .drive_pending_reads()
        .expect("an abandoned barrier is not retried");
}

/// A caller with several writes in flight tells them apart by the ID it
/// supplied, which is why `PendingWrite` carries both.
#[test]
fn a_pending_write_is_addressable_before_it_resolves() {
    let nodes = cluster(&[1, 2]);
    elect(&nodes, NodeId(1));
    let (driver, _transport) = &nodes[&NodeId(1)];
    let handle = driver.handle();

    let options = WriteOptions {
        client_request_id: Some(ClientRequestId {
            client_id: 7,
            sequence: 3,
        }),
    };
    let mut write =
        Box::pin(handle.write_with_options(("alpha".to_owned(), "one".to_owned()), options));
    assert!(start(&mut write).is_none());

    let pending = driver.pending_writes();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].client_request_id, options.client_request_id);

    settle(&nodes);
    let _ = poll_once(&mut write)
        .expect("the write resolves")
        .expect("the write commits and applies");
    assert!(
        driver.pending_writes().is_empty(),
        "a resolved write is no longer pending"
    );
}

/// The counterpart to `adopt_routes_the_recovery_outputs_it_was_given`: a first
/// incarnation over non-empty storage recovers effects too, and a caller that
/// applied them outside the driver would drop them.
#[test]
fn new_routes_the_recovery_outputs_it_was_given() {
    let transport = QueueTransport::default();
    let validator = Validator {
        transport: transport.clone(),
        authorized: BTreeSet::from([NodeId(2)]),
    };
    let recovery_outputs = vec![RaftOutput::Send {
        to: NodeId(2),
        message: Message::RequestVote(RequestVote {
            term: Term(1),
            candidate_id: NodeId(1),
            last_log_index: LogIndex::ZERO,
            last_log_term: Term(0),
        }),
    }];

    let _driver: Driver = TransportRaftDriver::new(
        numbered_group(GROUP, 1, &[2], 3),
        recovery_outputs,
        transport.clone(),
        validator,
        TransportDriverOptions::default(),
    )
    .expect("a quiescent group is adoptable");

    let observed = transport.observed();
    assert_eq!(
        observed.len(),
        1,
        "the recovery report's peer messages reached the transport, got {observed:?}"
    );
    assert_eq!(observed[0].to, NodeId(2));
}

/// Zero is meaningless rather than merely small, so the driver refuses to be
/// built with it instead of failing at the first request.
#[test]
fn a_zero_bound_is_refused_at_construction() {
    let transport = QueueTransport::default();
    let validator = Validator {
        transport: transport.clone(),
        authorized: BTreeSet::from([NodeId(2)]),
    };

    let error = TransportRaftDriver::new(
        numbered_group(GROUP, 1, &[2], 3),
        Vec::new(),
        transport,
        validator,
        TransportDriverOptions::default().with_max_pending_waiters(0),
    )
    .map(|_: Driver| ())
    .expect_err("a driver that admits no waiters cannot serve anything");

    assert!(
        matches!(
            error,
            ManagedDriverError::InvalidOptions {
                field: "max_pending_waiters",
                ..
            }
        ),
        "got {error:?}"
    );
}

fn vote_envelope(from: NodeId, to: NodeId) -> AuthenticatedPeerEnvelope<u64, Principal> {
    AuthenticatedPeerEnvelope {
        group_id: GROUP,
        authenticated_peer: Principal::for_node(from),
        raft_from: from,
        raft_to: to,
        message: Message::RequestVote(RequestVote {
            term: Term(9),
            candidate_id: from,
            last_log_index: LogIndex::ZERO,
            last_log_term: Term(0),
        }),
    }
}
