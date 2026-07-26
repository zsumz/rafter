#![allow(dead_code)]

//! The link, the directory, and the cluster every transport-driver test needs.
//!
//! Shared rather than repeated: four test binaries drive the same one-replica
//! composition from different angles, and a second copy of a deterministic
//! network is a second thing to keep honest.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    error::Error,
    fmt,
    sync::{Arc, Mutex},
};

use rafter::{InMemorySnapshotChunkSource, RaftSnapshot};
use rafter_runtime::DurableRaftNode;
use rafter_service::{
    AuthenticatedPeerEnvelope, AuthenticatedPeerValidator, PeerEnvelope, PeerSet, RaftTransport,
    SnapshotChunkEnvelope, TransportDriverOptions, TransportRaftDriver,
};

use super::{
    numbered_group, numbered_group_with_app, poll_once, KvStateMachine, Message, NodeId,
    NumberedGroup, Role,
};

pub(crate) const GROUP: u64 = 7;

/// Authenticated transport identity of one replica.
///
/// Deliberately not a `NodeId`: the transport authenticates a principal, and
/// the validator decides which Raft replica that principal is allowed to be.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct Principal(String);

impl Principal {
    pub(crate) fn for_node(node_id: NodeId) -> Self {
        Self(format!("replica-{}", node_id.0))
    }
}

#[derive(Debug)]
pub(crate) struct TransportError(&'static str);

impl fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for TransportError {}

#[derive(Default)]
pub(crate) struct QueueState {
    sent: VecDeque<PeerEnvelope<u64>>,
    observed: Vec<PeerEnvelope<u64>>,
    cut: bool,
    fenced: BTreeSet<NodeId>,
    /// Every peer set this transport was told to authorize, in order.
    peer_sets: Vec<Vec<Principal>>,
    /// The snapshot payloads this link can serve, which is what makes it able
    /// to resolve a chunk directive into a frame.
    snapshots: InMemorySnapshotChunkSource,
}

/// A transport that holds every frame until a test asks for it.
#[derive(Clone, Default)]
pub(crate) struct QueueTransport {
    inner: Arc<Mutex<QueueState>>,
}

impl QueueTransport {
    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, QueueState> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Returns every frame the transport accepted since the last call.
    pub(crate) fn take_deliverable(&self) -> Vec<PeerEnvelope<u64>> {
        self.lock().sent.drain(..).collect()
    }

    /// Every frame the transport was ever handed, refused or not.
    pub(crate) fn observed(&self) -> Vec<PeerEnvelope<u64>> {
        self.lock().observed.clone()
    }

    pub(crate) fn cut(&self) {
        self.lock().cut = true;
    }

    /// Every peer set the driver published, in the order it published them.
    pub(crate) fn peer_sets(&self) -> Vec<Vec<Principal>> {
        self.lock().peer_sets.clone()
    }

    /// Registers the payload this link will serve for `snapshot`.
    pub(crate) fn serve_snapshot(&self, snapshot: &RaftSnapshot, payload: Vec<u8>) {
        self.lock()
            .snapshots
            .insert(snapshot, payload)
            .expect("the payload matches the snapshot it belongs to");
    }

    pub(crate) fn is_fenced(&self, node_id: NodeId) -> bool {
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

    /// Resolves the directive against this link's own snapshot payloads and
    /// sends the frame, which is the division of labour the kernel documents:
    /// the driver routes directives, the transport owns the bytes.
    fn send_snapshot_chunk(&self, envelope: SnapshotChunkEnvelope<u64>) -> Result<(), Self::Error> {
        let resolved = {
            let state = self.lock();
            envelope.chunk.resolve(&state.snapshots)
        };
        let Some(chunk) = resolved else {
            return Err(TransportError("this link serves no such snapshot payload"));
        };
        self.send(PeerEnvelope {
            group_id: envelope.group_id,
            from: envelope.from,
            to: envelope.to,
            message: Message::InstallSnapshotChunk(chunk),
        })
    }

    fn update_peers(
        &self,
        _group_id: &u64,
        peers: PeerSet<Self::PeerPrincipal>,
    ) -> Result<(), Self::Error> {
        self.lock().peer_sets.push(peers.into_peers());
        Ok(())
    }

    fn fence_peer(&self, _group_id: &u64, peer: Self::PeerPrincipal) -> Result<(), Self::Error> {
        let node_id = principal_node(&peer).ok_or(TransportError("unknown principal"))?;
        self.lock().fenced.insert(node_id);
        Ok(())
    }
}

pub(crate) fn principal_node(principal: &Principal) -> Option<NodeId> {
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
pub(crate) struct Validator {
    pub(crate) transport: QueueTransport,
    pub(crate) authorized: BTreeSet<NodeId>,
    /// Which replicas this deployment can name a principal for.
    ///
    /// `None` means every replica, which is the ordinary case. A test that
    /// restricts it is modelling a directory that has not learned a new
    /// replica's identity yet.
    pub(crate) nameable: Option<BTreeSet<NodeId>>,
}

impl AuthenticatedPeerValidator<u64, Principal> for Validator {
    fn is_known_group(&self, group_id: &u64) -> bool {
        *group_id == GROUP
    }

    fn node_for_authenticated_peer(&self, _group_id: &u64, peer: &Principal) -> Option<NodeId> {
        principal_node(peer)
    }

    fn principal_for_node(&self, _group_id: &u64, node_id: NodeId) -> Option<Principal> {
        self.nameable
            .as_ref()
            .is_none_or(|nameable| nameable.contains(&node_id))
            .then(|| Principal::for_node(node_id))
    }

    fn is_authorized_peer(&self, _group_id: &u64, node_id: NodeId) -> bool {
        self.authorized.contains(&node_id)
    }

    fn is_fenced_peer(&self, _group_id: &u64, node_id: NodeId) -> bool {
        self.transport.is_fenced(node_id)
    }
}

pub(crate) type Driver =
    TransportRaftDriver<u64, KvStateMachine, DurableRaftNode, QueueTransport, Validator>;

pub(crate) fn driver_for(node_id: u64, peers: &[u64]) -> (Driver, QueueTransport) {
    driver_with_options(node_id, peers, TransportDriverOptions::default())
}

pub(crate) fn driver_with_options(
    node_id: u64,
    peers: &[u64],
    options: TransportDriverOptions,
) -> (Driver, QueueTransport) {
    driver_over(numbered_group(GROUP, node_id, peers, 3), peers, options)
}

/// A driver over one replica whose state machine the test chose.
pub(crate) fn driver_over_app(
    node_id: u64,
    peers: &[u64],
    app: KvStateMachine,
) -> (Driver, QueueTransport) {
    driver_over(
        numbered_group_with_app(GROUP, node_id, peers, 3, app),
        peers,
        TransportDriverOptions::default(),
    )
}

/// The state machine every poison fixture wants: one that refuses to apply.
pub(crate) fn failing_apply() -> KvStateMachine {
    KvStateMachine {
        fail_apply: true,
        ..KvStateMachine::default()
    }
}

fn driver_over(
    group: NumberedGroup,
    peers: &[u64],
    options: TransportDriverOptions,
) -> (Driver, QueueTransport) {
    let transport = QueueTransport::default();
    let validator = Validator {
        transport: transport.clone(),
        authorized: peers.iter().copied().map(NodeId).collect(),
        nameable: None,
    };
    let driver = TransportRaftDriver::new(group, Vec::new(), transport.clone(), validator, options)
        .expect("a quiescent group is adoptable");
    (driver, transport)
}

/// Ticks one replica until it takes leadership on its own.
///
/// Only a single-voter replica reaches this without an exchange; a test with
/// peers uses [`elect`].
pub(crate) fn elect_single_voter(driver: &Driver) {
    for _ in 0..16 {
        if driver.handle().metrics().expect("metrics").current().role == Role::Leader {
            return;
        }
        driver.tick().expect("a tick advances the protocol");
    }
    panic!("the single-voter replica never took leadership");
}

/// Ticks past the election timeout this fixture configures.
pub(crate) fn tick_past_election_timeout(driver: &Driver) {
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
pub(crate) fn start<F: std::future::Future + Unpin>(future: &mut F) -> Option<F::Output> {
    poll_once(future)
}

/// Moves every frame the transports accepted to the driver it is addressed to,
/// then collects any barrier proofs the deliveries granted.
pub(crate) fn exchange(nodes: &BTreeMap<NodeId, (Driver, QueueTransport)>) -> usize {
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

pub(crate) fn cluster(node_ids: &[u64]) -> BTreeMap<NodeId, (Driver, QueueTransport)> {
    cluster_with_options(node_ids, &BTreeMap::new())
}

pub(crate) fn cluster_with_options(
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
pub(crate) fn elect(nodes: &BTreeMap<NodeId, (Driver, QueueTransport)>, primary: NodeId) {
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
pub(crate) fn settle(nodes: &BTreeMap<NodeId, (Driver, QueueTransport)>) {
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
