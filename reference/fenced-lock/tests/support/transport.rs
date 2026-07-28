//! Consumer-owned deterministic transport over the public service contract.
//!
//! A real deployment supplies a real network and a test supplies a controllable
//! one, so this module is consumer-owned on purpose rather than for want of a
//! shipped driver. It implements [`RaftTransport`] with explicit delivery
//! control: nothing moves between replicas until the test asks for it, and a
//! cut link is a real refusal rather than a silently skipped queue.
//!
//! Frames leave a node as [`PeerEnvelope`] values and arrive as
//! [`AuthenticatedPeerEnvelope`] values, which `TransportRaftDriver::deliver`
//! validates before any group sees them. That is the production shape the crate
//! documents, and it is the only reason a `PeerPrincipal` exists here: proving
//! who is on the far end of a connection and deciding which Raft replica that is
//! are separate steps, so [`PeerDirectory`] is the driver's validator while
//! [`NodeTransport`] is its link.
//!
//! Lock order is node state first, network second. Every path that sends holds
//! a node lock and then takes the network lock; no path takes them the other
//! way around.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    error::Error,
    fmt,
    sync::{Arc, Mutex, MutexGuard, PoisonError},
};

use rafter::NodeId;
use rafter_service::{
    AuthenticatedPeerEnvelope, AuthenticatedPeerValidator, PeerEnvelope, PeerPolicy, RaftTransport,
    SnapshotChunkEnvelope,
};

use crate::cluster::{LockGroupId, GROUP_ID};

/// Frames the network will hold before it refuses to accept more.
///
/// The service contract asks transports for bounded queues rather than
/// unbounded growth. A deterministic test never reaches this bound, which is
/// the point: exceeding it means the driver stopped draining.
const MAX_IN_FLIGHT: usize = 256;

/// Authenticated transport identity of one replica.
///
/// A principal is deliberately not a [`NodeId`]. The transport authenticates a
/// principal; the validator decides which Raft replica that principal is
/// allowed to be. Collapsing the two would erase the check that
/// [`AuthenticatedPeerEnvelopeError::AuthenticatedPeerMismatch`] exists for.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct PeerPrincipal(String);

impl PeerPrincipal {
    /// Returns the principal this deployment issues to one replica.
    pub fn for_node(node_id: NodeId) -> Self {
        Self(format!("replica-{}", node_id.0))
    }
}

impl fmt::Display for PeerPrincipal {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Why the deterministic transport refused a frame.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransportError {
    /// Every link out of the sending replica is cut.
    PeerUnreachable { peer: NodeId },
    /// The frame names a group this deployment does not route.
    UnknownGroup,
    /// The bounded in-flight queue is full, so the frame fails closed.
    QueueFull { limit: usize },
    /// A leader snapshot chunk directive reached the transport.
    ///
    /// This deployment's runtime resolves chunk directives against its own
    /// snapshot store before the app layer ever sees one, so a directive
    /// arriving here means the runtime and the driver disagree about who owns
    /// the payload. Refusing says so; the driver counts it and the protocol
    /// re-sends.
    UnresolvedSnapshotChunk { to: NodeId },
}

impl fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PeerUnreachable { peer } => write!(formatter, "peer {peer} is unreachable"),
            Self::UnknownGroup => formatter.write_str("frame names an unrouted group"),
            Self::QueueFull { limit } => {
                write!(formatter, "transport queue is full at {limit} frames")
            }
            Self::UnresolvedSnapshotChunk { to } => write!(
                formatter,
                "a snapshot chunk directive for {to} reached a transport that resolves none"
            ),
        }
    }
}

impl Error for TransportError {}

/// Shared deterministic in-memory network for one lock cluster.
#[derive(Clone, Debug, Default)]
pub struct DeterministicNetwork {
    shared: Arc<Mutex<NetworkState>>,
}

#[derive(Debug, Default)]
struct NetworkState {
    in_flight: VecDeque<AuthenticatedPeerEnvelope<LockGroupId, PeerPrincipal>>,
    blocked_inbound: BTreeSet<NodeId>,
    blocked_outbound: BTreeSet<NodeId>,
    dropped_inbound: u64,
}

impl DeterministicNetwork {
    /// Creates an idle network with every link up.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the transport endpoint one replica sends through.
    pub fn endpoint(&self, node_id: NodeId, directory: PeerDirectory) -> NodeTransport {
        NodeTransport {
            network: self.clone(),
            local_node_id: node_id,
            local_principal: PeerPrincipal::for_node(node_id),
            directory,
        }
    }

    /// Removes every frame currently in flight, dropping those the receiver
    /// cannot hear.
    ///
    /// Draining in one batch is what makes delivery deterministic: frames a
    /// delivery produces are handled by the next call, never appended to the
    /// batch being processed.
    pub fn take_deliverable(&self) -> Vec<AuthenticatedPeerEnvelope<LockGroupId, PeerPrincipal>> {
        let mut state = lock(&self.shared);
        let batch = std::mem::take(&mut state.in_flight);
        let mut deliverable = Vec::with_capacity(batch.len());
        for envelope in batch {
            if state.blocked_inbound.contains(&envelope.raft_to) {
                state.dropped_inbound += 1;
                continue;
            }
            deliverable.push(envelope);
        }
        deliverable
    }

    /// Cuts every link to and from `node_id`.
    pub fn isolate(&self, node_id: NodeId) {
        let mut state = lock(&self.shared);
        state.blocked_inbound.insert(node_id);
        state.blocked_outbound.insert(node_id);
    }

    /// Cuts only the links into `node_id`.
    ///
    /// A replica in this state keeps sending and keeps believing whatever its
    /// last successful round told it, because nothing can contradict it. That
    /// is exactly the isolated-former-leader shape the fencing proof needs.
    pub fn isolate_inbound(&self, node_id: NodeId) {
        lock(&self.shared).blocked_inbound.insert(node_id);
    }

    /// Restores every cut link.
    pub fn heal(&self) {
        let mut state = lock(&self.shared);
        state.blocked_inbound.clear();
        state.blocked_outbound.clear();
    }

    /// Returns whether `node_id` can currently receive frames.
    pub fn reaches(&self, node_id: NodeId) -> bool {
        let state = lock(&self.shared);
        !state.blocked_inbound.contains(&node_id) && !state.blocked_outbound.contains(&node_id)
    }

    /// Returns how many accepted frames were dropped before delivery.
    pub fn dropped_inbound(&self) -> u64 {
        lock(&self.shared).dropped_inbound
    }

    /// Returns whether any accepted frame is still waiting to be delivered.
    pub fn is_idle(&self) -> bool {
        lock(&self.shared).in_flight.is_empty()
    }

    fn accept(
        &self,
        envelope: AuthenticatedPeerEnvelope<LockGroupId, PeerPrincipal>,
        from: NodeId,
    ) -> Result<(), TransportError> {
        let mut state = lock(&self.shared);
        if state.blocked_outbound.contains(&from) {
            return Err(TransportError::PeerUnreachable {
                peer: envelope.raft_to,
            });
        }
        if state.in_flight.len() >= MAX_IN_FLIGHT {
            return Err(TransportError::QueueFull {
                limit: MAX_IN_FLIGHT,
            });
        }
        state.in_flight.push_back(envelope);
        Ok(())
    }
}

/// One replica's endpoint on the deterministic network.
#[derive(Clone, Debug)]
pub struct NodeTransport {
    network: DeterministicNetwork,
    local_node_id: NodeId,
    local_principal: PeerPrincipal,
    directory: PeerDirectory,
}

impl RaftTransport<LockGroupId> for NodeTransport {
    type PeerPrincipal = PeerPrincipal;
    type Error = TransportError;

    fn send(&self, envelope: PeerEnvelope<LockGroupId>) -> Result<(), Self::Error> {
        if envelope.group_id != GROUP_ID {
            return Err(TransportError::UnknownGroup);
        }
        self.network.accept(
            AuthenticatedPeerEnvelope {
                group_id: envelope.group_id,
                authenticated_peer: self.local_principal.clone(),
                raft_from: envelope.from,
                raft_to: envelope.to,
                message: envelope.message,
            },
            self.local_node_id,
        )
    }

    /// Refuses every directive, and says why.
    ///
    /// [`LockNode`](crate::cluster::NodeDriver) is a `DurableRaftNode`, which
    /// owns its snapshot store and resolves `SendSnapshotChunk` outputs into
    /// `InstallSnapshotChunk` frames inside `step`. Those frames arrive here
    /// through [`RaftTransport::send`] like any other. A directive reaching this
    /// method would mean a runtime that did not resolve it, which this
    /// deployment does not have — so the honest body is a refusal rather than a
    /// second snapshot store bolted onto a link.
    fn send_snapshot_chunk(
        &self,
        envelope: SnapshotChunkEnvelope<LockGroupId>,
    ) -> Result<(), Self::Error> {
        if envelope.group_id != GROUP_ID {
            return Err(TransportError::UnknownGroup);
        }
        Err(TransportError::UnresolvedSnapshotChunk { to: envelope.to })
    }

    fn update_peers(
        &self,
        group_id: &LockGroupId,
        policy: PeerPolicy<Self::PeerPrincipal>,
    ) -> Result<(), Self::Error> {
        if *group_id != GROUP_ID {
            return Err(TransportError::UnknownGroup);
        }
        self.directory
            .install_policy(policy.retirement_floor(), policy.into_peers());
        Ok(())
    }
}

/// One replica's view of which principals may speak to it.
#[derive(Clone, Debug, Default)]
pub struct PeerDirectory {
    shared: Arc<Mutex<DirectoryState>>,
}

#[derive(Debug, Default)]
struct DirectoryState {
    /// Every principal this deployment can name, and the replica it is.
    known: BTreeMap<PeerPrincipal, NodeId>,
    /// The principals currently allowed to speak, set through `update_peers`.
    authorized: BTreeSet<PeerPrincipal>,
    /// The greatest identity the group has ever committed, as last published.
    ///
    /// An identity at or below this that `authorized` does not name is retired.
    /// Kept as a maximum, so a link that misses a publication retires fewer
    /// identities rather than un-retiring one.
    retirement_floor: Option<NodeId>,
}

impl PeerDirectory {
    /// Builds a directory that can name every replica and authorizes none.
    ///
    /// Naming is the deployment's knowledge and authorizing is the cluster's,
    /// so this half is all a directory can supply on its own. The driver
    /// publishes the group's membership as a peer set when it adopts the group,
    /// before it serves anything, which is what fills the other half — and a
    /// directory that pre-authorized its peers here would hide whether that
    /// happened.
    pub fn new(all_nodes: &[NodeId]) -> Self {
        let directory = Self::default();
        let mut state = lock(&directory.shared);
        for node_id in all_nodes {
            state
                .known
                .insert(PeerPrincipal::for_node(*node_id), *node_id);
        }
        drop(state);
        directory
    }

    /// Names and authorizes one principal the cluster's membership does not.
    ///
    /// The deployment knowing about a replica the cluster does not name is the
    /// ordinary state of affairs in the window between a committed removal and
    /// the policy the link layer has accepted — and this lock cluster's membership
    /// never changes, so the window cannot be produced by reconfiguring it.
    /// Widening the directory by hand puts the validator and the group in
    /// exactly the disagreement a late frame from a retired replica creates.
    ///
    /// Additive rather than replacing, because `update_peers` owns the
    /// membership-derived half and a test must not silently take it away.
    pub fn authorize_beyond_membership(&self, node_id: NodeId) {
        let principal = PeerPrincipal::for_node(node_id);
        let mut state = lock(&self.shared);
        state.known.insert(principal.clone(), node_id);
        state.authorized.insert(principal);
    }

    fn install_policy(&self, retirement_floor: Option<NodeId>, peers: Vec<PeerPrincipal>) {
        let mut state = lock(&self.shared);
        state.authorized = peers.into_iter().collect();
        state.retirement_floor = match (state.retirement_floor, retirement_floor) {
            (Some(held), Some(published)) => Some(held.max(published)),
            (held, None) => held,
            (None, published) => published,
        };
    }
}

impl AuthenticatedPeerValidator<LockGroupId, PeerPrincipal> for PeerDirectory {
    fn is_known_group(&self, group_id: &LockGroupId) -> bool {
        *group_id == GROUP_ID
    }

    fn node_for_authenticated_peer(
        &self,
        _group_id: &LockGroupId,
        peer: &PeerPrincipal,
    ) -> Option<NodeId> {
        lock(&self.shared).known.get(peer).copied()
    }

    /// The inverse lookup, out of the same directory the forward one uses.
    ///
    /// The driver needs it to express a group's membership as a peer set: a
    /// membership names replicas, and `update_peers` takes principals.
    fn principal_for_node(
        &self,
        _group_id: &LockGroupId,
        node_id: NodeId,
    ) -> Option<PeerPrincipal> {
        let state = lock(&self.shared);
        state
            .known
            .iter()
            .find_map(|(principal, mapped)| (*mapped == node_id).then(|| principal.clone()))
    }

    fn is_authorized_peer(&self, _group_id: &LockGroupId, node_id: NodeId) -> bool {
        let state = lock(&self.shared);
        state
            .authorized
            .iter()
            .any(|peer| state.known.get(peer) == Some(&node_id))
    }

    /// Derived from the published policy rather than recorded per principal.
    fn is_fenced_peer(&self, _group_id: &LockGroupId, node_id: NodeId) -> bool {
        let state = lock(&self.shared);
        state.retirement_floor.is_some_and(|floor| node_id <= floor)
            && !state
                .authorized
                .iter()
                .any(|peer| state.known.get(peer) == Some(&node_id))
    }
}

fn lock<T>(shared: &Mutex<T>) -> MutexGuard<'_, T> {
    shared.lock().unwrap_or_else(PoisonError::into_inner)
}
