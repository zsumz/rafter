#![allow(clippy::wildcard_imports)]

//! The one membership branch no public entry point of this driver can reach.
//!
//! [`MembershipEvent::Appended`] is emitted only for a step that carried a
//! membership *request*, and the only input that carries one is
//! `GroupInput::Membership`, which [`super::super::TransportRaftDriver`] has no
//! method to produce — deliberately, because a membership-change API on this
//! driver is a promoted mechanism with no consumer behind it. The widening arm
//! is therefore live code waiting for an entry point rather than dead code, and
//! the difference is only checkable from inside the crate. Every other
//! membership clause is pinned by `tests/transport_streams.rs`, over a real
//! group.

use std::{
    error::Error,
    fmt,
    sync::{Arc, Mutex},
};

use rafter::{MembershipConfig, MembershipSet, NodeConfig, Term};
use rafter_app::state_machine::{
    ApplyBatch, ApplyResult, ReadBarrier, ReplicatedStateMachine, SnapshotSupport,
};
use rafter_runtime::DurableRaftNode;
use rafter_storage::InMemoryRaftHardStateStore;

use super::super::{TransportDriverOptions, TransportRaftDriver};
use super::*;
use crate::transport::SnapshotChunkEnvelope;

const GROUP: u64 = 3;

#[derive(Debug)]
struct NeverFails;

impl fmt::Display for NeverFails {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("this fixture never fails")
    }
}

impl Error for NeverFails {}

/// A state machine with nothing in it: these tests route membership events
/// and apply nothing.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
struct EmptyStateMachine;

impl ReplicatedStateMachine for EmptyStateMachine {
    type Command = ();
    type CommandResult = ();
    type Query = ();
    type QueryResult = ();
    type Error = NeverFails;

    const SNAPSHOT_SUPPORT: SnapshotSupport = SnapshotSupport::Unsupported;

    fn applied_index(&self) -> Result<LogIndex, Self::Error> {
        Ok(LogIndex::ZERO)
    }

    fn encode_command(&self, (): &Self::Command) -> Result<Vec<u8>, Self::Error> {
        Ok(Vec::new())
    }

    fn decode_command(&self, _payload: &[u8]) -> Result<Self::Command, Self::Error> {
        Ok(())
    }

    fn apply_batch(
        &mut self,
        batch: ApplyBatch<Self::Command>,
    ) -> Result<Vec<ApplyResult<Self::CommandResult>>, Self::Error> {
        Ok(batch
            .entries
            .into_iter()
            .map(|entry| ApplyResult {
                index: entry.index,
                term: entry.term,
                result: (),
                local_proposal_id: entry.local_proposal_id,
            })
            .collect())
    }

    fn read(
        &self,
        (): Self::Query,
        _barrier: ReadBarrier,
    ) -> Result<Self::QueryResult, Self::Error> {
        Ok(())
    }
}

/// A principal is not a `NodeId`, here as everywhere: the transport
/// authenticates a principal and the validator decides which replica it is.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Principal(NodeId);

#[derive(Default)]
struct RecordedLink {
    peer_sets: Vec<Vec<Principal>>,
    fenced: Vec<Principal>,
}

#[derive(Clone, Default)]
struct RecordingTransport {
    inner: Arc<Mutex<RecordedLink>>,
}

impl RecordingTransport {
    fn peer_sets(&self) -> Vec<Vec<NodeId>> {
        lock_state(&self.inner)
            .peer_sets
            .iter()
            .map(|peers| peers.iter().map(|peer| peer.0).collect())
            .collect()
    }

    fn fenced(&self) -> Vec<NodeId> {
        lock_state(&self.inner)
            .fenced
            .iter()
            .map(|peer| peer.0)
            .collect()
    }
}

impl RaftTransport<u64> for RecordingTransport {
    type PeerPrincipal = Principal;
    type Error = NeverFails;

    fn send(&self, _envelope: PeerEnvelope<u64>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn send_snapshot_chunk(
        &self,
        _envelope: SnapshotChunkEnvelope<u64>,
    ) -> Result<(), Self::Error> {
        Ok(())
    }

    fn update_peers(
        &self,
        _group_id: &u64,
        peers: PeerSet<Self::PeerPrincipal>,
    ) -> Result<(), Self::Error> {
        lock_state(&self.inner).peer_sets.push(peers.into_peers());
        Ok(())
    }

    fn fence_peer(&self, _group_id: &u64, peer: Self::PeerPrincipal) -> Result<(), Self::Error> {
        lock_state(&self.inner).fenced.push(peer);
        Ok(())
    }
}

#[derive(Clone)]
struct NameEveryone;

impl AuthenticatedPeerValidator<u64, Principal> for NameEveryone {
    fn is_known_group(&self, group_id: &u64) -> bool {
        *group_id == GROUP
    }

    fn node_for_authenticated_peer(&self, _group_id: &u64, peer: &Principal) -> Option<NodeId> {
        Some(peer.0)
    }

    fn principal_for_node(&self, _group_id: &u64, node_id: NodeId) -> Option<Principal> {
        Some(Principal(node_id))
    }

    fn is_authorized_peer(&self, _group_id: &u64, _node_id: NodeId) -> bool {
        true
    }

    fn is_fenced_peer(&self, _group_id: &u64, _node_id: NodeId) -> bool {
        false
    }
}

type TestDriver =
    TransportRaftDriver<u64, EmptyStateMachine, DurableRaftNode, RecordingTransport, NameEveryone>;

fn driver(peers: &[u64]) -> (TestDriver, RecordingTransport) {
    let transport = RecordingTransport::default();
    let config = NodeConfig::new(NodeId(1), peers.iter().copied().map(NodeId).collect(), 3)
        .expect("static Raft config is valid");
    let raft = DurableRaftNode::new(config, InMemoryRaftHardStateStore::new())
        .expect("in-memory durable node opens");
    let driver = TransportRaftDriver::new(
        RaftGroup::new(GROUP, NodeId(1), raft, EmptyStateMachine),
        Vec::new(),
        transport.clone(),
        NameEveryone,
        TransportDriverOptions::default(),
    )
    .expect("a quiescent group is adoptable");
    (driver, transport)
}

fn membership(voters: &[u64]) -> MembershipConfig {
    MembershipConfig::stable(
        MembershipSet::new(voters.iter().copied().map(NodeId).collect(), Vec::new())
            .expect("scripted membership is valid"),
    )
}

fn appended(voters: &[u64]) -> MembershipEvent<u64> {
    MembershipEvent::Appended {
        group_id: GROUP,
        index: LogIndex(1),
        term: Term(1),
        membership: membership(voters),
    }
}

/// An effective-but-uncommitted change may only widen. A replica joining
/// under joint consensus has to be able to speak before the change commits,
/// or it can never catch up and the change can never commit.
#[test]
fn an_appended_membership_widens_the_published_peer_set() {
    let (driver, transport) = driver(&[2]);

    driver
        .inner
        .lock()
        .route_membership_event(&appended(&[1, 2, 3]));

    assert_eq!(
        transport.peer_sets(),
        vec![vec![NodeId(2)], vec![NodeId(2), NodeId(3)]],
        "construction published the group's membership, and the append widened it"
    );
    assert!(
        transport.fenced().is_empty(),
        "an uncommitted change fences nothing"
    );
    assert_eq!(driver.refused_peer_updates(), 0);
}

/// The union is the whole rule: an uncommitted change can still be
/// reverted, so nothing may be taken away for one.
#[test]
fn an_appended_membership_never_narrows_and_never_fences() {
    let (driver, transport) = driver(&[2, 3]);

    driver
        .inner
        .lock()
        .route_membership_event(&appended(&[1, 2]));

    assert_eq!(
        transport.peer_sets(),
        vec![vec![NodeId(2), NodeId(3)], vec![NodeId(2), NodeId(3)]],
        "node 3 stayed authorized: the change that drops it has not committed"
    );
    assert!(
        transport.fenced().is_empty(),
        "only a committed removal licenses a fence"
    );
}

/// The two arms compose, and a commit does not retract authorization that the
/// membership currently in effect still needs.
///
/// One step can both commit a change and append the next one, and
/// `rafter-app` emits the `Appended` for the newer one *before* the `Applied`
/// for the older. An `Applied` arm that replaced the peer set with the
/// committed configuration alone would undo the widening two lines earlier and
/// fence the replica it authorized, so it publishes the union of the committed
/// membership and the effective one and fences only what neither names. Here
/// the fixture's runtime is a real node whose effective membership is `{1,2}`,
/// so node 2 keeps speaking through a committed configuration that omits it.
#[test]
fn a_committed_change_does_not_narrow_past_the_membership_in_effect() {
    let (driver, transport) = driver(&[2]);

    {
        let mut state = driver.inner.lock();
        state.route_membership_event(&appended(&[1, 2, 3]));
        state.route_membership_event(&MembershipEvent::Applied {
            group_id: GROUP,
            index: LogIndex(2),
            term: Term(1),
            membership: membership(&[1, 3]),
        });
    }

    assert_eq!(
        transport
            .peer_sets()
            .last()
            .expect("a peer set was published"),
        &vec![NodeId(2), NodeId(3)],
        "node 3 joined the committed set and node 2 is still in effect"
    );
    assert!(
        transport.fenced().is_empty(),
        "nothing left both the committed membership and the effective one, so \
         nothing may be fenced: fenced = {:?}",
        transport.fenced()
    );
}
