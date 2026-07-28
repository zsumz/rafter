//! Who this driver authorizes while its own runtime is behind its durable
//! record.
//!
//! **A checkpoint stands ahead of the runtime it is restored beside, and that is
//! ordinary.** A commit index is volatile: a rebuilt runtime legitimately reports
//! a lower one than the incarnation that wrote the record had reached, and the
//! record's positioned observation of the committed membership is then the later
//! one. The register keeps it — that is what makes the register a register — and
//! the identities it names are live members of the group by this driver's own
//! durable evidence.
//!
//! The publication and the inbound check used to read a narrower set than the
//! register: the two *runtime* facts alone, effective and raw committed. So a
//! replica the record named live and the lagging runtime did not was published
//! beneath the retirement floor and absent from the peer set — which is the
//! wire definition of *retired* — and refused at this driver's own door. If that
//! replica was the leader, the frames that would have caught the runtime up were
//! the frames being refused, and nothing else was ever going to arrive.
//!
//! So authorization is derived from all three facts in union. The record's later
//! observation is sufficient protocol authorization for an identity while the
//! local runtime catches up; Raft itself validates what the frames say. What the
//! union deliberately does *not* move is the local replica's own service state,
//! which follows the runtime: a replica its own runtime does not name is
//! receiving no replication and must not answer clients, even while its identity
//! stays unretired everywhere else.

#![allow(clippy::wildcard_imports)]

mod support;

use std::collections::BTreeSet;

use rafter_service::{
    AuthenticatedPeerEnvelope, CurrentCommittedState, DriverServiceState,
    PeerControlPlaneCheckpoint, TransportDriverOptions,
};
use support::scripted::*;
use support::transport::*;
use support::*;

/// Where the durable record stands: ahead of every runtime in this file.
const RECORD_AT: LogIndex = LogIndex(10);

/// Where the rebuilt runtime stands: behind the record, which is the whole
/// subject.
const RUNTIME_AT: LogIndex = LogIndex(2);

fn ids(node_ids: &[u64]) -> BTreeSet<NodeId> {
    node_ids.iter().copied().map(NodeId).collect()
}

/// The record a previous incarnation persisted, at [`RECORD_AT`].
fn record(mark: u64, live: &[u64]) -> PeerControlPlaneCheckpoint<u64> {
    let mut checkpoint = PeerControlPlaneCheckpoint::empty(GROUP);
    checkpoint.committed_id_high_water = Some(NodeId(mark));
    checkpoint.current_committed = Some(CurrentCommittedState::new(RECORD_AT, ids(live)));
    checkpoint
}

fn a_vote(from: NodeId, to: NodeId) -> AuthenticatedPeerEnvelope<u64, Principal> {
    AuthenticatedPeerEnvelope {
        group_id: GROUP,
        authenticated_peer: Principal::for_node(from),
        raft_from: from,
        raft_to: to,
        message: Message::RequestVote(RequestVote {
            term: Term(1),
            candidate_id: from,
            last_log_index: LogIndex(5),
            last_log_term: Term(1),
        }),
    }
}

/// The reviewer's exact composition: a record through 10 naming `{1,2,3,4}`
/// under a runtime that has only caught up to `{1,2,3}` at commit 2.
///
/// `local` is which replica this driver is, because the same composition has to
/// be read from a peer's side and from node 4's own.
fn behind_runtime(local: u64) -> (ScriptedDriver, QueueTransport) {
    scripted_driver_with_checkpoint(
        ScriptedMembershipRuntime::for_node_at(NodeId(local), &[1, 2, 3], &[1, 2, 3], RUNTIME_AT),
        Nameable::all(),
        &[NodeId(1), NodeId(2), NodeId(3), NodeId(4)],
        TransportDriverOptions::default(),
        record(4, &[1, 2, 3, 4]),
    )
}

/// The published policy does not retire a replica this driver's own record calls
/// live.
///
/// Retirement is permanent and the floor never falls, so publishing one over an
/// identity the register still names is the one mistake no later publication
/// takes back. The floor here is 4 because the group committed node 4; the peer
/// set has to name node 4 as well, or the pair *is* a retirement.
#[test]
fn a_checkpoint_live_replica_is_not_published_as_retired() {
    let (_driver, transport) = behind_runtime(1);

    assert!(
        !transport.retires(NodeId(4)),
        "the record observed node 4 committed at 10 and the runtime has only \
         reached 2, so the lagging runtime's silence is not a removal: the link \
         layer holds {:?}",
        transport.policies().last()
    );
    assert_eq!(
        transport.policies().last().map(|(peers, _)| peers.clone()),
        Some(vec![NodeId(2), NodeId(3), NodeId(4)]),
        "and the peer set names it, which is what keeps the floor from retiring it"
    );
}

/// A frame from that replica reaches the group.
///
/// The self-locking half of the defect. If node 4 is the leader, its
/// `AppendEntries` are the only thing that advances this replica's runtime past
/// the record — and a driver that refuses them at its own door can never catch
/// up, so the condition that produced the refusal never ends.
#[test]
fn a_frame_from_a_checkpoint_live_replica_is_admitted() {
    let (driver, _transport) = behind_runtime(1);

    driver
        .deliver(a_vote(NodeId(4), NodeId(1)))
        .expect("node 4 is live in this driver's own record, so its frame is admitted");
}

/// A later *uncommitted* narrowing does not take that authorization away.
///
/// The effective configuration moves in both directions and can be truncated
/// back off the log, so it is a widening input and never a narrowing one. An
/// effective set that stops naming node 4 therefore leaves the record's
/// observation standing, and node 4 stays authorized until a *committed* fact
/// spends it.
#[test]
fn an_uncommitted_narrowing_leaves_a_checkpoint_live_replica_authorized() {
    let runtime =
        ScriptedMembershipRuntime::for_node_at(NodeId(1), &[1, 2, 3], &[1, 2, 3], RUNTIME_AT);
    let handle = runtime.handle();
    let (driver, transport) = scripted_driver_with_checkpoint(
        runtime,
        Nameable::all(),
        &[NodeId(1), NodeId(2), NodeId(3), NodeId(4)],
        TransportDriverOptions::default(),
        record(4, &[1, 2, 3, 4]),
    );

    // A new leader appends a configuration this replica adopts as effective and
    // that has not committed anywhere.
    change_on_step(&handle, &[1, 2], &[1, 2, 3]);
    driver.tick().expect("a tick advances the protocol");

    assert!(
        !transport.retires(NodeId(4)),
        "an uncommitted narrowing licenses no retirement: {:?}",
        transport.policies().last()
    );
    driver
        .deliver(a_vote(NodeId(4), NodeId(1)))
        .expect("and the frame is still admitted");
}

/// The local replica reads its own standing from its runtime, not from the
/// record.
///
/// The deliberate asymmetry. Node 4's own driver has a runtime that does not
/// name it, so it is receiving no replication and must refuse client work —
/// `NotMember`, which ends by itself when the runtime catches up, and not
/// `Decommissioned`, which is permanent. Meanwhile nothing about its *identity*
/// is retired: the record names it live, and every peer's policy says so.
#[test]
fn the_local_replica_is_not_a_member_while_its_own_runtime_is_behind() {
    let (driver, _transport) = behind_runtime(4);

    assert_eq!(
        driver.service_state(),
        DriverServiceState::NotMember { node_id: NodeId(4) },
        "the runtime does not name this replica, so it answers no client — but \
         its identity is unspent, so this is the condition that ends"
    );

    // And a peer reading the same record publishes node 4 as live.
    let (_peer, peer_transport) = behind_runtime(1);
    assert!(
        !peer_transport.retires(NodeId(4)),
        "the identity stays unretired everywhere else: {:?}",
        peer_transport.policies().last()
    );
}
