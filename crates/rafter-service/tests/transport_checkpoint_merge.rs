//! Joining a recovered checkpoint into a driver, and what a join must never do.
//!
//! A checkpoint is allowed to be stale — the public contract says so, and bounds
//! what staleness costs — so a *valid* record that predates a removal is an
//! ordinary input rather than a corruption. The join therefore has to be a
//! lattice join and not three independent merges: taking the union of the two
//! live sets makes a stale record's memory of a since-removed replica outrank
//! the removal the driver actually watched, and the identity the cluster
//! consumed becomes adoptable again.
//!
//! The three properties the join is written to have — symmetric, order-free,
//! monotone in spent-ness — are argued at
//! `TransportDriverState::restore_control_plane_checkpoint`. This file is the
//! evidence for them, plus the validation that keeps a damaged record from being
//! absorbed in part.

#![allow(clippy::wildcard_imports)]

mod support;

use std::collections::BTreeSet;

use rafter_service::{
    ControlPlaneCheckpointError, ManagedDriverError, PeerControlPlaneCheckpoint,
    TransportDriverOptions,
};
use support::scripted::*;
use support::transport::*;
use support::*;

fn ids(node_ids: &[u64]) -> BTreeSet<NodeId> {
    node_ids.iter().copied().map(NodeId).collect()
}

/// A checkpoint built by hand, the way a durable file hands one back.
fn checkpoint(mark: Option<u64>, live: &[u64], fences: &[u64]) -> PeerControlPlaneCheckpoint<u64> {
    let mut checkpoint = PeerControlPlaneCheckpoint::empty(GROUP);
    checkpoint.committed_id_high_water = mark.map(NodeId);
    checkpoint.live_committed_members = ids(live);
    checkpoint.pending_fences = ids(fences);
    checkpoint
}

/// A driver holding `{mark, live}` and nothing else, ready to be joined into.
///
/// Built through the documented restore path rather than by reaching inside, so
/// the state under test is a state a driver can actually be in. `mark: None`
/// means no record at all, and the mark the driver ends up with is the one its
/// own adoption derives from the runtime — which is what a first incarnation
/// looks like.
fn driver_holding(mark: Option<u64>, live: &[u64]) -> (ScriptedDriver, QueueTransport) {
    driver_holding_named(mark, live, Nameable::all())
}

/// The same, with a directory that can name only some replicas.
///
/// A fence for a replica this directory cannot name stays *owed* rather than
/// being discharged, which is how a test observes the obligations a join
/// contributed instead of watching a cooperative link swallow them.
fn driver_holding_named(
    mark: Option<u64>,
    live: &[u64],
    nameable: Nameable,
) -> (ScriptedDriver, QueueTransport) {
    let record = match mark {
        Some(mark) => checkpoint(Some(mark), live, &[]),
        None => PeerControlPlaneCheckpoint::empty(GROUP),
    };
    scripted_driver_with_checkpoint(
        ScriptedMembershipRuntime::new(live, live),
        nameable,
        &[NodeId(2), NodeId(5)],
        TransportDriverOptions::default(),
        record,
    )
}

/// The reviewer's exact scenario: a stale-but-valid checkpoint un-spends a
/// retired identity, and a group offered as that identity is then adopted.
///
/// `{mark 5, live {1,2,5}}` is what a process persisted *before* node 5 was
/// removed — a legal checkpoint, one the contract explicitly permits to be
/// stale. Joined by union into a driver that had watched the removal and holds
/// `{mark 5, live {1,2}}`, node 5 came back into the live set, stopped being
/// spent, and passed the adoption gate.
#[test]
fn a_stale_checkpoint_cannot_un_spend_a_retired_identity() {
    let (driver, _transport) = driver_holding(Some(5), &[1, 2]);
    let _ = driver.release_group().expect("the driver holds a group");

    let refused = driver.adopt_group_with_checkpoint(
        RaftGroup::new(
            GROUP,
            NodeId(5),
            ScriptedMembershipRuntime::for_node(NodeId(5), &[1, 2], &[1, 2]),
            KvStateMachine::default(),
        ),
        Vec::new(),
        checkpoint(Some(5), &[1, 2, 5], &[]),
    );

    assert!(
        matches!(
            refused,
            Err(ManagedDriverError::RetiredNodeId { node_id: NodeId(5) })
        ),
        "the driver watched node 5 leave, and no stale record may take that back: \
         got {refused:?}"
    );
    assert!(
        !driver
            .control_plane_checkpoint()
            .live_committed_members
            .contains(&NodeId(5)),
        "and the refusal left the identity spent rather than half-installed"
    );
}

/// The join is symmetric: the same two records in the other order agree.
///
/// The stale record is what the *driver* holds this time and the removal is what
/// arrives. A union would have been symmetric too and still wrong; what this
/// pins is that the spent-ness filter is applied to both sides rather than only
/// to the incoming one.
#[test]
fn the_join_is_symmetric_in_the_two_records() {
    let (driver, _transport) = driver_holding(Some(5), &[1, 2, 5]);
    let _ = driver.release_group().expect("the driver holds a group");

    let refused = driver.adopt_group_with_checkpoint(
        RaftGroup::new(
            GROUP,
            NodeId(5),
            ScriptedMembershipRuntime::for_node(NodeId(5), &[1, 2], &[1, 2]),
            KvStateMachine::default(),
        ),
        Vec::new(),
        checkpoint(Some(5), &[1, 2], &[]),
    );

    assert!(
        matches!(
            refused,
            Err(ManagedDriverError::RetiredNodeId { node_id: NodeId(5) })
        ),
        "spent-ness from either side sticks, got {refused:?}"
    );
}

/// Three records in any order reach the same state.
///
/// Order-freedom is what makes a supervisor's recovery sequence unimportant: a
/// takeover that reads two peers' checkpoints and its own has no correct order
/// to apply them in, and must not need one.
#[test]
fn the_join_is_order_free_across_three_records() {
    let records = [
        checkpoint(Some(4), &[1, 2, 3, 4], &[]),
        checkpoint(Some(6), &[1, 2, 3, 5, 6], &[4]),
        checkpoint(Some(6), &[1, 2, 5], &[3]),
    ];
    let orders = [[0, 1, 2], [2, 1, 0], [1, 0, 2], [0, 2, 1]];

    // The runtime's committed configuration names every identity any record
    // mentions, so the publication each adoption performs is a no-op and the only
    // thing moving state is the join. A narrower runtime would retire identities
    // between joins and measure the publication instead.
    let mut settled: Option<PeerControlPlaneCheckpoint<u64>> = None;
    for order in orders {
        let (driver, _transport) =
            driver_holding_named(None, &[1, 2, 3, 4, 5, 6], Nameable::only(&[NodeId(2)]));
        for index in order {
            let group = driver.release_group().expect("the driver holds a group");
            driver
                .adopt_group_with_checkpoint(group, Vec::new(), records[index].clone())
                .expect("each record is valid for this group");
        }
        let reached = driver.control_plane_checkpoint();
        match &settled {
            None => settled = Some(reached),
            Some(first) => assert_eq!(
                &reached, first,
                "the order {order:?} reached a different state"
            ),
        }
    }

    let settled = settled.expect("four orders were run");
    assert_eq!(settled.committed_id_high_water, Some(NodeId(6)));
    assert_eq!(
        settled.live_committed_members,
        ids(&[1, 2, 5]),
        "3 and 4 were each witnessed spent by one record, and 6 by another"
    );
    assert_eq!(
        settled.pending_fences,
        ids(&[3, 4]),
        "each record's obligations are owed, and this directory can name neither"
    );
}

/// Joining the same record twice changes nothing.
///
/// Idempotence is what lets an embedder re-read its file after a partial
/// recovery without reasoning about whether it already applied it.
#[test]
fn joining_the_same_record_twice_is_the_same_as_once() {
    let (driver, _transport) = driver_holding_named(None, &[1, 2], Nameable::only(&[NodeId(2)]));
    let record = checkpoint(Some(5), &[1, 2], &[5]);

    let group = driver.release_group().expect("the driver holds a group");
    driver
        .adopt_group_with_checkpoint(group, Vec::new(), record.clone())
        .expect("a valid record");
    let once = driver.control_plane_checkpoint();

    let group = driver.release_group().expect("the driver holds a group");
    driver
        .adopt_group_with_checkpoint(group, Vec::new(), record)
        .expect("a valid record");

    assert_eq!(driver.control_plane_checkpoint(), once);
}

/// An identity above one record's mark is judged only by the record that covers
/// it.
///
/// The clause that keeps the join from over-retiring. A record whose mark is 2
/// has no opinion about node 5 — it never saw the configuration that admitted
/// it — and must not be read as evidence that node 5 is spent just because its
/// live set does not name it.
#[test]
fn an_identity_above_a_records_mark_is_not_spent_by_that_record() {
    let (driver, _transport) = driver_holding(Some(5), &[1, 2, 5]);
    let group = driver.release_group().expect("the driver holds a group");
    driver
        .adopt_group_with_checkpoint(group, Vec::new(), checkpoint(Some(2), &[1, 2], &[]))
        .expect("an older record is valid");

    let settled = driver.control_plane_checkpoint();
    assert_eq!(settled.committed_id_high_water, Some(NodeId(5)));
    assert!(
        settled.live_committed_members.contains(&NodeId(5)),
        "the older record's silence about node 5 is not a removal"
    );
}

/// A checkpoint from another group is refused, and installs nothing.
#[test]
fn a_checkpoint_from_another_group_is_refused() {
    let (driver, _transport) = driver_holding(Some(3), &[1, 2, 3]);
    let before = driver.control_plane_checkpoint();
    let group = driver.release_group().expect("the driver holds a group");

    let mut foreign = checkpoint(Some(9), &[1, 2, 3, 9], &[7]);
    foreign.group = GROUP + 1;
    let refused = driver.adopt_group_with_checkpoint(group, Vec::new(), foreign);

    assert!(
        matches!(
            refused,
            Err(ManagedDriverError::InvalidControlPlaneCheckpoint {
                reason: ControlPlaneCheckpointError::ForeignGroup
            })
        ),
        "got {refused:?}"
    );
    assert_eq!(
        driver.control_plane_checkpoint(),
        before,
        "a refused record moves nothing"
    );
}

/// Each way a record can contradict a driver's invariants is refused whole.
///
/// The three clauses hold by construction for a record a driver produced, so
/// each failure means the durable bytes were damaged — and each one lowers a
/// retirement record in the direction that un-retires an identity.
#[test]
fn a_contradictory_checkpoint_is_refused_and_installs_nothing() {
    let cases: [(PeerControlPlaneCheckpoint<u64>, ControlPlaneCheckpointError); 3] = [
        (
            checkpoint(None, &[1, 2], &[]),
            ControlPlaneCheckpointError::LiveMembersWithoutMark { node_id: NodeId(1) },
        ),
        (
            checkpoint(Some(2), &[1, 2, 5], &[]),
            ControlPlaneCheckpointError::LiveMemberAboveMark {
                node_id: NodeId(5),
                mark: NodeId(2),
            },
        ),
        (
            checkpoint(Some(3), &[1, 2, 3], &[2]),
            ControlPlaneCheckpointError::FenceNamesLiveMember { node_id: NodeId(2) },
        ),
    ];

    for (damaged, expected) in cases {
        let (driver, _transport) = driver_holding(Some(3), &[1, 2, 3]);
        let before = driver.control_plane_checkpoint();
        let group = driver.release_group().expect("the driver holds a group");

        let refused = driver.adopt_group_with_checkpoint(group, Vec::new(), damaged);
        let Err(ManagedDriverError::InvalidControlPlaneCheckpoint { reason }) = refused else {
            panic!("expected a typed checkpoint refusal, got {refused:?}");
        };
        assert_eq!(reason, expected);
        assert_eq!(
            driver.control_plane_checkpoint(),
            before,
            "a refused record moves nothing, so nothing is half-installed"
        );
    }
}

/// A fence the record cannot show a mark for is refused.
///
/// The clause the other three do not imply, and the one whose absence points the
/// wrong way. `FenceNamesLiveMember` catches a fence naming an identity this
/// record says is *live*; nothing caught a fence naming one this record has no
/// opinion about at all. An identity above the mark was never in any committed
/// configuration this record witnessed, so no committed removal here can have
/// spent it — and a fence is the residue of a committed removal or it is
/// nothing.
///
/// Absorbed instead of refused, it is the one contradiction that survives the
/// join intact: the mark rises to cover a *live* identity, the obligation
/// travels with it, and the driver publishes the replica to its link layer and
/// then permanently fences it. `fence_peer` has no inverse, so that is not a
/// stale peer set that the next flush corrects.
#[test]
fn a_fence_naming_an_identity_the_record_never_spent_is_refused() {
    let cases: [(PeerControlPlaneCheckpoint<u64>, ControlPlaneCheckpointError); 2] = [
        (
            checkpoint(None, &[], &[7]),
            ControlPlaneCheckpointError::FenceNamesUnspentIdentity { node_id: NodeId(7) },
        ),
        (
            checkpoint(Some(5), &[1, 2, 5], &[7]),
            ControlPlaneCheckpointError::FenceNamesUnspentIdentity { node_id: NodeId(7) },
        ),
    ];

    for (damaged, expected) in cases {
        let (driver, _transport) = driver_holding(Some(3), &[1, 2, 3]);
        let before = driver.control_plane_checkpoint();
        let group = driver.release_group().expect("the driver holds a group");

        let refused = driver.adopt_group_with_checkpoint(group, Vec::new(), damaged);
        let Err(ManagedDriverError::InvalidControlPlaneCheckpoint { reason }) = refused else {
            panic!("expected a typed checkpoint refusal, got {refused:?}");
        };
        assert_eq!(reason, expected);
        assert_eq!(
            driver.control_plane_checkpoint(),
            before,
            "a refused record moves nothing, so nothing is half-installed"
        );
    }
}

/// The refusal lands before the link layer is told anything.
///
/// The cross-record shape, which is the one that costs a live replica. This
/// driver's own committed configuration names node 7, so the join would raise
/// the mark past it, keep it live — and keep the obligation the record brought.
/// The next flush is then two contradictory statements about one replica:
/// publish it, then fence it forever.
///
/// So the assertion is about *when* rather than only about what. Validation runs
/// before the first field moves and therefore before any derivation reaches the
/// transport, which is what makes a damaged file a refusal to open rather than a
/// replica this process has already helped destroy.
#[test]
fn a_fence_contradicting_a_live_member_is_refused_before_any_transport_call() {
    let (driver, transport) = driver_holding(Some(7), &[1, 2, 7]);
    let before = driver.control_plane_checkpoint();
    let fences_before = transport.fence_attempts();
    let group = driver.release_group().expect("the driver holds a group");

    let refused = driver.adopt_group_with_checkpoint(
        group,
        Vec::new(),
        checkpoint(Some(5), &[1, 2, 5], &[7]),
    );

    assert!(
        matches!(
            refused,
            Err(ManagedDriverError::InvalidControlPlaneCheckpoint {
                reason: ControlPlaneCheckpointError::FenceNamesUnspentIdentity {
                    node_id: NodeId(7)
                }
            })
        ),
        "got {refused:?}"
    );
    assert_eq!(
        driver.control_plane_checkpoint(),
        before,
        "a refused record moves nothing"
    );
    assert_eq!(
        transport.fence_attempts(),
        fences_before,
        "the link layer was asked to fence a replica this driver still needs"
    );
    assert!(
        !transport.is_fenced(NodeId(7)),
        "node 7 is live in this driver's own committed configuration"
    );
}

/// The control: a valid stale record still contributes everything it knows.
///
/// Without it, a join that refused every stale record would pass every clause
/// above and lose the whole point of the checkpoint. A record that saw a fence
/// the driver never did still owes that fence.
#[test]
fn a_stale_record_still_contributes_the_fence_it_witnessed() {
    let (driver, transport) = driver_holding(Some(3), &[1, 2, 3]);
    let group = driver.release_group().expect("the driver holds a group");

    driver
        .adopt_group_with_checkpoint(group, Vec::new(), checkpoint(Some(9), &[1, 2, 3], &[9]))
        .expect("a valid record from a process that saw further");

    let settled = driver.control_plane_checkpoint();
    assert_eq!(settled.committed_id_high_water, Some(NodeId(9)));
    assert!(
        transport.is_fenced(NodeId(9)),
        "the obligation the other process could not discharge became this one's,          and this link took it"
    );
}
