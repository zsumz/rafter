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

/// Where a hand-built record has consumed the configuration stream through.
///
/// Below the scripted runtime's commit index on purpose, so every adoption's
/// endpoint publication still folds rather than being skipped as replayed. These
/// tests are about the identity lattice, and a fixture that gated the
/// publication away would be measuring the gate instead.
const CONSUMED_THROUGH: LogIndex = LogIndex(1);

/// A checkpoint built by hand, the way a durable file hands one back.
///
/// **Both offsets travel with the retirement record**, because they are one
/// record and a driver never writes the state without them. A helper that left
/// them out would be building a shape the validator now refuses — which is the
/// point of `a_record_that_separates_an_offset_from_what_it_retired_is_refused`
/// below, and not something every other case here should be quietly asserting.
fn checkpoint(mark: Option<u64>, live: &[u64], fences: &[u64]) -> PeerControlPlaneCheckpoint<u64> {
    checkpoint_through(
        mark,
        live,
        fences,
        Some(CONSUMED_THROUGH),
        Some(CONSUMED_THROUGH),
    )
}

/// The same, with both consumer offsets chosen by the caller.
fn checkpoint_through(
    mark: Option<u64>,
    live: &[u64],
    fences: &[u64],
    crossings: Option<LogIndex>,
    endpoint: Option<LogIndex>,
) -> PeerControlPlaneCheckpoint<u64> {
    let mut checkpoint = PeerControlPlaneCheckpoint::empty(GROUP);
    checkpoint.committed_id_high_water = mark.map(NodeId);
    checkpoint.live_committed_members = ids(live);
    checkpoint.pending_fences = ids(fences);
    checkpoint.committed_crossings_through = crossings;
    checkpoint.committed_endpoint_through = endpoint;
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

/// A record that separates an offset from what it retired is refused, every way
/// round.
///
/// The offsets and the retirement record are one record. Each half is
/// individually well-formed here — every clause the other cases check still
/// holds — and each pair is a state no driver produces, because every committed
/// fact that touches the mark, the live set, or the obligations advances its own
/// offset in the same call.
///
/// **The coupling is not symmetric, and stating it as "at least one offset"
/// would let the dangerous shape through.** Every driver that folded anything
/// was constructed or adopted, and both entry points publish an endpoint fact
/// unconditionally — so retirement state implies the *endpoint* offset
/// specifically, while the crossing offset is genuinely optional beside a full
/// record. The cases below therefore refuse a crossing offset standing alone
/// with retirement state, and the case after this one accepts the endpoint-only
/// record it would otherwise have swept up.
///
/// The directions fail differently and an operator needs to know which. A final
/// live set with no endpoint offset leaves the endpoint fold ungated, so the
/// next adoption diffs a rebuilt runtime's committed configuration — possibly
/// *behind*, because a commit index is volatile — against a live set that has
/// moved past it, and permanently fences everything the newer configurations
/// added. An offset with nothing behind it makes recovery *skip* that history
/// and keep nothing from it, so every identity those configurations spent
/// becomes allocatable again with no fence owed.
#[test]
fn a_record_that_separates_an_offset_from_what_it_retired_is_refused() {
    let cases: [(PeerControlPlaneCheckpoint<u64>, ControlPlaneCheckpointError); 6] = [
        (
            checkpoint_through(Some(3), &[1, 2, 3], &[], None, None),
            ControlPlaneCheckpointError::CommittedStateWithoutEndpoint,
        ),
        // A mark and an obligation with no live set is still retirement state,
        // so it needs an endpoint offset like any other.
        (
            checkpoint_through(Some(5), &[], &[5], None, None),
            ControlPlaneCheckpointError::CommittedStateWithoutEndpoint,
        ),
        // **The shape "at least one offset" would have accepted.** A crossing
        // offset is not a substitute: no lifecycle produces retirement state
        // without an endpoint observation, and absorbing this one leaves the
        // endpoint fold ungated for the very next adoption.
        (
            checkpoint_through(Some(3), &[1, 2, 3], &[], Some(LogIndex(3)), None),
            ControlPlaneCheckpointError::CommittedStateWithoutEndpoint,
        ),
        (
            checkpoint_through(None, &[], &[], None, Some(LogIndex(3))),
            ControlPlaneCheckpointError::EndpointWithoutCommittedState,
        ),
        // The same emptiness through the other offset, and it needs its own
        // clause: with no endpoint offset beside it, the endpoint biconditional
        // is satisfied by both sides being absent and says nothing about this.
        (
            checkpoint_through(None, &[], &[], Some(LogIndex(3)), None),
            ControlPlaneCheckpointError::CrossingsWithoutCommittedState,
        ),
        // `LogIndex(0)` is a real position rather than an absence, which is why
        // the offsets are `Option`s — so this is the same separation and not a
        // zero standing in for `None`.
        (
            checkpoint_through(None, &[], &[], None, Some(LogIndex(0))),
            ControlPlaneCheckpointError::EndpointWithoutCommittedState,
        ),
    ];

    for (damaged, expected) in cases {
        let (driver, transport) = driver_holding(Some(3), &[1, 2, 3]);
        let before = driver.control_plane_checkpoint();
        let fences_before = transport.fence_attempts();
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
        assert_eq!(
            transport.fence_attempts(),
            fences_before,
            "and the link layer was told nothing on the way to the refusal"
        );
    }
}

/// A driver's own checkpoint keeps the coupling the validator now checks.
///
/// The control the clause needs, and the one that would catch it being stated
/// backwards. Every record this suite feeds back through the join is one a
/// driver produced, so if the invariant were not maintained by construction the
/// refusal would be turning away ordinary output rather than damage.
///
/// **The endpoint offset is the one that must be there**, and this is where that
/// half of the derivation is checked against the real lifecycle rather than
/// argued: adoption publishes an endpoint fact unconditionally, so a driver that
/// holds a group has one whatever else it has done.
#[test]
fn a_driver_that_has_observed_a_configuration_records_the_endpoint_offset() {
    let (driver, _transport) = driver_holding(None, &[1, 2, 3]);

    let produced = driver.control_plane_checkpoint();
    assert!(
        produced.committed_id_high_water.is_some(),
        "adoption observed a committed configuration, so it raised the mark"
    );
    assert!(
        produced.committed_endpoint_through.is_some(),
        "and advanced the endpoint offset in the same call: {produced:?}"
    );

    // And the empty record keeps the other side of the biconditional.
    let empty = PeerControlPlaneCheckpoint::<u64>::empty(GROUP);
    assert!(empty.committed_id_high_water.is_none());
    assert!(empty.live_committed_members.is_empty());
    assert!(empty.pending_fences.is_empty());
    assert!(empty.committed_crossings_through.is_none());
    assert!(empty.committed_endpoint_through.is_none());
}

/// A full retirement record with an endpoint offset and no crossing offset is
/// accepted.
///
/// **The shape the split exists to represent, and the one a symmetric invariant
/// would have refused.** A process that recovered from a snapshot folded the
/// boundary configuration at its commit index and no configuration entry at all,
/// so it honestly has an endpoint offset, a mark, a live set — and nothing to
/// put in the crossing offset. A first incarnation over a fresh cluster produces
/// the same shape for the same reason.
///
/// Refusing it would make every such process unable to restart, and accepting it
/// is only safe because the crossing offset it lacks is exactly the one that
/// must not be invented: leaving it `None` is what lets a later recovery replay
/// the crossings this record never saw.
#[test]
fn an_endpoint_only_record_is_a_record_a_driver_produces() {
    let (driver, _transport) = driver_holding(Some(3), &[1, 2, 3]);
    let group = driver.release_group().expect("the driver holds a group");

    driver
        .adopt_group_with_checkpoint(
            group,
            Vec::new(),
            checkpoint_through(Some(3), &[1, 2, 3], &[], None, Some(LogIndex(9))),
        )
        .expect("a snapshot-recovered record has an endpoint and no crossings");

    let settled = driver.control_plane_checkpoint();
    assert_eq!(
        settled.committed_endpoint_through,
        Some(LogIndex(9)),
        "the endpoint offset joined by max like any other position"
    );
    assert_eq!(
        settled.committed_crossings_through,
        Some(CONSUMED_THROUGH),
        "and the crossing offset kept what this driver had earned, because the \
         incoming record contributed none"
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
