//! Joining a recovered checkpoint into a driver, and what a join must never do.
//!
//! A checkpoint is allowed to be behind the runtime it is restored beside — the
//! public contract says so, and bounds what that costs — so a *valid* record that
//! predates a removal is an ordinary input rather than a corruption. The join
//! therefore has to be a lattice join and not three independent merges: taking
//! the union of the two live sets makes a record's memory of a since-removed
//! replica outrank the removal the driver actually watched, and the identity the
//! cluster consumed becomes adoptable again.
//!
//! The properties the join is written to have — symmetric, monotone in
//! spent-ness, and order-free *along one chain* — are argued at the
//! crate-internal `restore_checkpoint`. This file is the evidence for them, plus
//! the validation that keeps a damaged record from being absorbed in part.
//!
//! **Order-freedom is claimed over one chain and no wider, and that narrowing is
//! the eleventh round's.** It used to be claimed over arbitrary records, and was
//! only ever checked against mutually reconcilable ones. It is false for records
//! that conflict: the register keeps one positioned observation, so two records
//! that contradict each other are compared only while the register still stands
//! where they do, and any later record moves it past them. `(A∨B)∨C` refuses at
//! the collision while `A∨(B∨C)` never sees it.
//!
//! What replaces the wider claim is a rule on the *input*: a record standing
//! before what the driver already observed is refused, because along one chain it
//! cannot arise and across a fork it is exactly that laundering vector. The
//! supervisor owns chain identity; a record from before this driver existed goes
//! to the constructor, which restores into empty state. The chain property that
//! survives is the one an embedder depends on — a crash loses a suffix of its own
//! writes, and which prefix of its chain it kept does not change where it
//! settles.

#![allow(clippy::wildcard_imports)]

mod support;

use std::collections::BTreeSet;

use rafter_service::{
    ControlPlaneCheckpointError, CurrentCommittedState, ManagedDriverError,
    PeerControlPlaneCheckpoint, TransportDriverOptions,
};
use support::scripted::*;
use support::transport::*;
use support::*;

fn ids(node_ids: &[u64]) -> BTreeSet<NodeId> {
    node_ids.iter().copied().map(NodeId).collect()
}

/// Where a hand-built record observed the committed membership.
///
/// **Above the scripted runtime's commit index on purpose**, which is the
/// inversion the chain rule forces. A driver's register ends up at the later of
/// its record and its runtime, so a record at or after this position is the one a
/// mid-life adoption may still offer — and every adoption's endpoint publication
/// is then the older observation, which contributes nothing and leaves these
/// cases measuring the identity lattice rather than the register's ordering.
const OBSERVED_AT: LogIndex = LogIndex(9);

/// Where a record offered *after* one at [`OBSERVED_AT`] stands.
///
/// The next link of one chain. Records at one position meet the contradiction
/// arm, which is a different rule and has its own cases.
const LATER_AT: LogIndex = LogIndex(12);

/// A checkpoint built by hand, the way a durable file hands one back.
///
/// **The current state travels with the retirement record**, because they are
/// one record and a driver never writes either without the other. A helper that
/// left it out would be building a shape the validator now refuses — which is
/// the point of `a_record_that_separates_retirement_from_its_current_state_is_refused`
/// below, and not something every other case here should be quietly asserting.
fn checkpoint(mark: Option<u64>, live: &[u64]) -> PeerControlPlaneCheckpoint<u64> {
    checkpoint_at(mark, live, Some(OBSERVED_AT))
}

/// The same, with the observation's position chosen by the caller.
///
/// `None` builds a record with no current state at all, which is the shape the
/// coupling biconditional refuses beside any retirement state.
fn checkpoint_at(
    mark: Option<u64>,
    live: &[u64],
    through: Option<LogIndex>,
) -> PeerControlPlaneCheckpoint<u64> {
    let mut checkpoint = PeerControlPlaneCheckpoint::empty(GROUP);
    checkpoint.committed_id_high_water = mark.map(NodeId);
    checkpoint.current_committed =
        through.map(|through| CurrentCommittedState::new(through, ids(live)));
    checkpoint
}

/// The membership a settled record calls live.
fn live_of(checkpoint: &PeerControlPlaneCheckpoint<u64>) -> BTreeSet<NodeId> {
    checkpoint
        .current_committed
        .as_ref()
        .map(|current| current.membership.clone())
        .unwrap_or_default()
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
/// A publication naming a replica this directory cannot resolve is withheld
/// whole, which is how a test reaches a driver whose link layer is behind it
/// without arranging a transport refusal.
fn driver_holding_named(
    mark: Option<u64>,
    live: &[u64],
    nameable: Nameable,
) -> (ScriptedDriver, QueueTransport) {
    let record = match mark {
        Some(mark) => checkpoint(Some(mark), live),
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
        checkpoint(Some(5), &[1, 2, 5]),
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
        !live_of(&driver.control_plane_checkpoint()).contains(&NodeId(5)),
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
        checkpoint(Some(5), &[1, 2]),
    );

    assert!(
        matches!(
            refused,
            Err(ManagedDriverError::RetiredNodeId { node_id: NodeId(5) })
        ),
        "spent-ness from either side sticks, got {refused:?}"
    );
}

/// One chain settles the same wherever its own prefix was lost.
///
/// **The order-freedom claim, narrowed to what is true and to what an embedder
/// depends on.** A replica's records form a chain — each incarnation is handed
/// the previous record before it observes anything — and a crash can lose any
/// suffix of its own writes. So the question that matters is not "any order" but
/// "which links survived": a supervisor that persisted `R1, R2, R3` and one that
/// lost `R2` must settle on the same record, and the one that lost `R2` must
/// still spend everything `R2` witnessed.
///
/// That is the property the register buys and the union could not. `R2` names
/// nodes 3 and 4 nowhere and stands at 10; `R3` names them nowhere either and
/// stands at 12. A driver that skipped straight from `R1` to `R3` infers both
/// removals from the pair it does have, because being named at 7 and absent at 12
/// is a committed removal whichever record in between it never saw.
///
/// The runtime stands at commit 0 naming `{1}`, beneath every record and inside
/// every one of them, so the adoption publication contributes no inference of its
/// own and this is measuring the join.
#[test]
fn one_chain_settles_the_same_whatever_prefix_survived() {
    // One replica's own chain: `{1,2,3,4}` at 7, then 3 and 4 out and 5 in at 10,
    // then 2 and 5 out and 6 in at 12. Each record's mark covers everything the
    // previous ones named, which is what makes it a chain rather than a fork.
    let chain = [
        checkpoint_at(Some(4), &[1, 2, 3, 4], Some(LogIndex(7))),
        checkpoint_at(Some(5), &[1, 2, 5], Some(LogIndex(10))),
        checkpoint_at(Some(6), &[1, 6], Some(LogIndex(12))),
    ];
    let surviving_prefixes: [&[usize]; 4] = [&[0, 1, 2], &[0, 2], &[1, 2], &[2]];

    let mut settled: Option<PeerControlPlaneCheckpoint<u64>> = None;
    for surviving in surviving_prefixes {
        let (driver, transport) = scripted_driver_with_checkpoint(
            ScriptedMembershipRuntime::for_node_at(NodeId(1), &[1], &[1], LogIndex(0)),
            Nameable::all(),
            &[NodeId(2), NodeId(3), NodeId(4), NodeId(5), NodeId(6)],
            TransportDriverOptions::default(),
            PeerControlPlaneCheckpoint::empty(GROUP),
        );
        for index in surviving {
            let group = driver.release_group().expect("the driver holds a group");
            driver
                .adopt_group_with_checkpoint(group, Vec::new(), chain[*index].clone())
                .expect("each record of one chain is valid for this group");
        }

        let reached = driver.control_plane_checkpoint();
        assert!(
            transport.retires(NodeId(2)),
            "node 2 was committed at 10 and absent at 12, so it is retired \
             however much of the chain survived: {surviving:?} left the link \
             layer holding {:?}",
            transport.policies().last()
        );
        match &settled {
            None => settled = Some(reached),
            Some(first) => assert_eq!(
                &reached, first,
                "the surviving prefix {surviving:?} settled somewhere else"
            ),
        }
    }

    let settled = settled.expect("four prefixes were run");
    assert_eq!(settled.committed_id_high_water, Some(NodeId(6)));
    assert_eq!(live_of(&settled), ids(&[1, 6]));
}

/// A record standing before what this driver already observed is refused, and the
/// constructor is where it still belongs.
///
/// **The chain rule and its cost, in one case.** The record here is valid and
/// carries a mark of 9 that the driver has no other source for — a
/// snapshot-derived incarnation observes the boundary configuration and nothing
/// beneath it, so an *earlier* record genuinely can know a retirement a later one
/// does not. Refusing it mid-life is therefore a real capability given up, not a
/// no-op, and the second half of this case is where it went: restored into empty
/// held state by the constructor, the same record contributes everything it
/// witnessed.
///
/// What the refusal buys is that a record can never merge against a position it
/// never saw. That is the only way the register — which keeps one observation —
/// can be kept from laundering a fork, and the supervisor owns the chain identity
/// the rule assumes.
#[test]
fn a_record_from_before_this_drivers_own_observation_is_refused() {
    let (driver, transport) = driver_holding(Some(3), &[1, 2, 3]);
    let before = driver.control_plane_checkpoint();
    let policies_before = transport.policies();
    let group = driver.release_group().expect("the driver holds a group");

    let earlier = checkpoint_at(Some(9), &[1, 2, 3], Some(LogIndex(2)));
    let refused = driver.adopt_group_with_checkpoint(group, Vec::new(), earlier.clone());

    assert!(
        matches!(
            refused,
            Err(ManagedDriverError::InvalidControlPlaneCheckpoint {
                reason: ControlPlaneCheckpointError::StaleCurrentState {
                    held: OBSERVED_AT,
                    incoming: LogIndex(2),
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
        transport.policies(),
        policies_before,
        "and the link layer was told nothing on the way to the refusal"
    );

    // And the supported path for exactly this record: a driver built around it,
    // which restores into empty held state and keeps the mark it carries.
    let (opened, _fresh_transport) = try_scripted_driver_with_checkpoint(
        ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]),
        Nameable::all(),
        &[NodeId(2), NodeId(3)],
        earlier,
    );
    let opened = opened.expect("the constructor restores a record into empty held state");
    assert_eq!(
        opened.control_plane_checkpoint().committed_id_high_water,
        Some(NodeId(9)),
        "so the retirement that record witnessed is not lost, only relocated"
    );
}

/// Joining the same record twice changes nothing.
///
/// Idempotence is what lets an embedder re-read its file after a partial
/// recovery without reasoning about whether it already applied it.
#[test]
fn joining_the_same_record_twice_is_the_same_as_once() {
    let (driver, _transport) = driver_holding_named(None, &[1, 2], Nameable::only(&[NodeId(2)]));
    let record = checkpoint(Some(5), &[1, 2]);

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
/// The clause that keeps the join from over-retiring. The driver's own record has
/// a mark of 2, so it has no opinion about node 5 at all — it never saw the
/// configuration that admitted it — and its silence must not be read as evidence
/// that node 5 was removed. The later record's mark of 5 is what gives anything
/// an opinion, and it names node 5 live.
#[test]
fn an_identity_above_a_records_mark_is_not_spent_by_that_record() {
    let (driver, _transport) = driver_holding(Some(2), &[1, 2]);
    let group = driver.release_group().expect("the driver holds a group");
    driver
        .adopt_group_with_checkpoint(
            group,
            Vec::new(),
            checkpoint_at(Some(5), &[1, 2, 5], Some(LATER_AT)),
        )
        .expect("the next record of one chain is valid");

    let settled = driver.control_plane_checkpoint();
    assert_eq!(settled.committed_id_high_water, Some(NodeId(5)));
    assert!(
        live_of(&settled).contains(&NodeId(5)),
        "the held record's mark of 2 gives it no opinion about node 5, so its \
         silence is not a removal: {:?}",
        live_of(&settled)
    );
}

/// Two honest records jointly prove a removal neither one witnessed.
///
/// **The reviewer's counterexample, and the missing fact is *between* the
/// records.** The older one stands at endpoint 7 and says node 5 was live there;
/// the later one stands at endpoint 10 and says the committed membership is
/// `{1,2,3}` — it is snapshot-derived, so the removal that took node 5 out
/// happened below its boundary and it has no record of it. Neither record is
/// damaged and neither alone is evidence: the older one's mark is 5 with node 5
/// live, so it does not spend it, and the later one's mark is 3, so it has no
/// opinion about node 5 at all.
///
/// Together they do prove it. Node 5 was in the committed membership at
/// position 7 and is not in the committed membership at position 10, and a
/// committed configuration is permanent — so a committed removal happened
/// between them.
///
/// Treating the current membership as a grow-only set loses exactly that. The
/// union keeps node 5 live under the joined mark of 5, which leaves it unspent
/// and still authorized, and the joined endpoint of 10 then makes the runtime's
/// own index-10 publication look already-consumed so no fold ever runs.
#[test]
fn two_records_that_jointly_prove_a_removal_spend_the_identity() {
    // A directory that can name every replica, so the policy the pair licenses
    // actually reaches the link layer and the assertion is about what was
    // published rather than about what was derived.
    let (driver, transport) = driver_holding(None, &[1, 2, 3]);
    let group = driver.release_group().expect("the driver holds a group");
    driver
        .adopt_group_with_checkpoint(
            group,
            Vec::new(),
            checkpoint_at(Some(5), &[1, 2, 3, 5], Some(LogIndex(7))),
        )
        .expect("an older honest record");

    let group = driver.release_group().expect("the driver holds a group");
    driver
        .adopt_group_with_checkpoint(
            group,
            Vec::new(),
            checkpoint_at(Some(3), &[1, 2, 3], Some(LogIndex(10))),
        )
        .expect("a later honest snapshot-derived record");

    let settled = driver.control_plane_checkpoint();
    assert!(
        !live_of(&settled).contains(&NodeId(5)),
        "node 5 was live at position 7 and absent at position 10, which is a \
         committed removal: {:?}",
        live_of(&settled)
    );
    assert_eq!(
        settled.committed_id_high_water,
        Some(NodeId(5)),
        "and the mark still covers it, so the spent test can see it"
    );
    assert!(
        transport.retires(NodeId(5)),
        "and the policy this driver publishes retires it: {:?}",
        transport.policies().last()
    );
}

/// A checkpoint from another group is refused, and installs nothing.
#[test]
fn a_checkpoint_from_another_group_is_refused() {
    let (driver, _transport) = driver_holding(Some(3), &[1, 2, 3]);
    let before = driver.control_plane_checkpoint();
    let group = driver.release_group().expect("the driver holds a group");

    let mut foreign = checkpoint(Some(9), &[1, 2, 3, 9]);
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
/// **Two clauses where there were four**, and the two that left were both about
/// the obligation ledger: a fence naming a live member, and a fence naming an
/// identity the record never saw spent. Neither is expressible now — a record
/// carries a mark and a current state, and retirement is derived from them — so
/// the validator states what is left, which is the coupling between the two and
/// the mark covering every live identity.
///
/// Both hold by construction for a record a driver produced, so each failure
/// means the durable bytes were damaged, and each one lowers a retirement record
/// in the direction that un-retires an identity.
#[test]
fn a_contradictory_checkpoint_is_refused_and_installs_nothing() {
    let cases: [(PeerControlPlaneCheckpoint<u64>, ControlPlaneCheckpointError); 2] = [
        (
            checkpoint(None, &[1, 2]),
            ControlPlaneCheckpointError::CurrentStateWithoutRetirement,
        ),
        (
            checkpoint(Some(2), &[1, 2, 5]),
            ControlPlaneCheckpointError::LiveMemberAboveMark {
                node_id: NodeId(5),
                mark: NodeId(2),
            },
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
/// The assertion is about *when* rather than only about what. Validation runs
/// before the first field moves and therefore before any derivation reaches the
/// transport, which is what makes a damaged file a refusal to open rather than a
/// replica this process has already helped destroy.
///
/// The stakes changed shape and did not go away. The permanent statement used to
/// be a per-principal fence; it is the retirement floor now, and a floor raised
/// from a damaged record is exactly as uninvertible — every identity beneath it
/// that the peer set does not name is refused for the life of the group.
#[test]
fn a_damaged_record_is_refused_before_any_transport_call() {
    let (driver, transport) = driver_holding(Some(7), &[1, 2, 7]);
    let before = driver.control_plane_checkpoint();
    let policies_before = transport.policies();
    let group = driver.release_group().expect("the driver holds a group");

    let refused =
        driver.adopt_group_with_checkpoint(group, Vec::new(), checkpoint(Some(2), &[1, 2, 7]));

    assert!(
        matches!(
            refused,
            Err(ManagedDriverError::InvalidControlPlaneCheckpoint {
                reason: ControlPlaneCheckpointError::LiveMemberAboveMark {
                    node_id: NodeId(7),
                    mark: NodeId(2),
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
        transport.policies(),
        policies_before,
        "and the link layer was told nothing on the way to the refusal"
    );
    assert!(
        !transport.retires(NodeId(7)),
        "node 7 is live in this driver's own committed configuration"
    );
}

/// A record that separates retirement from its current state is refused, both
/// ways round.
///
/// The mark and the current state are one record. Each half is individually
/// well-formed here — every clause the other cases check still holds — and each
/// pair is a state no driver produces, because every observation of a committed
/// configuration raises the mark and assigns the current state in the same call.
///
/// The directions fail differently and an operator needs to know which. A mark
/// with nothing to read it against spends every identity at or below it, so the
/// driver refuses the whole cluster. A current state with no mark is a record
/// whose retirement half was truncated away, so every identity the lost facts
/// spent is allocatable again and the floor this driver publishes stops covering
/// any of them.
#[test]
fn a_record_that_separates_retirement_from_its_current_state_is_refused() {
    let cases: [(PeerControlPlaneCheckpoint<u64>, ControlPlaneCheckpointError); 3] = [
        (
            checkpoint_at(Some(3), &[1, 2, 3], None),
            ControlPlaneCheckpointError::RetirementWithoutCurrentState,
        ),
        // A mark with no live set at all is still retirement state, so it needs a
        // current state like any other: read against nothing, it spends the
        // cluster.
        (
            checkpoint_at(Some(5), &[], None),
            ControlPlaneCheckpointError::RetirementWithoutCurrentState,
        ),
        // `LogIndex(0)` is a real position rather than an absence, which is why
        // the current state is an `Option` — so this is the same separation and
        // not a zero standing in for `None`.
        (
            checkpoint_at(None, &[], Some(LogIndex(0))),
            ControlPlaneCheckpointError::CurrentStateWithoutRetirement,
        ),
    ];

    for (damaged, expected) in cases {
        let (driver, transport) = driver_holding(Some(3), &[1, 2, 3]);
        let before = driver.control_plane_checkpoint();
        let policies_before = transport.policies();
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
            transport.policies(),
            policies_before,
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
/// **Both halves arrive together or neither does**, and this is where that is
/// checked against the real lifecycle rather than argued: adoption publishes a
/// committed fact unconditionally, so a driver that holds a group has a mark and
/// a current state whatever else it has done.
#[test]
fn a_driver_that_has_observed_a_configuration_records_its_current_state() {
    let (driver, _transport) = driver_holding(None, &[1, 2, 3]);

    let produced = driver.control_plane_checkpoint();
    assert!(
        produced.committed_id_high_water.is_some(),
        "adoption observed a committed configuration, so it raised the mark"
    );
    assert!(
        produced.current_committed.is_some(),
        "and assigned the current state in the same call: {produced:?}"
    );

    // And the empty record keeps the other side of the biconditional.
    let empty = PeerControlPlaneCheckpoint::<u64>::empty(GROUP);
    assert!(empty.committed_id_high_water.is_none());
    assert!(empty.current_committed.is_none());
}

/// The later of two current states wins, and the earlier is not merged into it.
///
/// **The rule that replaced two consumer offsets.** A record that looked at
/// position 9 is believed over one that looked at position 1, whatever each
/// names, because "who is committed now" is an answer that was true somewhere
/// rather than a fact to accumulate. The incoming record here is the
/// snapshot-recovered shape — it observed the boundary configuration at its
/// commit index and no configuration entry at all — and it is an ordinary input
/// rather than a special case.
#[test]
fn the_later_of_two_current_states_is_the_one_that_is_believed() {
    let (driver, _transport) = driver_holding(Some(3), &[1, 2, 3]);
    let group = driver.release_group().expect("the driver holds a group");

    driver
        .adopt_group_with_checkpoint(
            group,
            Vec::new(),
            checkpoint_at(Some(3), &[1, 2, 3], Some(LATER_AT)),
        )
        .expect("a snapshot-recovered record is an ordinary input");

    let settled = driver.control_plane_checkpoint();
    let current = settled
        .current_committed
        .as_ref()
        .expect("the driver holds a current state");
    assert_eq!(
        current.through, LATER_AT,
        "the later observation is the one this driver now holds"
    );
    assert_eq!(current.membership, ids(&[1, 2, 3]));
}

/// Two records that disagree at one position are refused rather than merged.
///
/// The committed membership at one log index is one set, so this is two claims
/// about a single fact and not two observations to reconcile. Picking either
/// would be choosing which record to believe with nothing to decide on; merging
/// them would invent a third that neither record ever held, and a merged live
/// set is exactly how a removal gets lost.
///
/// **The pair has to be one neither side's spent-ness explains**, which is what
/// the normalization ahead of the comparison forces. The incoming record's mark
/// is 2, so it has never seen node 5 in any committed configuration and has no
/// opinion to filter with; the driver calls node 5 live at that same position.
/// One of them is wrong about the raw fact, and nothing here can decide which.
///
/// A record that omits node 5 while its own mark *covers* it is the other case
/// entirely — that record witnessed a removal — and
/// `a_stale_checkpoint_cannot_un_spend_a_retired_identity` is where it lands.
#[test]
fn two_records_that_disagree_at_one_position_are_refused() {
    let (driver, transport) = driver_holding(Some(5), &[1, 2, 5]);
    let before = driver.control_plane_checkpoint();
    let standing_at = before
        .current_committed
        .as_ref()
        .expect("the driver holds a current state")
        .through;
    let policies_before = transport.policies();
    let group = driver.release_group().expect("the driver holds a group");

    let refused = driver.adopt_group_with_checkpoint(
        group,
        Vec::new(),
        checkpoint_at(Some(2), &[1, 2], Some(standing_at)),
    );

    assert!(
        matches!(
            refused,
            Err(ManagedDriverError::InvalidControlPlaneCheckpoint {
                reason: ControlPlaneCheckpointError::ContradictoryCurrentState { through }
            }) if through == standing_at
        ),
        "got {refused:?}"
    );
    assert_eq!(
        driver.control_plane_checkpoint(),
        before,
        "a refused record moves nothing"
    );
    assert_eq!(
        transport.policies(),
        policies_before,
        "and the link layer was told nothing on the way to the refusal"
    );
}

/// A record whose omission its own mark explains is a removal, not a
/// contradiction.
///
/// The control for the clause above, and the line the normalization draws. This
/// record stands where the driver stands and omits node 5 — but its mark is 5,
/// so the omission *is* its report that a committed removal spent the identity.
/// Refusing it would turn the commonest legitimate stale record into a file this
/// process cannot open.
#[test]
fn a_record_whose_mark_explains_its_omission_is_a_removal() {
    let (driver, transport) = driver_holding(Some(5), &[1, 2, 5]);
    let standing_at = driver
        .control_plane_checkpoint()
        .current_committed
        .expect("the driver holds a current state")
        .through;
    let group = driver.release_group().expect("the driver holds a group");

    driver
        .adopt_group_with_checkpoint(
            group,
            Vec::new(),
            checkpoint_at(Some(5), &[1, 2], Some(standing_at)),
        )
        .expect("a record that witnessed a removal the driver had not");

    assert!(
        !live_of(&driver.control_plane_checkpoint()).contains(&NodeId(5)),
        "the witnessed removal wins over the driver's own older reading"
    );
    assert!(
        transport.retires(NodeId(5)),
        "and the policy this driver publishes retires it: {:?}",
        transport.policies().last()
    );
}

/// Three records no one chain produces are refused in every order, and no order
/// retires a member the latest of them names live.
///
/// **The eleventh reviewer's counterexample.** Each record is individually valid.
/// `A {through 1, mark 2, {1,2}}` and `B {through 1, mark 3, {1,3}}` stand at one
/// position and disagree about it; `C {through 2, mark 3, {1,2,3}}` stands later
/// and names node 2 live.
///
/// `(A∨B)∨C` refuses at the first step, because the collision is right there.
/// `A∨(B∨C)` never compares A against B at all: `B∨C` moves the register to
/// position 2 — spending node 2 on the way, since B's mark covers it and B's
/// membership omits it — and A then merges against a register that has already
/// left A's position behind. One order refuses; the other settles on a record
/// that permanently retires a replica the latest record calls live.
///
/// The register keeps one observation, so the equal-position check can only ever
/// fire while the register still stands where the incoming record does. That
/// makes contradiction detection order-dependent by construction, and no rule
/// short of keeping per-position history restores it. What is withdrawn instead
/// is the input: one authoritative chain per replica, and a record older than
/// what this driver holds is refused rather than merged.
///
/// **What is asserted is that every order refuses, and not that no order retires
/// node 2.** The stronger claim is unreachable and the reason is worth having in
/// the file: `B` on its own is a valid single record whose mark covers node 2 and
/// whose membership omits it, so `B` *is* a witnessed removal of node 2. Refusing
/// that would be refusing a legal record for saying what it saw. What the chain
/// rule guarantees is that no *sequence* of these three settles — the fork is
/// always found, at a collision or at a position it cannot merge across.
#[test]
fn a_forked_set_of_records_is_refused_in_every_order() {
    // `A` and `B` collide at position 1; `C` stands at 2 and readmits node 2.
    let forked = [
        checkpoint_at(Some(2), &[1, 2], Some(LogIndex(1))),
        checkpoint_at(Some(3), &[1, 3], Some(LogIndex(1))),
        checkpoint_at(Some(3), &[1, 2, 3], Some(LogIndex(2))),
    ];
    let orders = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];

    for order in orders {
        // A runtime standing at position 0 over `{1}`, which every record names:
        // it can neither win a comparison nor infer a removal, so the orders are
        // measuring the join and nothing else.
        let (driver, _transport) = scripted_driver_with_checkpoint(
            ScriptedMembershipRuntime::for_node_at(NodeId(1), &[1], &[1], LogIndex(0)),
            Nameable::all(),
            &[NodeId(2), NodeId(3)],
            TransportDriverOptions::default(),
            PeerControlPlaneCheckpoint::empty(GROUP),
        );

        let mut refused = None;
        for index in order {
            let group = driver.release_group().expect("the driver holds a group");
            let outcome =
                driver.adopt_group_with_checkpoint(group, Vec::new(), forked[index].clone());
            if let Err(error) = outcome {
                refused = Some(error);
                break;
            }
        }

        assert!(
            matches!(
                refused,
                Some(ManagedDriverError::InvalidControlPlaneCheckpoint { .. })
            ),
            "order {order:?} settled instead of refusing: {refused:?}"
        );
    }
}

/// The control: a record that saw further still contributes everything it knows.
///
/// Without it, a join that refused every record whose reading differs from the
/// driver's would pass every clause above and lose the whole point of the
/// checkpoint. This one stands exactly where the driver does and agrees about the
/// membership there, so the chain rule and the contradiction arm both let it
/// through — and its mark reaches past anything the driver has seen, which is the
/// retirement it exists to carry.
#[test]
fn a_record_that_saw_further_contributes_the_retirement_it_witnessed() {
    let (driver, transport) = driver_holding(Some(3), &[1, 2, 3]);
    let group = driver.release_group().expect("the driver holds a group");

    driver
        .adopt_group_with_checkpoint(group, Vec::new(), checkpoint(Some(9), &[1, 2, 3]))
        .expect("a valid record from a process that saw further");

    let settled = driver.control_plane_checkpoint();
    assert_eq!(settled.committed_id_high_water, Some(NodeId(9)));
    assert!(
        transport.retires(NodeId(9)),
        "the retirement the other process witnessed is now this driver's, and it \
         published it: {:?}",
        transport.policies().last()
    );
}
