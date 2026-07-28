//! One merge of two committed-membership observations, wherever the pair comes
//! from.
//!
//! A driver decides "which of these two observations of the current committed
//! membership do I believe" in four places: joining two checkpoints, opening a
//! checkpoint against the adopted runtime's own endpoint, folding a routed
//! `CommittedEndpoint`, and folding a crossing that advances the register. They
//! were four expressions of one rule, and only the first refused a pair standing
//! at one position and disagreeing about it — the other three let the incoming
//! fact win a tie, which silently retires a live replica in one direction and
//! silently authorizes a never-committed one in the other.
//!
//! The committed membership at one log position is one set. Two observations of
//! it that still disagree after every identity either side has proven spent is
//! removed are not two readings to reconcile; they are two claims about a single
//! fact, and this driver refuses to open rather than pick one.
//!
//! The carve-out is the readmission. A cluster that names a spent identity again
//! has broken the single-use contract, and this driver already has an answer for
//! that — refuse the replica, count the violation. That is not storage
//! corruption and must not be reported as it.

#![allow(clippy::wildcard_imports)]

mod support;

use std::collections::BTreeSet;

use rafter_service::{
    ControlPlaneCheckpointError, CurrentCommittedState, DriverServiceState, ManagedDriverError,
    PeerControlPlaneCheckpoint,
};
use support::scripted::*;
use support::transport::*;
use support::*;

/// Where every record and every runtime in this file stands.
///
/// One position for both sides, because that is the whole subject: a tie is the
/// only case the four sites answered differently.
const AT: LogIndex = LogIndex(10);

fn ids(node_ids: &[u64]) -> BTreeSet<NodeId> {
    node_ids.iter().copied().map(NodeId).collect()
}

/// A record standing at [`AT`] with `live` as the membership it observed there.
fn record(mark: u64, live: &[u64]) -> PeerControlPlaneCheckpoint<u64> {
    let mut checkpoint = PeerControlPlaneCheckpoint::empty(GROUP);
    checkpoint.committed_id_high_water = Some(NodeId(mark));
    checkpoint.current_committed = Some(CurrentCommittedState::new(AT, ids(live)));
    checkpoint
}

/// A runtime reporting `committed` as its committed membership at [`AT`].
fn runtime(committed: &[u64]) -> ScriptedMembershipRuntime {
    ScriptedMembershipRuntime::for_node_at(NodeId(1), committed, committed, AT)
}

/// The membership a settled driver calls live.
fn live_of(driver: &ScriptedDriver) -> BTreeSet<NodeId> {
    driver
        .control_plane_checkpoint()
        .current_committed
        .map(|current| current.membership)
        .unwrap_or_default()
}

/// A record and a runtime that disagree at one position refuse to open, in both
/// directions.
///
/// **The tenth reviewer's second P1, and the two directions fail differently
/// enough that one case cannot stand for both.**
///
/// The record naming *more* than the runtime is read as a removal: the runtime
/// wins the tie, the difference becomes a committed removal that nothing
/// committed, and the replica it names is retired for good.
///
/// The record naming *less* is read as an admission: the runtime wins the tie
/// again, the mark rises past an identity this record says was never committed,
/// and a replica the durable record has no evidence for is authorized.
///
/// Neither is a reconciliation. The committed membership at position 10 is one
/// set, and no identity's proven spent-ness explains either gap — in the first
/// case node 3 is live in the record, so the record does not spend it; in the
/// second the record's mark is 2, so it has never seen node 3 at all and has no
/// opinion to filter with.
#[test]
fn a_record_and_a_runtime_that_disagree_at_one_position_refuse_to_open() {
    let cases = [
        // The record names node 3 live at 10; the runtime does not.
        (record(3, &[1, 2, 3]), runtime(&[1, 2])),
        // The record has never seen node 3 committed at all; the runtime says it
        // is committed at the very position the record observed.
        (record(2, &[1, 2]), runtime(&[1, 2, 3])),
    ];

    for (durable, rebuilt) in cases {
        let (opened, transport) = try_scripted_driver_with_checkpoint(
            rebuilt,
            Nameable::all(),
            &[NodeId(2), NodeId(3)],
            durable.clone(),
        );

        let Err(ManagedDriverError::InvalidControlPlaneCheckpoint { reason }) = opened else {
            panic!("expected a refusal to open over {durable:?}, got {opened:?}");
        };
        assert_eq!(
            reason,
            ControlPlaneCheckpointError::ContradictoryCurrentState { through: AT }
        );
        assert!(
            transport.policies().is_empty(),
            "no policy may be published while the two licensing inputs disagree: \
             {:?}",
            transport.policies()
        );
        assert_eq!(
            transport.retirement_floor(),
            None,
            "and no retirement floor either, which is the half that never falls"
        );
    }
}

/// Adoption over a runtime that contradicts the record refuses, installs no
/// group, and tells the link layer nothing.
///
/// The adoption half of the same rule, and the one where "refuses" has to mean
/// more than "returns an error": a driver that had already installed the group
/// would keep serving clients from a replica whose licensing inputs contradict
/// each other, and the peer set it publishes for that group is the permanent
/// statement this refusal exists to withhold.
///
/// The record offered here agrees with what the driver already holds, so the
/// checkpoint join has nothing to say. The disagreement is with the *runtime*
/// the supervisor rebuilt, which is the site a record-versus-record refusal
/// never covered.
#[test]
fn adoption_over_a_contradicted_runtime_refuses_and_installs_nothing() {
    let (driver, transport) = scripted_driver_with_checkpoint(
        runtime(&[1, 2, 3]),
        Nameable::all(),
        &[NodeId(2), NodeId(3)],
        rafter_service::TransportDriverOptions::default(),
        record(3, &[1, 2, 3]),
    );
    let before = driver.control_plane_checkpoint();
    let sets_before = transport.peer_sets();
    let _ = driver.release_group().expect("the driver holds a group");

    let refused = driver.adopt_group_with_checkpoint(
        RaftGroup::new(
            GROUP,
            NodeId(1),
            runtime(&[1, 2]),
            KvStateMachine::default(),
        ),
        Vec::new(),
        record(3, &[1, 2, 3]),
    );

    let Err(ManagedDriverError::InvalidControlPlaneCheckpoint { reason }) = refused else {
        panic!("expected a refusal to adopt, got {refused:?}");
    };
    assert_eq!(
        reason,
        ControlPlaneCheckpointError::ContradictoryCurrentState { through: AT }
    );
    assert_eq!(
        driver.control_plane_checkpoint(),
        before,
        "a refused adoption moves no control-plane state"
    );
    assert_eq!(
        transport.peer_sets(),
        sets_before,
        "and publishes nothing on the way to the refusal"
    );
    assert_eq!(
        driver.service_state(),
        DriverServiceState::Released,
        "and installs no group, so the driver is holding none"
    );
}

/// A readmitted spent identity is a counted violation and not a contradiction.
///
/// **The carve-out, and the regression that keeps the refusal from swallowing
/// it.** The record stands at 10 with node 3 already spent — mark 3, live
/// `{1,2}` — and the runtime reports `{1,2,3}` committed at exactly that
/// position. That is the single-use contract broken by the cluster, which this
/// driver has always had an answer for: keep the identity refused, keep it out
/// of the peer set, and count it.
///
/// Read as a disagreement about the membership at position 10 it would be a
/// refusal to open, which turns a cluster's configuration bug into a replica
/// that will not start. So the comparison normalizes both sides by what either
/// has proven spent first, and only what survives that is a contradiction.
#[test]
fn a_readmitted_spent_identity_is_counted_rather_than_refused() {
    let (driver, transport) = scripted_driver_with_checkpoint(
        runtime(&[1, 2, 3]),
        Nameable::all(),
        &[NodeId(2), NodeId(3)],
        rafter_service::TransportDriverOptions::default(),
        record(3, &[1, 2]),
    );

    assert_eq!(
        live_of(&driver),
        ids(&[1, 2]),
        "the identity a committed removal spent stays spent, whatever the \
         cluster names again"
    );
    assert_eq!(
        driver.readmitted_retired_peers(),
        1,
        "and the violation is countable rather than absorbed"
    );
    assert!(
        !transport
            .peer_sets()
            .last()
            .expect("a set was published")
            .iter()
            .any(|principal| principal_node(principal) == Some(NodeId(3))),
        "and node 3 is not published to the link layer: {:?}",
        transport.peer_sets().last()
    );
}

/// The agreeing tie is an ordinary input.
///
/// The control. A record and a runtime that observed position 10 and agree about
/// it must open, or the refusal above would be turning away the commonest
/// restart there is: a process that persisted its checkpoint at the same point
/// its runtime last committed.
#[test]
fn a_record_and_a_runtime_that_agree_at_one_position_open() {
    let (driver, transport) = scripted_driver_with_checkpoint(
        runtime(&[1, 2, 3]),
        Nameable::all(),
        &[NodeId(2), NodeId(3)],
        rafter_service::TransportDriverOptions::default(),
        record(3, &[1, 2, 3]),
    );

    assert_eq!(live_of(&driver), ids(&[1, 2, 3]));
    assert_eq!(driver.service_state(), DriverServiceState::Serving);
    assert!(
        !transport.peer_sets().is_empty(),
        "and the link layer was told who may speak"
    );
}

/// A runtime that contradicts the register mid-flight stops the driver serving
/// and stops it publishing.
///
/// **The live path, which is the one that cannot return a refusal.**
/// `route_membership_event` runs from every step outcome including a failing one,
/// so a committed fact that disagrees with the register at the register's own
/// position has nowhere to be raised — it is recorded, and
/// [`DriverServiceState::ContradictoryCurrentState`] is how a supervisor hears
/// about it.
///
/// It takes a runtime breaking its own contract to produce, and that is the
/// honest description of the fixture rather than an apology for it: one commit
/// index names one committed membership for good, so a *correct* runtime cannot
/// report two. What the driver must not do is believe the second one. Retiring
/// node 3 here would be permanent — the floor never falls — and there is nothing
/// deciding which of the two claims is the true one.
///
/// Two things hold afterwards. Client work is refused with a reason that names
/// the position, and every later entry point's flush publishes nothing: the last
/// policy the link layer accepted was licensed by inputs that agreed, and no
/// policy this driver could derive now is.
#[test]
fn a_runtime_that_contradicts_the_register_stops_serving_and_stops_publishing() {
    let runtime = ScriptedMembershipRuntime::for_node_at(NodeId(1), &[1, 2, 3], &[1, 2, 3], AT);
    let handle = runtime.handle();
    let (driver, transport) = scripted_driver_with_checkpoint(
        runtime,
        Nameable::all(),
        &[NodeId(2), NodeId(3)],
        rafter_service::TransportDriverOptions::default(),
        record(3, &[1, 2, 3]),
    );
    assert_eq!(driver.service_state(), DriverServiceState::Serving);
    let published_before = transport.policies();

    // The committed membership moves and the commit index does not, which is the
    // pair a correct runtime cannot produce.
    contradict_committed_in_place(&handle, &[1, 2]);
    driver.tick().expect("the protocol still advances");

    assert_eq!(
        driver.service_state(),
        DriverServiceState::ContradictoryCurrentState { through: AT },
        "the driver names the position its two inputs disagree at"
    );
    assert_eq!(
        live_of(&driver),
        ids(&[1, 2, 3]),
        "and moved nothing: the register is still the last consistent reading"
    );

    let refused = driver
        .begin_write(
            ("key".to_owned(), "value".to_owned()),
            rafter_service::WriteOptions::default(),
        )
        .map(|(id, _future)| id)
        .expect_err("a driver whose licensing inputs disagree takes no client work");
    assert!(
        matches!(
            refused,
            WriteError::Unavailable {
                reason: rafter_service::DriverUnavailableReason::ContradictoryCurrentState
            }
        ),
        "got {refused:?}"
    );

    // **And the flush stays silent even when something it *could* publish
    // moves.** A driver that only stopped publishing because nothing changed
    // would be relying on the accident that the refused fact moved nothing. The
    // effective configuration widens here — the committed one does not, so the
    // index is untouched and the fixture stays as dishonest as it already was —
    // and the peer set that widening would license is withheld with everything
    // else.
    change_on_step(&handle, &[1, 2, 3, 4], &[1, 2]);
    driver.tick().expect("later entry points still run");
    driver
        .drive_pending_reads()
        .expect("and so does the third one");
    assert!(
        driver.peer_policy_is_stale(),
        "the driver knows its link layer is behind what it would otherwise \
         publish"
    );
    assert_eq!(
        transport.policies(),
        published_before,
        "and publishes nothing anyway, because a retirement floor is permanent \
         and none of the ones derivable here is licensed"
    );
}

/// A contradicted driver's durable record freezes, and the marker survives a
/// restart.
///
/// **The evidence used to be overwritten and then forgotten.** `contradicted_at`
/// stopped the flush and nothing else: `route_membership_events` kept folding
/// later batches into the checkpointable fields and kept advancing the epoch, so
/// the embedder persisted a *newer* record — one carrying no trace of the fork —
/// and a crash restored it into a driver that started clean. Where the rebuilt
/// runtime happened to agree at the newer position, the replica went back to
/// serving and to publishing retirement floors, and the unresolved same-position
/// fork simply disappeared.
///
/// Both halves are pinned here because either alone is insufficient. Freezing the
/// fields without a durable marker loses the fork the moment the process restarts
/// from the record it wrote *before* the contradiction; recording the marker
/// without freezing persists a record whose register moved past the position the
/// marker names.
#[test]
fn a_contradicted_driver_freezes_its_record_and_carries_the_marker_across_a_restart() {
    let runtime = ScriptedMembershipRuntime::for_node_at(NodeId(1), &[1, 2, 3], &[1, 2, 3], AT);
    let handle = runtime.handle();
    let (driver, _transport) = scripted_driver_with_checkpoint(
        runtime,
        Nameable::all(),
        &[NodeId(2), NodeId(3)],
        rafter_service::TransportDriverOptions::default(),
        record(3, &[1, 2, 3]),
    );

    contradict_committed_in_place(&handle, &[1, 2]);
    driver.tick().expect("the protocol still advances");
    assert_eq!(
        driver.service_state(),
        DriverServiceState::ContradictoryCurrentState { through: AT }
    );
    let frozen = driver.control_plane_checkpoint();
    let epoch = driver.control_plane_checkpoint_epoch();

    // A later committed fact arrives, and it is one the driver would ordinarily
    // absorb: node 4 is admitted at a position above the fork. A contradicted
    // driver keeps stepping — it is still a useful follower — and that is exactly
    // why the record has to stop moving on its own.
    change_on_step(&handle, &[1, 2, 3, 4], &[1, 2, 3, 4]);
    driver.tick().expect("a contradicted driver still steps");

    assert_eq!(
        driver.control_plane_checkpoint(),
        frozen,
        "the durable retirement record froze at the contradiction"
    );
    assert_eq!(
        driver.control_plane_checkpoint_epoch(),
        epoch,
        "so no epoch move asks the embedder to write a record derived from \
         inputs this driver has declared untrustworthy"
    );
    assert_eq!(
        frozen.contradicted_at,
        Some(AT),
        "and the record carries the position the fork was found at"
    );

    // The restart. A rebuilt runtime that agrees with the record everywhere is
    // the case that used to serve.
    let (restarted, transport) = scripted_driver_with_checkpoint(
        ScriptedMembershipRuntime::for_node_at(NodeId(1), &[1, 2, 3], &[1, 2, 3], AT),
        Nameable::all(),
        &[NodeId(2), NodeId(3)],
        rafter_service::TransportDriverOptions::default(),
        frozen,
    );

    assert_eq!(
        restarted.service_state(),
        DriverServiceState::ContradictoryCurrentState { through: AT },
        "a restored marker starts the driver refusing, whatever the rebuilt \
         runtime happens to agree about"
    );
    assert!(
        transport.policies().is_empty(),
        "and it publishes nothing: {:?}",
        transport.policies()
    );
}

/// A marked record is refused at an adoption and accepted by a constructor.
///
/// **One record, two entry points, and the line between them is the chain rule
/// the join already draws.** A constructor restores into empty held state, which
/// is this chain resuming itself: the marker has to be carried or the terminal
/// state ends at the next crash, which is the whole defect the durable marker
/// closes. An adoption merges a record into a driver that has been running, and a
/// marked record is a statement that its chain observed a fork nothing can
/// resolve — taking its mark and register on trust is licensing exactly what it
/// refused, and the joined record would carry no marker at all.
///
/// Refusing costs nothing, which is what makes the asymmetry affordable: the file
/// is still on the embedder's disk, and the supported way to read one back is the
/// constructor the other half of this test uses.
#[test]
fn a_marked_record_is_refused_at_an_adoption_and_resumed_by_a_constructor() {
    let mut marked = record(3, &[1, 2, 3]);
    marked.contradicted_at = Some(AT);

    let (driver, transport) = scripted_driver_with_checkpoint(
        runtime(&[1, 2, 3]),
        Nameable::all(),
        &[NodeId(2), NodeId(3)],
        rafter_service::TransportDriverOptions::default(),
        PeerControlPlaneCheckpoint::empty(GROUP),
    );
    let policies_before = transport.policies();
    let group = driver.release_group().expect("the driver holds a group");

    let refused = driver.adopt_group_with_checkpoint(group, Vec::new(), marked.clone());
    assert!(
        matches!(
            refused,
            Err(ManagedDriverError::InvalidControlPlaneCheckpoint {
                reason: ControlPlaneCheckpointError::ContradictedRecordMerged { through }
            }) if through == AT
        ),
        "got {refused:?}"
    );
    assert_eq!(
        transport.policies(),
        policies_before,
        "and the refusal reached nothing: {:?}",
        transport.policies()
    );

    // The same record, through the entry point that is allowed to take it.
    let (resumed, resumed_transport) = scripted_driver_with_checkpoint(
        runtime(&[1, 2, 3]),
        Nameable::all(),
        &[NodeId(2), NodeId(3)],
        rafter_service::TransportDriverOptions::default(),
        marked.clone(),
    );
    assert_eq!(
        resumed.service_state(),
        DriverServiceState::ContradictoryCurrentState { through: AT },
        "the constructor carries the marker rather than starting clean"
    );
    assert!(
        resumed_transport.policies().is_empty(),
        "and publishes nothing: {:?}",
        resumed_transport.policies()
    );
    assert_eq!(
        resumed.control_plane_checkpoint(),
        marked,
        "the record it would hand back is the record it was given, marker \
         included — an incarnation that dropped the line would end the refusal at \
         the next restart"
    );
}

/// A frame from `from`, used only to ask whether this driver admits it at all.
fn a_vote(from: NodeId) -> rafter_service::AuthenticatedPeerEnvelope<u64, Principal> {
    rafter_service::AuthenticatedPeerEnvelope {
        group_id: GROUP,
        authenticated_peer: Principal::for_node(from),
        raft_from: from,
        raft_to: NodeId(1),
        message: Message::RequestVote(RequestVote {
            term: Term(1),
            candidate_id: from,
            last_log_index: AT,
            last_log_term: Term(1),
        }),
    }
}

/// A restored marker does not stop the replica being replicated to.
///
/// **Terminal for client work is not terminal for the protocol**, and the two
/// halves of the membership state are what keep those separable. The durable
/// record freezes where the marker says; the two runtime facts go on tracking
/// what this replica's own stream reports, because the inbound admission check
/// reads them and a replica that refused every frame could never catch up. That
/// is the same catch-up the terminal state explicitly allows.
#[test]
fn a_resumed_marked_driver_still_admits_the_cluster_it_belongs_to() {
    let mut marked = record(3, &[1, 2, 3]);
    marked.contradicted_at = Some(AT);

    let (resumed, _transport) = scripted_driver_with_checkpoint(
        runtime(&[1, 2, 3]),
        Nameable::all(),
        &[NodeId(2), NodeId(3)],
        rafter_service::TransportDriverOptions::default(),
        marked,
    );

    assert!(
        resumed.deliver(a_vote(NodeId(2))).is_ok(),
        "a frame from a replica the cluster still has committed is delivered"
    );
    assert_eq!(
        resumed.refused_non_member_frames(),
        0,
        "so no committed member was turned away by a record that froze"
    );
    assert_eq!(
        resumed.service_state(),
        DriverServiceState::ContradictoryCurrentState { through: AT },
        "and stepping did not clear the marker"
    );
}
