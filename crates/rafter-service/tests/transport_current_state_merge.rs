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
