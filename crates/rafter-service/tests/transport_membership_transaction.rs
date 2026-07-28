//! One membership statement per batch of facts, or none at all.
//!
//! **A retirement floor is permanent, so the driver must never issue one from a
//! prefix of a batch it goes on to refuse.** The facts that license a publication
//! do not arrive one at a time: an adoption offers a checkpoint *and* a runtime,
//! and one step's report can carry an effective change, a crossing, and a
//! committed endpoint together. Each of those used to be merged into live state
//! and flushed to the link layer the moment it was read — so a contradiction in
//! the second fact arrived after the first had already been recorded durably and
//! stated to the transport, and neither could be taken back.
//!
//! So the membership fields move as one transaction. Every fact of one batch is
//! folded into a candidate; contradictions are found there; and only a candidate
//! that survives the whole batch is installed, with exactly one policy flush
//! behind it. A batch that refuses leaves the driver holding the last consistent
//! state it had — not the prefix that happened to parse — and tells the link
//! layer nothing.
//!
//! What deliberately stays outside the transaction is everything loss-tolerant:
//! peer sends, snapshot directives, proposal and read resolutions. Raft re-sends
//! a dropped frame and nothing re-derives a permanent control-plane statement,
//! which is the whole reason only one of them is transactional.

#![allow(clippy::wildcard_imports)]

mod support;

use rafter_service::{
    AuthenticatedPeerEnvelope, CurrentCommittedState, DriverServiceState, InboundEnvelopeError,
    ManagedDriverError, PeerControlPlaneCheckpoint, TransportDriverOptions,
};
use support::scripted::*;
use support::transport::*;
use support::*;

/// Where the scripted runtimes in this file stand.
///
/// [`ScriptedMembershipRuntime`] opens at commit index 5 and advances it only
/// when the committed membership honestly moves, so this is where a driver built
/// over one ends up after its adoption publication.
const AT: LogIndex = LogIndex(5);

/// Where a record offered mid-life stands: after what the driver holds, so it
/// merges rather than being refused as an older observation of one chain.
const LATER: LogIndex = LogIndex(7);

fn a_vote(from: NodeId) -> AuthenticatedPeerEnvelope<u64, Principal> {
    AuthenticatedPeerEnvelope {
        group_id: GROUP,
        authenticated_peer: Principal::for_node(from),
        raft_from: from,
        raft_to: NodeId(1),
        message: Message::RequestVote(RequestVote {
            term: Term(1),
            candidate_id: from,
            last_log_index: LogIndex(5),
            last_log_term: Term(1),
        }),
    }
}

/// A driver over `{1,2,3}` whose handle the caller can move behind its back.
fn driver_over_one_two_three() -> (ScriptedDriver, QueueTransport, ScriptedMembershipHandle) {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let handle = runtime.handle();
    let (driver, transport) = scripted_driver_with_checkpoint(
        runtime,
        Nameable::all(),
        &[NodeId(2), NodeId(3), NodeId(4)],
        TransportDriverOptions::default(),
        PeerControlPlaneCheckpoint::empty(GROUP),
    );
    (driver, transport, handle)
}

/// An adoption whose checkpoint changes this driver and whose runtime then
/// contradicts it installs nothing at all.
///
/// **The refusal the existing regression could not see**, because that one
/// offered a checkpoint identical to the state the driver already held: the join
/// moved nothing, so a driver that kept the join's result and a driver that
/// rolled it back were indistinguishable. Here the record moves the register, the
/// mark, and the epoch — and the runtime offered beside it disagrees with the
/// merged record at the very position both stand at.
///
/// Adoption's refusals above the installation must leave the driver exactly as it
/// was, and the checkpoint is durable state: a driver that kept a mark and a
/// register recovered from a record it went on to declare contradictory would
/// persist them at the next epoch poll, and no later fact takes a floor back.
#[test]
fn a_refused_adoption_keeps_neither_the_record_nor_the_epoch_nor_the_policy() {
    let (driver, transport, _handle) = driver_over_one_two_three();
    let before = driver.control_plane_checkpoint();
    let epoch_before = driver.control_plane_checkpoint_epoch();
    let policies_before = transport.policies();
    let _ = driver.release_group().expect("the driver holds a group");

    // The record stands *later* than the driver does, so it merges cleanly and
    // moves the register, the mark, and the epoch. The runtime offered beside it
    // then stands at the record's own position and names `{1,2,3}` there. One of
    // them is wrong about a single fact and nothing here can decide which.
    let mut record = PeerControlPlaneCheckpoint::empty(GROUP);
    record.committed_id_high_water = Some(NodeId(4));
    record.current_committed = Some(CurrentCommittedState::new(
        LATER,
        [1, 2, 4].into_iter().map(NodeId).collect(),
    ));

    let refused = driver.adopt_group_with_checkpoint(
        RaftGroup::new(
            GROUP,
            NodeId(1),
            ScriptedMembershipRuntime::for_node_at(NodeId(1), &[1, 2, 3], &[1, 2, 3], LATER),
            KvStateMachine::default(),
        ),
        Vec::new(),
        record,
    );

    assert!(
        matches!(
            refused,
            Err(ManagedDriverError::InvalidControlPlaneCheckpoint { .. })
        ),
        "got {refused:?}"
    );
    assert_eq!(
        driver.control_plane_checkpoint(),
        before,
        "the record the adoption refused is not the record this driver keeps"
    );
    assert_eq!(
        driver.control_plane_checkpoint_epoch(),
        epoch_before,
        "and no epoch move tells an embedder to persist it"
    );
    assert!(
        matches!(
            driver.committed_application_index(),
            Err(ManagedDriverError::NoGroup)
        ),
        "the group slot is still empty, so the next adoption is an ordinary first \
         attempt"
    );
    assert_eq!(
        transport.policies(),
        policies_before,
        "and the link layer was told nothing on the way to the refusal"
    );
}

/// One report's effective change is not published before that report's committed
/// fact has been read.
///
/// The step reports the effective configuration first and the committed
/// observation second. A driver that flushed per event authorized node 4 from the
/// first half of a report whose second half it then declared contradictory —
/// and a peer set is not repairable by silence: the link layer holds it until
/// something replaces it, and a contradicted driver publishes nothing ever again.
#[test]
fn a_contradictory_report_publishes_no_intermediate_policy() {
    let (driver, transport, handle) = driver_over_one_two_three();
    let policies_before = transport.policies();

    contradict_committed_beneath_effective(&handle, &[1, 2, 3, 4], &[1, 2, 4]);
    driver.tick().expect("the protocol still advances");

    assert_eq!(
        driver.service_state(),
        DriverServiceState::ContradictoryCurrentState { through: AT },
        "the report's committed half disagrees with the register at {AT}"
    );
    assert_eq!(
        transport.policies(),
        policies_before,
        "so nothing from that report reached the link layer, including the \
         effective widening that arrived first"
    );
    assert!(
        !transport
            .policies()
            .last()
            .is_some_and(|(peers, _)| peers.contains(&NodeId(4))),
        "and node 4 was never authorized"
    );
}

/// A refused report leaves the last consistent state, not the part of it that
/// parsed.
///
/// The other half of the same guarantee, read from inside the driver. The
/// effective membership is what the inbound check is derived from, so a driver
/// that kept the widening from a refused report would go on admitting a replica
/// on the strength of a fact it had rejected — while publishing nothing that says
/// so, which leaves the admission with no record anywhere.
#[test]
fn a_refused_report_leaves_the_last_consistent_membership() {
    let (driver, transport, handle) = driver_over_one_two_three();

    contradict_committed_beneath_effective(&handle, &[1, 2, 3, 4], &[1, 2, 4]);
    driver.tick().expect("the protocol still advances");

    let refused = driver
        .deliver(a_vote(NodeId(4)))
        .expect_err("node 4 is named only by the report this driver refused");
    assert!(
        matches!(
            refused,
            InboundEnvelopeError::NotInMembership { node_id: NodeId(4) }
                | InboundEnvelopeError::Rejected { .. }
        ),
        "got {refused:?}"
    );
    assert!(
        !driver.peer_policy_is_stale(),
        "and the driver is not holding back a policy it would otherwise publish: \
         the membership it derives one from never moved"
    );
    assert!(
        !transport.retires(NodeId(3)),
        "node 3 is still live in the last consistent committed reading"
    );
}

/// A contradiction outlives the group it was found under, and adoption says so.
///
/// Both contradiction states are terminal for the incarnation, and releasing
/// the group is the prescribed first step of *retiring* that incarnation — not
/// a way to rearm it. A driver that reported `Released` after a contradiction
/// read as an ordinary reusable driver, and its next adoption installed a group
/// and a node ID before the old contradiction resurfaced as an error — a
/// partially adopted group produced by terminal state that predates the
/// adoption, which the partial-adoption contract reserves for failures the new
/// group's own recovery outputs produce.
#[test]
fn a_contradicted_driver_refuses_adoption_before_taking_the_group() {
    let (driver, transport, handle) = driver_over_one_two_three();

    contradict_committed_beneath_effective(&handle, &[1, 2, 3, 4], &[1, 2, 4]);
    driver.tick().expect("the protocol still advances");
    assert_eq!(
        driver.service_state(),
        DriverServiceState::ContradictoryCurrentState { through: AT },
        "the report's committed half disagrees with the register at {AT}"
    );

    driver
        .release_group()
        .expect("release the contradicted group");
    assert_eq!(
        driver.service_state(),
        DriverServiceState::ContradictoryCurrentState { through: AT },
        "releasing the group resolves nothing, so the state must still say so"
    );

    let record_before = driver.control_plane_checkpoint();
    let epoch_before = driver.control_plane_checkpoint_epoch();
    let policies_before = transport.policies();

    let refused = driver.adopt_group_with_checkpoint(
        RaftGroup::new(
            GROUP,
            NodeId(5),
            ScriptedMembershipRuntime::for_node_at(NodeId(5), &[1, 2, 3], &[1, 2, 3], AT),
            KvStateMachine::default(),
        ),
        Vec::new(),
        PeerControlPlaneCheckpoint::empty(GROUP),
    );
    assert!(
        matches!(
            refused,
            Err(ManagedDriverError::ControlPlaneContradicted { .. })
        ),
        "the refusal names the driver's own terminal state, got {refused:?}"
    );

    assert!(
        matches!(
            driver.committed_application_index(),
            Err(ManagedDriverError::NoGroup)
        ),
        "nothing about the offered group was taken"
    );
    assert_eq!(
        driver.control_plane_checkpoint(),
        record_before,
        "the durable record did not move"
    );
    assert_eq!(
        driver.control_plane_checkpoint_epoch(),
        epoch_before,
        "and no epoch move tells an embedder to persist anything"
    );
    assert_eq!(
        transport.policies(),
        policies_before,
        "the link layer was told nothing"
    );
}
