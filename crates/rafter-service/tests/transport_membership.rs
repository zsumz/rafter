//! What the membership event stream does to this driver, through its own front
//! door.
//!
//! Every scenario here drives `deliver` or `tick` and nothing else. That is
//! deliberate and it is the lesson of the round that added the file: the
//! effective-membership branch of the router was pinned only by in-crate tests
//! that handed it an event directly, because the app layer emitted that event
//! only for a step carrying a local membership *request* — an input this driver
//! has no method to produce. The branch passed its tests and no follower could
//! reach it.
//!
//! The two facts are tracked separately and each is *assigned* from its own
//! stream rather than merged into one set. A union that only ever grew could not
//! express a rollback at all: the replica an overwritten configuration named
//! would stay authorized for the life of the incarnation, with no later fact
//! able to take it back.

#![allow(clippy::wildcard_imports)]

mod support;

use rafter_service::{
    AuthenticatedPeerEnvelope, AuthenticatedPeerEnvelopeError, InboundEnvelopeError,
    TransportDriverOptions,
};
use support::scripted::*;
use support::transport::*;
use support::*;

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

fn principals(node_ids: &[u64]) -> Vec<Principal> {
    node_ids
        .iter()
        .map(|node_id| Principal::for_node(NodeId(*node_id)))
        .collect()
}

/// A follower that learns a configuration by replication widens its peer set.
///
/// The joiner has to be able to speak before the change commits, or it can never
/// catch up and the change can never commit — so a follower that heard nothing
/// for a replicated configuration entry could block the very transition it was
/// participating in. Nothing on this path carries a membership request: the
/// entry arrived in an `AppendEntries` and the driver observes it as a fact
/// about its own runtime.
#[test]
fn a_replicated_addition_widens_this_drivers_peer_set() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2], &[1, 2]);
    let handle = runtime.handle();
    let (driver, transport) = scripted_driver(runtime, Nameable::all());

    assert_eq!(
        transport.peer_sets(),
        vec![principals(&[2])],
        "adoption published the membership this replica opened with"
    );
    assert!(
        driver.deliver(a_vote(NodeId(3))).is_err(),
        "node 3 is in no configuration yet"
    );

    // The leader's next append carries the addition of node 3, uncommitted.
    change_on_step(&handle, &[1, 2, 3], &[1, 2]);
    driver
        .deliver(a_vote(NodeId(2)))
        .expect("node 2 is a member and its frame is accepted");

    assert_eq!(
        transport.peer_sets().last().expect("a set was published"),
        &principals(&[2, 3]),
        "the uncommitted addition reached the link layer"
    );
    assert!(
        !transport.retires(NodeId(3)),
        "and nothing was fenced: an uncommitted change may only widen"
    );
    driver
        .deliver(a_vote(NodeId(3)))
        .expect("the joiner may speak, which is what the widening is for");
}

/// A new leader overwriting the uncommitted addition takes the joiner back out.
///
/// The clause a peer set built by union could not express. `known_members` only
/// ever grew, so the rolled-back replica stayed in the published set and in the
/// inbound check until something *committed* removed it — and nothing ever would,
/// because it was never committed in. Assigning the effective fact from its own
/// stream is what makes the rollback expressible.
#[test]
fn an_overwritten_addition_leaves_the_peer_set_and_the_inbound_check() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2], &[1, 2]);
    let handle = runtime.handle();
    let (driver, transport) = scripted_driver(runtime, Nameable::all());

    change_on_step(&handle, &[1, 2, 3], &[1, 2]);
    driver
        .deliver(a_vote(NodeId(2)))
        .expect("node 2 is a member");
    driver
        .deliver(a_vote(NodeId(3)))
        .expect("the joiner speaks while the change is in flight");

    // A new leader wins an election with a log that never held the addition.
    change_on_step(&handle, &[1, 2], &[1, 2]);
    driver
        .deliver(a_vote(NodeId(2)))
        .expect("node 2 is a member");

    assert_eq!(
        transport.peer_sets().last().expect("a set was published"),
        &principals(&[2]),
        "the rolled-back replica left the published peer set"
    );
    assert!(
        !driver.peer_policy_is_stale(),
        "and the link layer holds exactly what the driver requires"
    );

    let refused = driver.deliver(a_vote(NodeId(3)));
    assert!(
        matches!(
            refused,
            Err(InboundEnvelopeError::NotInMembership { node_id: NodeId(3) })
        ),
        "and it left the local inbound check with it, got {refused:?}"
    );
}

/// The rolled-back ID was never committed, so nothing about it was spent.
///
/// The retirement rule reads the *committed* stream and only that. An addition
/// that never committed never raised the high-water mark, so its disappearance
/// licenses no fence, retires no identity, and leaves the ID allocatable — which
/// it must, because a change that was reverted may legitimately be proposed
/// again.
#[test]
fn a_rolled_back_addition_is_never_fenced_and_never_retired() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2], &[1, 2]);
    let handle = runtime.handle();
    let (driver, transport) = scripted_driver(runtime, Nameable::all());

    change_on_step(&handle, &[1, 2, 3], &[1, 2]);
    driver
        .deliver(a_vote(NodeId(2)))
        .expect("node 2 is a member");
    change_on_step(&handle, &[1, 2], &[1, 2]);
    driver
        .deliver(a_vote(NodeId(2)))
        .expect("node 2 is a member");

    // Something else commits later, which is the moment a retirement set built
    // from the wrong stream would have taken its diff and fenced node 3.
    change_on_step(&handle, &[1, 2], &[1, 2]);
    driver.tick().expect("the tick advances the protocol");

    assert!(
        !transport.retires(NodeId(3)),
        "no committed removal ever happened, so nothing retires node 3: {:?}",
        transport.policies().last()
    );
    assert_eq!(
        driver.readmitted_retired_peers(),
        0,
        "and node 3's identity is unspent"
    );
}

/// One step that commits a configuration while a later one is in effect keeps
/// both facts straight.
///
/// The composition, and the reason the two facts are separate values rather than
/// one merged set. The committed fact is the only one that licenses narrowing,
/// and it must not narrow past the configuration currently in effect: a change
/// committing does not retract a *later* change already appended over it, and a
/// driver that published the committed set alone would cut off the joiner the
/// change in flight depends on.
#[test]
fn a_commit_under_a_later_effective_configuration_keeps_both_facts() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2], &[1, 2]);
    let handle = runtime.handle();
    let (driver, transport) =
        scripted_driver_authorizing(runtime, Nameable::all(), &[NodeId(2), NodeId(3), NodeId(4)]);

    // Node 3's addition commits, and node 4's is already appended over it.
    change_on_step(&handle, &[1, 2, 3, 4], &[1, 2, 3]);
    driver
        .deliver(a_vote(NodeId(2)))
        .expect("node 2 is a member");

    assert_eq!(
        transport.peer_sets().last().expect("a set was published"),
        &principals(&[2, 3, 4]),
        "the committed set sets the floor and the effective one adds to it"
    );
    assert!(
        !transport.retires(NodeId(3)) && !transport.retires(NodeId(4)),
        "nothing left the committed configuration, so nothing may be retired: \
         {:?}",
        transport.policies().last()
    );
    driver
        .deliver(a_vote(NodeId(4)))
        .expect("the joiner of the change still in flight may speak");

    // Node 4's addition commits too, and node 3's removal is appended over it.
    change_on_step(&handle, &[1, 2, 4], &[1, 2, 3, 4]);
    driver
        .deliver(a_vote(NodeId(2)))
        .expect("node 2 is a member");

    assert_eq!(
        transport.peer_sets().last().expect("a set was published"),
        &principals(&[2, 3, 4]),
        "the removal of node 3 has not committed, so nothing may be taken away"
    );
    assert!(
        !transport.retires(NodeId(3)),
        "and no retirement is licensed for an uncommitted removal: {:?}",
        transport.policies().last()
    );
    driver
        .deliver(a_vote(NodeId(3)))
        .expect("node 3 is still committed and may still speak");

    // And now it commits.
    change_on_step(&handle, &[1, 2, 4], &[1, 2, 4]);
    driver
        .deliver(a_vote(NodeId(2)))
        .expect("node 2 is a member");

    assert_eq!(
        transport.peer_sets().last().expect("a set was published"),
        &principals(&[2, 4]),
        "the committed removal is what narrows the set"
    );
    assert!(transport.retires(NodeId(3)), "and what licenses the fence");
    let refused = driver.deliver(a_vote(NodeId(3)));
    assert!(
        refused.is_err(),
        "the removed replica may not speak, got {refused:?}"
    );
}

/// An uncommitted removal takes nothing away and fences nobody.
///
/// The mirror of the widening clause, and the reason the effective fact cannot
/// narrow the peer set on its own. A change that has appended and not committed
/// can still be reverted, so acting on it would cut off a replica the cluster
/// still counts — and nothing would repair it if the change never commits.
#[test]
fn an_uncommitted_removal_narrows_nothing_and_fences_nobody() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let handle = runtime.handle();
    let (driver, transport) = scripted_driver(runtime, Nameable::all());

    // Node 3's removal appends and does not commit.
    change_on_step(&handle, &[1, 2], &[1, 2, 3]);
    driver
        .deliver(a_vote(NodeId(2)))
        .expect("node 2 is a member");

    assert_eq!(
        transport.peer_sets(),
        vec![principals(&[2, 3])],
        "the committed configuration is still the floor, so nothing was \
         republished and nothing was taken away"
    );
    assert!(
        !driver.peer_policy_is_stale(),
        "and the driver agrees the link layer is level"
    );
    assert!(
        !transport.retires(NodeId(3)),
        "only a committed removal licenses a retirement: {:?}",
        transport.policies().last()
    );
    driver
        .deliver(a_vote(NodeId(3)))
        .expect("node 3 is still committed and may still speak");
}

/// A widening that names a spent identity again retires nothing new and
/// readmits nobody.
///
/// Two clauses the same fact decides. The fence stays owed because an effective
/// configuration may still be reverted, so it is too weak to retract an
/// obligation a committed fact created — a driver that let it would drop the
/// fence for a removal the cluster committed, on the strength of a change that
/// may never commit. And the replica stays refused because the committed removal
/// spent the `(group, NodeId)` pair: the widening is not a fact about who may
/// speak, it is a contract violation, visible as one and refused as one.
#[test]
fn an_uncommitted_widening_retires_nothing_and_readmits_no_spent_identity() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let handle = runtime.handle();
    let (driver, transport) = scripted_driver(runtime, Nameable::all());
    // The link refuses every publication for the first half of the test, which
    // is what makes the driver's own check the only admission control in play.
    transport.refuse_next_peer_updates(16);

    change_on_step(&handle, &[1, 2], &[1, 2]);
    driver
        .deliver(a_vote(NodeId(2)))
        .expect("node 2 is a member");

    assert!(
        driver.peer_policy_is_stale(),
        "the committed removal licensed a policy the link would not take"
    );

    // A later configuration names node 3 again and has not committed.
    change_on_step(&handle, &[1, 2, 3], &[1, 2]);
    driver
        .deliver(a_vote(NodeId(2)))
        .expect("node 2 is a member");

    assert!(
        !driver
            .control_plane_checkpoint()
            .current_committed
            .expect("the driver holds a current state")
            .membership
            .contains(&NodeId(3)),
        "the removal outlived the widening: a change that may be reverted \
         retracts nothing"
    );
    assert_eq!(
        driver.readmitted_retired_peers(),
        1,
        "and the violation is observable rather than merely absorbed"
    );
    let refused = driver.deliver(a_vote(NodeId(3)));
    assert!(
        matches!(
            refused,
            Err(InboundEnvelopeError::NotInMembership { node_id: NodeId(3) })
        ),
        "and naming a spent identity again does not un-spend it, got {refused:?}"
    );

    // The link recovers, and nothing about the membership changes.
    transport.allow_peer_updates();
    driver.tick().expect("the tick advances the protocol");

    assert_eq!(
        transport.peer_sets().last().expect("a set was published"),
        &principals(&[2]),
        "node 3 is not published back into the peer set"
    );
    assert!(
        transport.retires(NodeId(3)),
        "and the retry installed the policy that retires it: {:?}",
        transport.policies().last()
    );
    assert!(!driver.peer_policy_is_stale(), "and nothing is owed now");
}

/// A step that fails still narrows the peer set and fences what left.
///
/// The loss window at this driver's own boundary, closed. The runtime commits
/// node 3's removal and hands the group an entry the application then refuses,
/// so the step returns `Err` with no report — and the removal is the one fact
/// that licenses narrowing a peer set and fencing what left it. A driver that
/// routed only successful reports would wait for a later successful step to
/// rescue it, and there is no later step here: the group is poisoned, so the
/// fence and the peer set would stay wrong for the life of the incarnation.
#[test]
fn a_failed_step_still_routes_the_membership_it_moved_through() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let handle = runtime.handle();
    let (driver, transport) = scripted_driver_with_app(
        runtime,
        Nameable::all(),
        &[NodeId(2), NodeId(3)],
        TransportDriverOptions::default(),
        failing_apply(),
    );

    // The same step commits node 3 out and releases an entry the application
    // refuses. Both halves are load-bearing: without the failure the report
    // carries the removal, and without the removal there is nothing to lose.
    change_on_step(&handle, &[1, 2], &[1, 2]);
    outputs_on_step(
        &handle,
        vec![RaftOutput::Apply {
            index: LogIndex(6),
            term: Term(1),
            payload: SharedPayload::from(&b"key\nvalue"[..]),
            local_proposal_id: None,
        }],
    );

    let delivered = driver.deliver(a_vote(NodeId(2)));
    assert!(
        matches!(delivered, Err(InboundEnvelopeError::Driver { .. })),
        "the step failed applying, so no report reached the driver: {delivered:?}"
    );

    assert_eq!(
        transport.peer_sets().last().expect("a set was published"),
        &principals(&[2]),
        "the committed removal narrowed the published peer set anyway"
    );
    assert!(
        transport.retires(NodeId(3)),
        "and the retirement it licensed was installed: {:?}",
        transport.policies().last()
    );
    assert!(
        !driver.peer_policy_is_stale(),
        "the link layer holds exactly what the driver requires"
    );

    let refused = driver.deliver(a_vote(NodeId(3)));
    assert!(
        matches!(
            refused,
            Err(InboundEnvelopeError::Rejected {
                source: AuthenticatedPeerEnvelopeError::FencedPeer { node_id: NodeId(3) }
            })
        ),
        "and the removed replica is refused by the fence itself rather than by \
         this driver's own fail-closed check, got {refused:?}"
    );
}

/// A failed step's *effective* rollback reaches the link layer too.
///
/// The widening half of the same window, and the one no consumer can re-derive:
/// a new leader truncates an uncommitted addition back off the log and nothing
/// commits, so there is no later fact that names what was taken away. The peer
/// set must narrow back to the committed floor even though the step that
/// observed the truncation failed.
#[test]
fn a_failed_step_still_routes_an_effective_rollback() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2], &[1, 2]);
    let handle = runtime.handle();
    let (driver, transport) = scripted_driver_with_app(
        runtime,
        Nameable::all(),
        &[NodeId(2), NodeId(3)],
        TransportDriverOptions::default(),
        failing_apply(),
    );

    change_on_step(&handle, &[1, 2, 3], &[1, 2]);
    driver
        .deliver(a_vote(NodeId(2)))
        .expect("node 2 is a member and the step released no work to fail on");
    assert_eq!(
        transport.peer_sets().last().expect("a set was published"),
        &principals(&[2, 3]),
        "the joiner is authorized while the change is in flight"
    );

    // A new leader's log never held the addition, and the step that observes
    // the truncation also releases an entry the application refuses.
    change_on_step(&handle, &[1, 2], &[1, 2]);
    outputs_on_step(
        &handle,
        vec![RaftOutput::Apply {
            index: LogIndex(6),
            term: Term(1),
            payload: SharedPayload::from(&b"key\nvalue"[..]),
            local_proposal_id: None,
        }],
    );
    let delivered = driver.deliver(a_vote(NodeId(2)));
    assert!(
        matches!(delivered, Err(InboundEnvelopeError::Driver { .. })),
        "the step failed applying: {delivered:?}"
    );

    assert_eq!(
        transport.peer_sets().last().expect("a set was published"),
        &principals(&[2]),
        "the rolled-back replica left the published peer set anyway"
    );
    assert!(
        !transport.retires(NodeId(3)),
        "and nothing was fenced: an addition that never committed retires nothing"
    );
    let refused = driver.deliver(a_vote(NodeId(3)));
    assert!(
        matches!(
            refused,
            Err(InboundEnvelopeError::NotInMembership { node_id: NodeId(3) })
        ),
        "the rolled-back replica left the inbound check, got {refused:?}"
    );
}
