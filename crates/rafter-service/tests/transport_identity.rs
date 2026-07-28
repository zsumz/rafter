//! The `(group, NodeId)` pair is single-use, and this is what that costs.
//!
//! A committed removal spends the identity it names. The kernel keeps no
//! removed-node tombstones and cannot grow one, so a contract-violating
//! membership naming a spent ID can reach this driver; it is the last layer that
//! can still refuse — the transport fence a removal installs is permanent, and
//! there is no unfence to undo it with — so it refuses, reports, and does not
//! try to repair.
//!
//! The local replica's own identity is spent the same way and by the same fact,
//! which is the half this suite was added for: a driver that filtered itself out
//! of the removed diff retired nothing when the cluster removed *it*, and could
//! then adopt a peer's spent ID as its own.

#![allow(clippy::wildcard_imports)]

mod support;

use rafter_service::{
    AuthenticatedPeerEnvelope, AuthenticatedPeerEnvelopeError, DriverServiceState,
    DriverUnavailableReason, InboundEnvelopeError, ReadOptions, WriteOptions,
};
use support::scripted::*;
use support::transport::*;
use support::*;

/// Where a replacement incarnation's runtime opens, for the cases that rebuild
/// one after a removal has already committed.
///
/// Above the position the retired incarnation reached, because that is the only
/// honest place for it: the committed membership at one log index is one set,
/// and a replacement claiming an older index for a newer membership would be
/// stating that everything the newer one names and the older did not had been
/// removed in between.
const AFTER_THE_REMOVAL: LogIndex = LogIndex(7);

/// Whether one write refusal names this driver's own service state.
///
/// A typed variant and a typed reason, which is what changed: these refusals
/// used to ride `WriteError::Transport` with a crate-private cause, so a test —
/// like any external client — could only read them by rendering the error and
/// searching the string. Both facts a caller needs are now values: the reason,
/// and the fate, which is `NotAppended` from the variant alone.
fn write_refused_for(error: &WriteError, expected: DriverUnavailableReason) -> bool {
    let WriteError::Unavailable { reason } = error else {
        return false;
    };
    *reason == expected && error.fate() == WriteFate::NotAppended
}

fn read_refused_for(error: &ReadError, expected: DriverUnavailableReason) -> bool {
    matches!(error, ReadError::Unavailable { reason } if *reason == expected)
}

fn a_write(driver: &ScriptedDriver) -> Result<LocalProposalId, WriteError> {
    driver
        .begin_write(
            ("key".to_owned(), "value".to_owned()),
            WriteOptions::default(),
        )
        .map(|(local_proposal_id, _future)| local_proposal_id)
}

fn a_read(driver: &ScriptedDriver) -> Result<ReadId, ReadError> {
    driver
        .begin_read("key".to_owned(), ReadOptions::default())
        .map(|(read_id, _future)| read_id)
}

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

// ---------------------------------------------------------------------------
// A `(group, NodeId)` pair is single-use, and a committed removal spends it.
//
// The kernel keeps no removed-node tombstones and cannot grow one, so a
// contract-violating membership naming a retired ID can reach this driver. It
// is the last layer that can still refuse — the transport fence a removal
// installed is permanent, and there is no unfence to undo it with — so it
// refuses, reports, and does not try to repair.
// ---------------------------------------------------------------------------

/// A retired replica the cluster commits back in is refused on this driver's
/// own authority, whatever the link layer holds.
///
/// The link never took the policy here, which is what makes the driver's own
/// check the only admission control in play. Under the previous contract the
/// committed re-admission retracted the obligation and put the replica back in
/// `known_members`, so this frame was *accepted*: a replica whose removal the
/// cluster had committed, whose retirement had never landed, voting here on the
/// strength of a change that should never have been proposed.
#[test]
fn a_readmitted_retired_replica_is_refused_while_its_retirement_is_unpublished() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let handle = runtime.handle();
    change_on_step(&handle, &[1, 2], &[1, 2]);
    let (driver, transport) = scripted_driver(runtime, Nameable::all());
    // The link refuses every publication for the length of the test.
    transport.refuse_next_peer_updates(64);

    driver.tick().expect("the tick advances the protocol");

    assert!(
        driver.peer_policy_is_stale(),
        "the committed removal licensed a policy the link would not take"
    );
    assert_eq!(
        driver.readmitted_retired_peers(),
        0,
        "and nothing is wrong yet: a removal that stays removed violates nothing"
    );

    // The deployment reuses node 3's ID, and the cluster commits it back in.
    change_on_step(&handle, &[1, 2, 3], &[1, 2, 3]);
    driver.tick().expect("the tick advances the protocol");

    assert_eq!(
        driver.readmitted_retired_peers(),
        1,
        "a spent identity is named again, and that is the number to alert on"
    );
    assert!(
        !transport.retires(NodeId(3)),
        "the link layer still holds nothing that retires it, which is what \
         leaves the driver's own check as the only control in play"
    );
    let refused = driver.deliver(a_vote(NodeId(3)));

    assert!(
        matches!(
            refused,
            Err(InboundEnvelopeError::NotInMembership { node_id: NodeId(3) })
        ),
        "the driver refuses a spent identity whatever the committed membership \
         says, and with nothing published it is the only layer that can, got \
         {refused:?}"
    );
    assert_eq!(
        driver.refused_non_member_frames(),
        1,
        "and the refusal is counted rather than silent"
    );

    // And when the link recovers, the statement it finally takes still excludes
    // node 3: refusing the retired replica is the answer, not a publication
    // still being attempted.
    transport.allow_peer_updates();
    driver.tick().expect("the tick advances the protocol");
    assert_eq!(
        transport.peer_sets().last().expect("a set was published"),
        &vec![Principal::for_node(NodeId(2))],
        "node 3 is not published back into the peer set"
    );
    assert!(
        transport.retires(NodeId(3)),
        "and the floor that came with it retires the readmitted identity: {:?}",
        transport.policies().last()
    );
    assert!(!driver.peer_policy_is_stale());
}

/// A retired replica whose fence the link *accepted* never comes back, and
/// nothing asks the link to take it back.
///
/// This is the case that decides the design. `RaftTransport::fence_peer` is
/// permanent and has no inverse, so once the fence lands there is no operation
/// that could re-authorize node 3 — a driver that treated the committed
/// re-admission as an instruction would publish a peer set naming a principal
/// its own link layer will refuse forever, and then report itself level. It
/// keeps the replica out instead, and says so.
#[test]
fn a_readmitted_retired_replica_never_asks_for_an_unfence() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let handle = runtime.handle();
    change_on_step(&handle, &[1, 2], &[1, 2]);
    let (driver, transport) = scripted_driver(runtime, Nameable::all());

    driver.tick().expect("the tick advances the protocol");

    assert!(
        transport.retires(NodeId(3)),
        "the link took the fence, which is the fixture's whole point"
    );
    assert!(!driver.peer_policy_is_stale(), "so nothing is owed");
    let policies_before = transport.policies();
    let published_before = transport.peer_sets().len();

    // The deployment reuses node 3's ID, and the cluster commits it back in.
    change_on_step(&handle, &[1, 2, 3], &[1, 2, 3]);
    driver.tick().expect("the tick advances the protocol");
    driver
        .drive_pending_reads()
        .expect("a second entry point, in case retrying were tempted anywhere");

    assert_eq!(
        driver.readmitted_retired_peers(),
        1,
        "the violation is visible even though no obligation is outstanding"
    );
    assert_eq!(
        transport.peer_sets().len(),
        published_before,
        "and the link layer was told nothing: peer_sets = {:?}",
        transport.peer_sets()
    );
    assert_eq!(
        transport.policies(),
        policies_before,
        "no second statement, and nothing that un-retires — the driver simply \
         never authorizes a spent identity, so the policy does not move"
    );
    assert!(
        transport.retires(NodeId(3)),
        "and the retirement the removal installed still stands"
    );

    let refused = driver.deliver(AuthenticatedPeerEnvelope {
        group_id: GROUP,
        authenticated_peer: Principal::for_node(NodeId(3)),
        raft_from: NodeId(3),
        raft_to: NodeId(1),
        message: Message::RequestVote(RequestVote {
            term: Term(1),
            candidate_id: NodeId(3),
            last_log_index: LogIndex(5),
            last_log_term: Term(1),
        }),
    });

    assert!(
        matches!(refused, Err(InboundEnvelopeError::Rejected { .. })),
        "the fence is the outer control and answers first here, which is what \
         permanent means; the driver's own check stands behind it, got {refused:?}"
    );
}

/// A fence retried against a compliant directory resolves the *retired*
/// replica's mapping, and a replacement under a fresh ID joins beside it.
///
/// Both halves of the directory's obligation, in one scenario. The driver holds
/// the outstanding fence as a `NodeId` and asks `principal_for_node` again at
/// each retry, so a directory that dropped node 3 the moment its removal
/// committed would leave the fence permanently unmade, so the directory had to
/// keep the retired mapping resolvable until the fence was accepted. It no
/// longer does: the retirement names no principal, so the retry re-states the
/// same floor whether or not node 3 can still be named — and the replacement
/// joins under a fresh identity with a principal of its own either way.
#[test]
fn a_retried_retirement_needs_no_mapping_while_a_fresh_id_joins() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let handle = runtime.handle();
    change_on_step(&handle, &[1, 2], &[1, 2]);
    // The directory has already forgotten node 3, which the validator contract
    // now permits: no per-removed-principal lookup is ever made.
    let (driver, transport) = scripted_driver_authorizing(
        runtime,
        Nameable::only(&[NodeId(1), NodeId(2), NodeId(4)]),
        &[NodeId(2), NodeId(3), NodeId(4)],
    );
    transport.refuse_next_peer_updates(1);

    driver.tick().expect("the tick advances the protocol");

    assert!(
        driver.peer_policy_is_stale(),
        "the link refused the policy, so the statement is outstanding"
    );

    // The replacement joins under a fresh ID, and the same step re-states the
    // policy that retires the old one.
    change_on_step(&handle, &[1, 2, 4], &[1, 2, 4]);
    driver.tick().expect("the tick advances the protocol");

    assert!(
        transport.retires(NodeId(3)),
        "so the retry landed, without this deployment ever naming node 3: {:?}",
        transport.policies().last()
    );
    assert!(!driver.peer_policy_is_stale(), "and nothing is owed");
    assert_eq!(
        transport.peer_sets().last().expect("a set was published"),
        &vec![
            Principal::for_node(NodeId(2)),
            Principal::for_node(NodeId(4))
        ],
        "the replacement is authorized under its own principal, and the retired \
         replica is not"
    );
    assert_eq!(
        driver.readmitted_retired_peers(),
        0,
        "a fresh ID is not a readmission, which is the whole point of using one"
    );

    let joined = driver.deliver(AuthenticatedPeerEnvelope {
        group_id: GROUP,
        authenticated_peer: Principal::for_node(NodeId(4)),
        raft_from: NodeId(4),
        raft_to: NodeId(1),
        message: Message::RequestVote(RequestVote {
            term: Term(1),
            candidate_id: NodeId(4),
            last_log_index: LogIndex(5),
            last_log_term: Term(1),
        }),
    });

    assert!(
        joined.is_ok(),
        "the replacement speaks immediately, got {joined:?}"
    );
}

/// Restarting a replica is not removing it, and nothing about retirement
/// touches one that comes back.
///
/// The distinction the whole contract rests on, and it has a real consumer: the
/// reference fenced-lock service kills replicas and restarts them from their own
/// durable state under the same node ID. That is legitimate and stays
/// legitimate. Retirement is created by a *committed removal* and by nothing
/// else, so a peer that stops talking and starts again — however long the gap,
/// whatever it did in between — is a member the whole time.
///
/// The driver cannot even see the restart, which is the honest form of the
/// claim: a restart produces no membership event, so there is no fact here to
/// react to.
#[test]
fn a_replica_that_restarts_under_its_own_id_is_untouched_by_retirement() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let (driver, transport) = scripted_driver(runtime, Nameable::all());
    let published_at_adoption = transport.peer_sets();

    let vote = |term: u64| AuthenticatedPeerEnvelope {
        group_id: GROUP,
        authenticated_peer: Principal::for_node(NodeId(3)),
        raft_from: NodeId(3),
        raft_to: NodeId(1),
        message: Message::RequestVote(RequestVote {
            term: Term(term),
            candidate_id: NodeId(3),
            last_log_index: LogIndex(5),
            last_log_term: Term(1),
        }),
    };

    driver.deliver(vote(1)).expect("node 3 is a member");

    // Node 3 is killed. Nothing arrives from it, and this driver keeps running.
    for _ in 0..8 {
        driver.tick().expect("the tick advances the protocol");
    }

    // It comes back from its own durable state, under the same ID and the same
    // principal, campaigning in a later term.
    driver
        .deliver(vote(2))
        .expect("a restarted replica is the same member it was");

    assert!(
        !transport.retires(NodeId(2)),
        "no removal committed, so no retirement was ever licensed"
    );
    assert_eq!(
        driver.readmitted_retired_peers(),
        0,
        "and no identity was spent, so none was reused"
    );
    assert_eq!(driver.refused_non_member_frames(), 0, "nothing was refused");
    assert!(
        !transport.retires(NodeId(3)),
        "and the link layer was never told to fence it"
    );
    assert_eq!(
        transport.peer_sets(),
        published_at_adoption,
        "the peer set published at adoption is still the only one: a restart is \
         not a membership fact"
    );
}

// ---------------------------------------------------------------------------
// Adoption is a publication like any other, and derives from the same fact.
//
// `TransportRaftDriver::new` and `TransportRaftDriver::adopt_group` are the two
// public entry points that publish without an event to carry the membership, so
// they are the two that have to read the authority for themselves.
// ---------------------------------------------------------------------------

/// An appended-but-uncommitted removal must not take authorization away at
/// adoption, for the reason the routed `Appended` arm refuses to: an
/// uncommitted change can still be reverted.
///
/// Nothing repairs it if it is. No `Applied` fires, because the committed
/// membership never moved, and no `Appended` fires, because this driver has no
/// input that carries a membership request — so the replica is cut off for as
/// long as this incarnation runs.
#[test]
fn an_uncommitted_removal_does_not_narrow_the_peer_set_at_adoption() {
    // Committed {1,2,3}; effective {1,2}, a removal of node 3 that has appended
    // and has not committed.
    let runtime = ScriptedMembershipRuntime::new(&[1, 2], &[1, 2, 3]);
    let (driver, transport) = scripted_driver(runtime, Nameable::all());

    assert_eq!(
        transport.peer_sets(),
        vec![vec![
            Principal::for_node(NodeId(2)),
            Principal::for_node(NodeId(3))
        ]],
        "node 3's removal has not committed, so node 3 may still speak"
    );
    assert!(
        !transport.retires(NodeId(3)),
        "and an uncommitted removal licenses no fence either"
    );
    assert_eq!(driver.refused_peer_updates(), 0);
}

/// The mirror clause: an appended-but-uncommitted *addition* is published at
/// adoption, because a joining replica has to be able to speak before the
/// change commits or the change can never commit.
///
/// This is what stops the fix above from being "publish the committed set":
/// a replica that rebuilt its runtime from durable storage can hold either kind
/// of change in its log, and only one of them may narrow.
#[test]
fn an_uncommitted_addition_widens_the_peer_set_at_adoption() {
    // Committed {1,2}; effective {1,2,3}, an addition of node 3 in flight.
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2]);
    let (driver, transport) = scripted_driver(runtime, Nameable::all());

    assert_eq!(
        transport.peer_sets(),
        vec![vec![
            Principal::for_node(NodeId(2)),
            Principal::for_node(NodeId(3))
        ]],
        "node 3 is catching up under an uncommitted change and must be able to"
    );
    assert!(!transport.retires(NodeId(3)));
    assert_eq!(driver.refused_peer_updates(), 0);
}

/// A committed removal observed across a release and re-adoption installs both
/// admission controls, not one of them.
///
/// This is the supervisor pattern the driver documents — release, rebuild the
/// runtime from durable storage, adopt — and it is the one path on which a
/// committed change arrives with no event to announce it. The driver still
/// holds `known_members` from before the release, so the difference is there
/// to be taken.
#[test]
fn a_committed_removal_across_release_and_adopt_narrows_and_fences() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let handle = runtime.handle();
    let (driver, transport) = scripted_driver(runtime, Nameable::all());

    assert_eq!(
        transport.peer_sets(),
        vec![vec![
            Principal::for_node(NodeId(2)),
            Principal::for_node(NodeId(3))
        ]],
        "adoption publishes the whole membership"
    );

    let group = driver.release_group().expect("the driver holds a group");
    // While detached, the cluster commits node 3's removal and this replica's
    // rebuilt runtime reports it.
    set_membership(&handle, &[1, 2], &[1, 2]);
    driver
        .adopt_group(group, Vec::new())
        .expect("the released group is adoptable");

    assert_eq!(
        transport.peer_sets().last().expect("a set was published"),
        &vec![Principal::for_node(NodeId(2))],
        "the narrowed set reached the link layer"
    );
    assert!(
        transport.retires(NodeId(3)),
        "and so did the fence the same committed fact licenses; \
         refused_peer_updates = {}",
        driver.refused_peer_updates(),
    );
    assert!(
        !transport.retires(NodeId(2)),
        "node 2 is still committed and must still be able to speak"
    );
    assert_eq!(driver.refused_peer_updates(), 0);
}

/// One committed removal, two ways of observing it, one answer.
///
/// The control for the pair above. A driver whose two publication paths derived
/// their fence from different facts gave two answers to the same question, and
/// only one of them was the safe one; this asserts they agree rather than
/// asserting each separately and hoping.
#[test]
fn a_committed_removal_fences_the_same_way_on_both_publication_paths() {
    // Path A: observed across release and re-adoption.
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let handle = runtime.handle();
    let (driver, adopted) = scripted_driver(runtime, Nameable::all());
    let group = driver.release_group().expect("the driver holds a group");
    set_membership(&handle, &[1, 2], &[1, 2]);
    driver
        .adopt_group(group, Vec::new())
        .expect("the released group is adoptable");

    // Path B: observed as a routed `Applied` event.
    let (ticked_driver, ticked) = shrink_driver(Nameable::all());
    ticked_driver
        .tick()
        .expect("the tick routes the membership change");

    assert_eq!(
        adopted.retires(NodeId(3)),
        ticked.retires(NodeId(3)),
        "one committed removal of node 3, two ways of observing it: across \
         release/adopt fenced = {} (refused_peer_updates = {}); through a routed \
         Applied event fenced = {} (refused_peer_updates = {})",
        adopted.retires(NodeId(3)),
        driver.refused_peer_updates(),
        ticked.retires(NodeId(3)),
        ticked_driver.refused_peer_updates(),
    );
    assert_eq!(
        adopted.peer_sets().last(),
        ticked.peer_sets().last(),
        "and the peer set they publish for it is the same set"
    );
    assert!(
        adopted.retires(NodeId(3)),
        "both paths fence, rather than both agreeing not to"
    );
}

// ---------------------------------------------------------------------------
// Adoption is where an identity is installed, so adoption is where a spent one
// has to be refused.
//
// `adopt_group` assigned the incoming group's node ID to the driver with no
// question asked, which let a driver that had watched node 3's removal commit
// come back *as* node 3 — installing an identity whose principal every other
// replica has permanently fenced, and reporting success.
// ---------------------------------------------------------------------------

/// A group whose node ID a committed removal already spent is refused, and the
/// driver is left exactly as it was.
///
/// Typed rather than counted, because this is not a link-layer condition a retry
/// could clear: the ID is gone for the life of the group, and the only correct
/// answer is for the supervisor to allocate a fresh one. Refused before any
/// state moves, so a driver that raises it has installed nothing — the very next
/// adoption of a legitimate identity works.
#[test]
fn adopting_a_spent_node_id_is_refused_and_installs_nothing() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let handle = runtime.handle();
    change_on_step(&handle, &[1, 2], &[1, 2]);
    let (driver, _transport) = scripted_driver(runtime, Nameable::all());

    driver.tick().expect("the tick advances the protocol");
    let group = driver.release_group().expect("the driver holds a group");
    drop(group);

    let refused = driver.adopt_group(
        RaftGroup::new(
            GROUP,
            NodeId(3),
            ScriptedMembershipRuntime::for_node(NodeId(3), &[1, 2, 3], &[1, 2, 3]),
            KvStateMachine::default(),
        ),
        Vec::new(),
    );

    assert!(
        matches!(
            refused,
            Err(ManagedDriverError::RetiredNodeId { node_id: NodeId(3) })
        ),
        "node 3's identity was spent by the committed removal this driver \
         watched, got {refused:?}"
    );
    assert!(
        matches!(driver.with_group(|_| ()), Err(ManagedDriverError::NoGroup)),
        "the refusal installed no group"
    );

    // The supervisor allocates a fresh identity, and the driver takes it — which
    // is the evidence that the refusal moved nothing rather than the claim.
    driver
        .adopt_group(
            RaftGroup::new(
                GROUP,
                NodeId(4),
                ScriptedMembershipRuntime::for_node(NodeId(4), &[1, 2, 4], &[1, 2, 4]),
                KvStateMachine::default(),
            ),
            Vec::new(),
        )
        .expect("a fresh identity above the high-water mark is adoptable");
}

/// Releasing and re-adopting under the *same* ID stays valid, because nothing
/// was removed.
///
/// The control, and the clause a spent-ID gate is most likely to break. A
/// supervisor that rebuilds its runtime from durable storage and adopts the same
/// replica back has retired nothing: the ID is still in the committed
/// membership, so it was never spent, so there is nothing to refuse.
#[test]
fn releasing_and_re_adopting_the_same_id_stays_valid() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let (driver, _transport) = scripted_driver(runtime, Nameable::all());

    let group = driver.release_group().expect("the driver holds a group");

    driver
        .adopt_group(group, Vec::new())
        .expect("no removal committed, so node 1's identity is still live");
    assert_eq!(
        driver.service_state(),
        DriverServiceState::Serving,
        "and the driver is serving, not decommissioned"
    );
}

// ---------------------------------------------------------------------------
// The local replica is retired by the same fact as any other, and its fence is
// deferred rather than dropped.
// ---------------------------------------------------------------------------

/// A committed removal of the local replica decommissions this driver.
///
/// The removed diff used to filter the local node out before it reached either
/// the fence set or the retirement set, so a driver the cluster removed observed
/// its own removal and recorded nothing — it kept admitting client writes into a
/// replica no quorum would ever count again. The identity is spent like any
/// other now, and the consequence is a typed state rather than silence.
///
/// The group stays. Stepping down is not the whole lifecycle: the durable log is
/// still there, the runtime is still live, and the supervisor's move is
/// `release_group` and then adopt a fresh identity. What ends is client service.
#[test]
fn a_committed_removal_of_the_local_replica_decommissions_the_driver() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let handle = runtime.handle();
    change_on_step(&handle, &[2, 3], &[2, 3]);
    let (driver, transport) =
        scripted_driver_authorizing(runtime, Nameable::all(), &[NodeId(2), NodeId(3)]);

    assert_eq!(
        driver.service_state(),
        DriverServiceState::Serving,
        "nothing has been removed yet"
    );

    driver.tick().expect("the tick advances the protocol");

    assert_eq!(
        driver.service_state(),
        DriverServiceState::Decommissioned { node_id: NodeId(1) },
        "the cluster committed this replica out"
    );

    let refused_write = a_write(&driver).expect_err("a decommissioned driver takes no writes");
    assert!(
        write_refused_for(&refused_write, DriverUnavailableReason::Decommissioned),
        "the refusal says why: {refused_write:?}"
    );
    let refused_read = a_read(&driver).expect_err("a decommissioned driver takes no reads");
    assert!(
        read_refused_for(&refused_read, DriverUnavailableReason::Decommissioned),
        "and a read hears the same thing: {refused_read:?}"
    );

    assert!(
        driver.with_group(RaftGroup::node_id).is_ok(),
        "the group is still here: release is the supervisor's call, not the \
         cluster's"
    );
    driver
        .tick()
        .expect("and the protocol still advances, so the log can still catch up");
    assert!(
        transport.retires(NodeId(1)),
        "and its own policy retires the identity the cluster spent, at once: a \
         node is not a peer of itself, so nothing about that cuts off the \
         replication it still needs"
    );
}

/// A self-removal is retired like any other, with no deferral to arrange.
///
/// The obligation is real: every other replica fences node 1's principal, and
/// this driver owes its own link layer the same statement. It cannot make it
/// while it *is* node 1 — that would cut off its own inbound frames — so the
/// flush skips the entry without removing it, and the first adoption under a
/// different identity discharges it like any other.
#[test]
fn the_local_replicas_retirement_needs_no_deferral() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let handle = runtime.handle();
    change_on_step(&handle, &[2, 3], &[2, 3]);
    let (driver, transport) =
        scripted_driver_authorizing(runtime, Nameable::all(), &[NodeId(2), NodeId(3), NodeId(4)]);

    driver.tick().expect("the tick advances the protocol");

    assert!(
        transport.retires(NodeId(1)),
        "the committed removal of node 1 retires it like any other, at once: \
         {:?}",
        transport.policies().last()
    );
    assert_eq!(
        driver.service_state(),
        DriverServiceState::Decommissioned { node_id: NodeId(1) },
        "and the driver knows what it has become"
    );

    // **And that costs the stepping-down replica nothing**, which is what made
    // the deferral necessary before: a policy retiring the local identity says
    // who may speak *to* this node, and a node is never a peer of itself. Its
    // peers are still authorized and still get through, so it can still receive
    // enough of the log to be useful until the supervisor lets go of it.
    driver
        .deliver(AuthenticatedPeerEnvelope {
            group_id: GROUP,
            authenticated_peer: Principal::for_node(NodeId(2)),
            raft_from: NodeId(2),
            raft_to: NodeId(1),
            message: Message::RequestVote(RequestVote {
                term: Term(1),
                candidate_id: NodeId(2),
                last_log_index: LogIndex(5),
                last_log_term: Term(1),
            }),
        })
        .expect("a committed member may still speak to a replica stepping down");

    let group = driver.release_group().expect("the driver holds a group");
    drop(group);
    driver
        .adopt_group(
            RaftGroup::new(
                GROUP,
                NodeId(4),
                ScriptedMembershipRuntime::for_node_at(
                    NodeId(4),
                    &[2, 3, 4],
                    &[2, 3, 4],
                    AFTER_THE_REMOVAL,
                ),
                KvStateMachine::default(),
            ),
            Vec::new(),
        )
        .expect("a fresh identity is adoptable");

    assert!(
        transport.retires(NodeId(1)),
        "and the fresh incarnation's own policy keeps retiring what it used to \
         be: {:?}",
        transport.policies().last()
    );
    assert!(!driver.peer_policy_is_stale(), "and nothing is owed");
    assert_eq!(
        driver.service_state(),
        DriverServiceState::Serving,
        "the fresh identity serves"
    );
}

/// The retired local ID stays refused when a later membership names it again.
///
/// Same rule as for a peer, reached the other way round. The identity was spent
/// by the committed removal and the `(group, NodeId)` pair is consumed, so a
/// committed configuration naming node 1 again is a contract violation and is
/// reported as one rather than obeyed.
#[test]
fn the_retired_local_id_stays_refused_when_a_later_membership_names_it() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let handle = runtime.handle();
    change_on_step(&handle, &[2, 3], &[2, 3]);
    let (driver, _transport) = scripted_driver_authorizing(
        runtime,
        Nameable::all(),
        &[NodeId(1), NodeId(2), NodeId(3), NodeId(4)],
    );

    driver.tick().expect("the tick advances the protocol");
    let group = driver.release_group().expect("the driver holds a group");
    drop(group);

    let replacement = ScriptedMembershipRuntime::for_node_at(
        NodeId(4),
        &[2, 3, 4],
        &[2, 3, 4],
        AFTER_THE_REMOVAL,
    );
    let replacement_handle = replacement.handle();
    driver
        .adopt_group(
            RaftGroup::new(GROUP, NodeId(4), replacement, KvStateMachine::default()),
            Vec::new(),
        )
        .expect("a fresh identity is adoptable");

    // The deployment reuses node 1's ID and the cluster commits it back in.
    change_on_step(&replacement_handle, &[1, 2, 3, 4], &[1, 2, 3, 4]);
    driver.tick().expect("the tick advances the protocol");

    assert_eq!(
        driver.readmitted_retired_peers(),
        1,
        "node 1's identity was spent, and naming it again does not un-spend it"
    );
    assert!(
        !driver.peer_policy_is_stale(),
        "and the driver's own policy already retires it, with nothing to take \
         back"
    );

    let refused = driver.deliver(AuthenticatedPeerEnvelope {
        group_id: GROUP,
        authenticated_peer: Principal::for_node(NodeId(1)),
        raft_from: NodeId(1),
        raft_to: NodeId(4),
        message: Message::RequestVote(RequestVote {
            term: Term(1),
            candidate_id: NodeId(1),
            last_log_index: LogIndex(5),
            last_log_term: Term(1),
        }),
    });

    assert!(
        refused.is_err(),
        "the readmitted replica is refused, got {refused:?}"
    );
}

// ---------------------------------------------------------------------------
// Spent-ness is derived from a high-water mark, and the contract that makes
// that sound is monotonic allocation.
// ---------------------------------------------------------------------------

/// An ID below the high-water mark that was never committed is refused, and that
/// is the cost of deriving spent-ness instead of remembering it.
///
/// "Fresh" means *greater than anything this group has ever committed*, not
/// merely unused. A deployment that allocates into a gap below the mark has
/// broken the allocation contract, and the driver fails closed: the ID is
/// treated as spent, kept out of the peer set, and refused inbound. Stated
/// rather than hidden, because a deployment that hits it needs to hear the real
/// reason.
#[test]
fn an_id_allocated_into_a_gap_below_the_high_water_mark_is_refused() {
    // The group has committed node 5, so the high-water mark is 5 and nothing at
    // or below it is allocatable any more.
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 5], &[1, 2, 5]);
    let handle = runtime.handle();
    let (driver, _transport) =
        scripted_driver_authorizing(runtime, Nameable::all(), &[NodeId(2), NodeId(3), NodeId(5)]);

    assert_eq!(
        driver.readmitted_retired_peers(),
        0,
        "nothing is wrong yet: every committed ID is live"
    );

    // The deployment allocates node 3, which is below the mark.
    change_on_step(&handle, &[1, 2, 3, 5], &[1, 2, 3, 5]);
    driver.tick().expect("the tick advances the protocol");

    assert_eq!(
        driver.readmitted_retired_peers(),
        1,
        "node 3 is at or below the high-water mark and was never committed, so \
         the driver reads it as spent and says so"
    );
    let refused = driver.deliver(a_vote(NodeId(3)));
    assert!(
        matches!(
            refused,
            Err(InboundEnvelopeError::Rejected {
                source: AuthenticatedPeerEnvelopeError::FencedPeer { node_id: NodeId(3) }
            })
        ),
        "and refuses it rather than authorizing an ID the contract forbids, \
         got {refused:?}"
    );
}

/// An addition that never committed spends nothing, so the same ID can be
/// committed later.
///
/// The window a retirement set built from the *effective* configuration got
/// wrong. Node 3 joins under a change that appends and is then rolled back: it
/// was never in a committed configuration, so the high-water mark never reached
/// it and its disappearance retires nothing. When the cluster later admits it
/// for real, it is an ordinary joiner.
#[test]
fn an_addition_that_never_committed_can_be_committed_later() {
    // Committed {1,2}; effective {1,2,3}, an addition of node 3 in flight.
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2]);
    let handle = runtime.handle();
    let (driver, transport) = scripted_driver(runtime, Nameable::all());

    // A new leader takes the uncommitted addition back.
    change_on_step(&handle, &[1, 2], &[1, 2]);
    driver.tick().expect("the tick advances the protocol");

    assert!(
        !transport.retires(NodeId(3)),
        "nothing committed and nothing was removed, so no retirement is \
         licensed: {:?}",
        transport.policies().last()
    );
    assert_eq!(
        driver.readmitted_retired_peers(),
        0,
        "and node 3's identity was never spent"
    );

    // The cluster admits node 3 for real this time.
    change_on_step(&handle, &[1, 2, 3], &[1, 2, 3]);
    driver.tick().expect("the tick advances the protocol");

    assert_eq!(
        driver.readmitted_retired_peers(),
        0,
        "a first admission is not a readmission"
    );
    assert_eq!(
        transport.peer_sets().last().expect("a set was published"),
        &vec![
            Principal::for_node(NodeId(2)),
            Principal::for_node(NodeId(3))
        ],
        "so node 3 is published and may speak"
    );
    driver
        .deliver(a_vote(NodeId(3)))
        .expect("node 3 is a member now");
}

// ---------------------------------------------------------------------------
// There is no structure holding exact identities left to bound, and that is why
// the bound left with it.
// ---------------------------------------------------------------------------

/// A link layer that refuses every publication does not degrade client service.
///
/// **The test that replaces the fence-backlog bound, and it states why the bound
/// is gone rather than merely dropping it.** A per-removal obligation queue grew
/// by one identity per committed removal and could not be capped — a committed
/// fact is not a request, so discarding an entry would have been the forgotten
/// fence the control plane exists to prevent — which left only one lever: stop
/// taking client work once the queue passed a threshold, and hope the link layer
/// recovers.
///
/// Retirement is a floor now. It is re-derived from the mark at every entry
/// point, it costs one `NodeId` whatever the group's removal history, and a link
/// layer that refuses it forever leaves the driver holding exactly the same two
/// facts it already had. There is nothing to grow, so there is nothing to
/// threshold, and refusing client work would buy nothing: the replica is a
/// correct member of a group whose link layer is behind, and the driver's own
/// inbound check is what refuses the replicas the cluster removed.
///
/// What replaces the degraded state is one alertable observation —
/// [`TransportRaftDriver::peer_policy_is_stale`] — and the retry that ends it.
#[test]
fn a_link_that_refuses_every_publication_still_serves_clients() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3, 4, 5], &[1, 2, 3, 4, 5]);
    let handle = runtime.handle();
    let (driver, transport) = scripted_driver_authorizing(
        runtime,
        Nameable::all(),
        &[NodeId(2), NodeId(3), NodeId(4), NodeId(5)],
    );
    transport.refuse_next_peer_updates(64);

    a_write(&driver).expect("a serving driver admits a write");

    // Three committed removals at once, and a link that will take none of them.
    change_on_step(&handle, &[1, 2], &[1, 2]);
    driver.tick().expect("the tick advances the protocol");

    assert!(
        driver.peer_policy_is_stale(),
        "the link layer is behind the group, and that is the one thing to alert \
         on"
    );
    for node_id in [NodeId(3), NodeId(4), NodeId(5)] {
        assert!(
            !transport.retires(node_id),
            "{node_id} is removed and its retirement is unpublished"
        );
        assert!(
            matches!(
                driver.deliver(a_vote(node_id)),
                Err(InboundEnvelopeError::NotInMembership { .. })
            ),
            "so the driver's own check is what refuses {node_id}"
        );
    }
    assert_eq!(
        driver.service_state(),
        DriverServiceState::Serving,
        "and the replica itself is a correct member of the group, so it serves"
    );
    a_write(&driver).expect("including new client work");
    assert!(
        driver.refused_peer_updates() > 0,
        "every withheld attempt is counted, which is the history beside the state"
    );

    // The link recovers, and one publication states everything at once.
    transport.allow_peer_updates();
    driver.tick().expect("the tick advances the protocol");

    assert!(!driver.peer_policy_is_stale(), "the retry brought it level");
    for node_id in [NodeId(3), NodeId(4), NodeId(5)] {
        assert!(
            transport.retires(node_id),
            "and one floor retires every identity beneath it: {:?}",
            transport.policies().last()
        );
    }
}
