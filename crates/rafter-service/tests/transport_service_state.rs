//! Why this driver is refusing client work, made a total answer.
//!
//! Its sibling `transport_identity` pins what a committed *removal* costs. This
//! one pins the three states that are not removals and were, until now, all
//! reported as `Serving` while the driver refused everything:
//!
//! - a local replica no configuration names — a joiner rolled back, or one
//!   constructed before its addition commits;
//! - a driver that released its group;
//! - a driver that has shut down.
//!
//! The first is the one with teeth. A rolled-back replica is receiving no
//! replication, and it used to answer [`ReadConsistency::Local`] reads from its
//! own applied state on the strength of an unspent ID — an unboundedly stale
//! view that a client had no way to tell from a fresh one.

#![allow(clippy::wildcard_imports)]

mod support;

use rafter_service::{
    AuthenticatedPeerEnvelope, DriverServiceState, DriverUnavailableReason, ManagedDriverError,
    ReadOptions, TransportDriverOptions, WriteOptions,
};
use support::scripted::*;
use support::transport::*;
use support::*;

fn a_write(driver: &ScriptedDriver) -> Result<LocalProposalId, WriteError> {
    driver
        .begin_write(
            ("key".to_owned(), "value".to_owned()),
            WriteOptions::default(),
        )
        .map(|(local_proposal_id, _future)| local_proposal_id)
}

fn a_linearizable_read(driver: &ScriptedDriver) -> Result<ReadId, ReadError> {
    driver
        .begin_read("key".to_owned(), ReadOptions::default())
        .map(|(read_id, _future)| read_id)
}

/// A local read through the handle, which is the only way to ask for one.
fn a_local_read(driver: &ScriptedDriver) -> Result<(), ReadError> {
    let handle = driver.handle();
    block_on(handle.read("key".to_owned(), ReadConsistency::Local)).map(|_| ())
}

/// A driver whose local replica is rolled back out of the configuration.
///
/// Node 4 joins effectively — an addition that appended and has not committed —
/// and a new leader then truncates it back off the log. Both memberships end up
/// naming `{1,2,3}`, node 4 is in neither, and no committed removal ever named
/// it, so its ID was never spent.
fn rolled_back_local_replica() -> (ScriptedDriver, QueueTransport, ScriptedMembershipHandle) {
    let runtime = ScriptedMembershipRuntime::for_node(NodeId(4), &[1, 2, 3, 4], &[1, 2, 3]);
    let handle = runtime.handle();
    let (driver, transport) =
        scripted_driver_authorizing(runtime, Nameable::all(), &[NodeId(1), NodeId(2), NodeId(3)]);
    (driver, transport, handle)
}

/// A local joiner that is rolled back stops serving, and says which state it is
/// in.
///
/// `Decommissioned` would be the wrong answer and `Serving` was the wrong
/// answer. Nothing was spent — the addition never committed, so the ID is still
/// allocatable and the change may legitimately be proposed again — but nothing
/// names this replica either, so the cluster is not replicating to it.
#[test]
fn a_rolled_back_local_joiner_reports_not_member() {
    let (driver, _transport, handle) = rolled_back_local_replica();

    assert_eq!(
        driver.service_state(),
        DriverServiceState::Serving,
        "the effective configuration names node 4 while the change is in flight"
    );

    // A new leader wins with a log that never held the addition.
    change_on_step(&handle, &[1, 2, 3], &[1, 2, 3]);
    driver.tick().expect("the tick advances the protocol");

    assert_eq!(
        driver.service_state(),
        DriverServiceState::NotMember { node_id: NodeId(4) },
        "no configuration names this replica, and no removal spent it"
    );
    assert_eq!(
        driver.readmitted_retired_peers(),
        0,
        "and nothing was retired: a rollback is not a removal"
    );
}

/// A rolled-back replica refuses writes and **both** read levels.
///
/// The local read is the reason this state exists. It answers from this
/// replica's own applied state and proves nothing about any other, which looks
/// harmless — but this replica is receiving no replication at all, so the answer
/// is a view of the past with no bound on how far back. Serving it is the one
/// way a client could not tell a rolled-back replica from a live one.
#[test]
fn a_rolled_back_local_joiner_refuses_writes_and_both_read_levels() {
    let (driver, _transport, handle) = rolled_back_local_replica();
    change_on_step(&handle, &[1, 2, 3], &[1, 2, 3]);
    driver.tick().expect("the tick advances the protocol");

    let write = a_write(&driver).expect_err("a replica in no configuration takes no writes");
    assert!(
        matches!(
            write,
            WriteError::Unavailable {
                reason: DriverUnavailableReason::NotMember
            }
        ),
        "got {write:?}"
    );
    assert_eq!(
        write.fate(),
        WriteFate::NotAppended,
        "the driver refused before it touched the group, so nothing can commit"
    );

    let read = a_linearizable_read(&driver).expect_err("nor a linearizable read");
    assert!(
        matches!(
            read,
            ReadError::Unavailable {
                reason: DriverUnavailableReason::NotMember
            }
        ),
        "got {read:?}"
    );

    let local = a_local_read(&driver).expect_err("nor a local one, which is the point");
    assert!(
        matches!(
            local,
            ReadError::Unavailable {
                reason: DriverUnavailableReason::NotMember
            }
        ),
        "got {local:?}"
    );
}

/// The protocol keeps running, which is what lets the state end.
///
/// A replica that stopped stepping could never catch up if the change is
/// re-proposed, and a fresh joiner constructed before its addition commits could
/// never join at all.
#[test]
fn a_rolled_back_local_joiner_still_ticks_and_delivers() {
    let (driver, _transport, handle) = rolled_back_local_replica();
    change_on_step(&handle, &[1, 2, 3], &[1, 2, 3]);
    driver.tick().expect("the tick advances the protocol");

    driver
        .tick()
        .expect("ticking is how this replica stays able to rejoin");
    driver
        .deliver(AuthenticatedPeerEnvelope {
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
        })
        .expect("and deliveries from members still reach the group");
}

/// The same unspent ID becoming effective again returns the driver to service.
///
/// `NotMember` is not terminal, which is the whole distinction from
/// `Decommissioned`: the change was reverted rather than committed, so proposing
/// it again is legitimate and this replica must be able to serve when it lands.
#[test]
fn a_re_proposed_joiner_returns_to_service() {
    let (driver, _transport, handle) = rolled_back_local_replica();
    change_on_step(&handle, &[1, 2, 3], &[1, 2, 3]);
    driver.tick().expect("the tick advances the protocol");
    assert_eq!(
        driver.service_state(),
        DriverServiceState::NotMember { node_id: NodeId(4) }
    );

    // The cluster proposes the addition again, and this time it commits.
    change_on_step(&handle, &[1, 2, 3, 4], &[1, 2, 3, 4]);
    driver.tick().expect("the tick advances the protocol");

    assert_eq!(
        driver.service_state(),
        DriverServiceState::Serving,
        "an unspent ID that is named again is a member again"
    );
    a_write(&driver).expect("and the driver admits client work");
}

/// A driver constructed around an ID no configuration names starts in
/// `NotMember`.
///
/// Construction succeeds, deliberately: this is a fresh joiner built before the
/// change that adds it has committed anywhere, and refusing to build it would
/// leave no replica for the addition to catch up.
#[test]
fn construction_around_an_unnamed_node_starts_not_member() {
    let runtime = ScriptedMembershipRuntime::for_node(NodeId(4), &[1, 2, 3], &[1, 2, 3]);
    let handle = runtime.handle();
    let (driver, _transport) =
        scripted_driver_authorizing(runtime, Nameable::all(), &[NodeId(1), NodeId(2), NodeId(3)]);

    assert_eq!(
        driver.service_state(),
        DriverServiceState::NotMember { node_id: NodeId(4) },
        "no configuration names this replica yet"
    );
    let write = a_write(&driver).expect_err("so it serves nothing");
    assert!(matches!(
        write,
        WriteError::Unavailable {
            reason: DriverUnavailableReason::NotMember
        }
    ));

    // The addition commits.
    change_on_step(&handle, &[1, 2, 3, 4], &[1, 2, 3, 4]);
    driver.tick().expect("the tick advances the protocol");

    assert_eq!(driver.service_state(), DriverServiceState::Serving);
}

/// A shut-down driver reports it, and outranks everything else it could say.
///
/// Terminal for the driver: adoption is refused too, so nothing this driver
/// could otherwise report changes what happens next. It used to report
/// `Serving` while refusing every operation.
#[test]
fn a_shut_down_driver_reports_shutting_down() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let (driver, _transport) = scripted_driver(runtime, Nameable::all());
    let handle = driver.handle();

    assert_eq!(driver.service_state(), DriverServiceState::Serving);
    block_on(handle.shutdown()).expect("the driver shuts down");

    assert_eq!(
        driver.service_state(),
        DriverServiceState::ShuttingDown,
        "and the one surface a supervisor polls says so"
    );
    assert_eq!(
        DriverUnavailableReason::from_service_state(driver.service_state()),
        Some(DriverUnavailableReason::ShuttingDown),
        "which projects to the reason a client would be refused for"
    );
    assert!(matches!(a_write(&driver), Err(WriteError::ShuttingDown)));
    assert!(matches!(
        a_linearizable_read(&driver),
        Err(ReadError::ShuttingDown)
    ));
}

/// A shut-down driver still reports the control plane its embedder must persist.
///
/// The last cell of the lifecycle, and the one where forgetting costs the most.
/// Shutdown is terminal for *service* and says nothing about the record: an
/// identity a committed removal spent is still spent, and whatever replica opens
/// this durable state next has to keep refusing it. A supervisor's final act is
/// to persist what it holds, and that read happens after the shutdown it is the
/// shutdown for — so the accessor has to keep answering past the point every
/// client surface has stopped.
///
/// Shutdown also retracts nothing and publishes nothing. There is no flush here
/// and there must not be: the retirement belongs to the `(group, NodeId)` pair
/// rather than to this process, and the record is what carries it.
#[test]
fn a_shut_down_driver_still_reports_what_its_embedder_must_persist() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let handle_to_membership = runtime.handle();
    let (driver, transport) = scripted_driver(runtime, Nameable::only(&[NodeId(2)]));
    let client = driver.handle();

    // A committed removal whose policy the link layer never accepted, so the
    // statement is still outstanding when the driver is asked to stop.
    transport.refuse_next_peer_updates(64);
    change_on_step(&handle_to_membership, &[1, 2], &[1, 2]);
    driver.tick().expect("the step that commits the removal");
    let before = driver.control_plane_checkpoint();
    assert!(
        !live_of(&before).contains(&NodeId(3)),
        "the fixture only means anything with a removal recorded"
    );
    assert!(
        driver.peer_policy_is_stale(),
        "and with the link layer still behind it"
    );

    block_on(client.shutdown()).expect("the driver shuts down");

    assert_eq!(
        driver.control_plane_checkpoint(),
        before,
        "shutdown is terminal for service and changes nothing about the record"
    );
    assert!(
        driver.peer_policy_is_stale(),
        "and states nothing new on the way out"
    );
    assert!(
        !transport.retires(NodeId(3)),
        "the link layer still never took it, which is why the record is what \
         carries the retirement across the restart"
    );
}

/// Shutdown outranks a released group, which outranks everything derived from a
/// group the driver does not hold.
#[test]
fn the_service_state_reports_the_most_terminal_condition_that_holds() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let (driver, _transport) = scripted_driver(runtime, Nameable::all());
    let handle = driver.handle();

    let _ = driver.release_group().expect("the driver holds a group");
    assert_eq!(driver.service_state(), DriverServiceState::Released);

    block_on(handle.shutdown()).expect("the driver shuts down");
    assert_eq!(
        driver.service_state(),
        DriverServiceState::ShuttingDown,
        "shutdown is terminal and a release is not, so shutdown is the answer"
    );
}

/// `Serving` is the only state that is not a refusal.
#[test]
fn only_serving_projects_to_no_reason() {
    assert_eq!(
        DriverUnavailableReason::from_service_state(DriverServiceState::Serving),
        None
    );
    for (state, reason) in [
        (
            DriverServiceState::Decommissioned { node_id: NodeId(1) },
            DriverUnavailableReason::Decommissioned,
        ),
        (
            DriverServiceState::NotMember { node_id: NodeId(1) },
            DriverUnavailableReason::NotMember,
        ),
        (
            DriverServiceState::ContradictoryCurrentState {
                through: LogIndex(10),
            },
            DriverUnavailableReason::ContradictoryCurrentState,
        ),
        (
            DriverServiceState::Released,
            DriverUnavailableReason::Released,
        ),
        (
            DriverServiceState::ShuttingDown,
            DriverUnavailableReason::ShuttingDown,
        ),
    ] {
        assert_eq!(
            DriverUnavailableReason::from_service_state(state),
            Some(reason),
            "{state:?} is a refusal"
        );
    }
}

// ---------------------------------------------------------------------------
// The peer control plane across a process restart.
//
// A driver reconstructed from durable Raft state alone starts with no
// high-water mark and no fence obligations. Raft cannot give either back: the
// retirement record is the *difference* between two committed configurations,
// and compaction erases the configuration history below the snapshot boundary.
// ---------------------------------------------------------------------------

/// A removal whose fence the link refused survives a driver the process
/// rebuilds.
///
/// The reviewer's process-restart case, at the level where a driver can actually
/// be destroyed and rebuilt. The first driver watches node 5 leave the committed
/// configuration and cannot fence it; the process dies; a second driver is
/// constructed from a *fresh* runtime that reports the committed set `{1,2}` and
/// nothing else. Without the checkpoint that driver has a high-water mark of 2,
/// so node 5 is unspent and allocatable, and the fence is never retried.
#[test]
fn a_rebuilt_driver_retries_the_fence_and_keeps_the_identity_spent() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 5], &[1, 2, 5]);
    let handle = runtime.handle();
    let (driver, transport) =
        scripted_driver_authorizing(runtime, Nameable::all(), &[NodeId(2), NodeId(5)]);
    transport.refuse_next_peer_updates(64);

    change_on_step(&handle, &[1, 2], &[1, 2]);
    driver.tick().expect("the tick advances the protocol");
    assert!(
        driver.peer_policy_is_stale(),
        "the committed removal licensed a policy the link would not take"
    );
    assert!(driver.control_plane_checkpoint_epoch() > 0, "and it moved");

    // The embedder persists here, and the process dies.
    let checkpoint = driver.control_plane_checkpoint();
    assert_eq!(checkpoint.committed_id_high_water, Some(NodeId(5)));
    assert!(!live_of(&checkpoint).contains(&NodeId(5)));
    drop(driver);

    // A new process: a new transport that has accepted nothing, and a runtime
    // rebuilt from durable Raft state, which reports only `{1,2}`.
    let reopened = ScriptedMembershipRuntime::new(&[1, 2], &[1, 2]);
    let (rebuilt, rebuilt_transport) = scripted_driver_with_checkpoint(
        reopened,
        Nameable::all(),
        &[NodeId(2), NodeId(5)],
        TransportDriverOptions::default(),
        checkpoint,
    );

    assert!(
        !rebuilt.peer_policy_is_stale(),
        "the restored record was re-stated at construction and this link took it"
    );
    assert!(
        rebuilt_transport.retires(NodeId(5)),
        "which is the retirement a process restart used to forget: {:?}",
        rebuilt_transport.policies().last()
    );
    assert_eq!(
        rebuilt_transport
            .peer_sets()
            .last()
            .expect("a set was published"),
        &vec![Principal::for_node(NodeId(2))],
        "and node 5 is not in the peer set the new link layer was given"
    );
}

/// The restored mark keeps the removed identity un-adoptable.
///
/// The other half of what a crash used to lose. A supervisor that reopened this
/// replica under node 5 — the ID the cluster spent — would install an identity
/// every other replica has permanently fenced, and the replica would appear to
/// join and then never be heard from.
#[test]
fn a_rebuilt_driver_refuses_to_adopt_the_spent_identity() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 5], &[1, 2, 5]);
    let handle = runtime.handle();
    let (driver, _transport) =
        scripted_driver_authorizing(runtime, Nameable::all(), &[NodeId(2), NodeId(5)]);
    change_on_step(&handle, &[1, 2], &[1, 2]);
    driver.tick().expect("the tick advances the protocol");
    let checkpoint = driver.control_plane_checkpoint();
    drop(driver);

    let reopened = ScriptedMembershipRuntime::new(&[1, 2], &[1, 2]);
    let (rebuilt, _rebuilt_transport) = scripted_driver_with_checkpoint(
        reopened,
        Nameable::all(),
        &[NodeId(2), NodeId(5)],
        TransportDriverOptions::default(),
        checkpoint,
    );
    let _ = rebuilt.release_group().expect("the driver holds a group");

    let refused = rebuilt.adopt_group(
        RaftGroup::new(
            GROUP,
            NodeId(5),
            ScriptedMembershipRuntime::for_node(NodeId(5), &[1, 2], &[1, 2]),
            KvStateMachine::default(),
        ),
        Vec::new(),
    );

    assert!(
        matches!(
            refused,
            Err(ManagedDriverError::RetiredNodeId { node_id: NodeId(5) })
        ),
        "the restored mark is what makes this refusable, got {refused:?}"
    );
}

/// A removed local replica stays `Decommissioned` after the rebuild, rather than
/// reporting `NotMember`.
///
/// The two states are told apart by the spent test, and the spent test is
/// exactly what a crash used to erase — so without the checkpoint a replica the
/// cluster *removed* would come back reporting a condition that clears by
/// itself.
#[test]
fn a_removed_local_replica_stays_decommissioned_after_a_rebuild() {
    let runtime = ScriptedMembershipRuntime::for_node(NodeId(5), &[1, 2, 5], &[1, 2, 5]);
    let handle = runtime.handle();
    let (driver, _transport) =
        scripted_driver_authorizing(runtime, Nameable::all(), &[NodeId(1), NodeId(2)]);
    change_on_step(&handle, &[1, 2], &[1, 2]);
    driver.tick().expect("the tick advances the protocol");
    assert_eq!(
        driver.service_state(),
        DriverServiceState::Decommissioned { node_id: NodeId(5) }
    );
    let checkpoint = driver.control_plane_checkpoint();
    drop(driver);

    let reopened = ScriptedMembershipRuntime::for_node(NodeId(5), &[1, 2], &[1, 2]);
    let (rebuilt, _rebuilt_transport) = scripted_driver_with_checkpoint(
        reopened,
        Nameable::all(),
        &[NodeId(1), NodeId(2)],
        TransportDriverOptions::default(),
        checkpoint,
    );

    assert_eq!(
        rebuilt.service_state(),
        DriverServiceState::Decommissioned { node_id: NodeId(5) },
        "the removal is permanent, and a restart is not a way out of it"
    );
}

/// A driver rebuilt with no checkpoint loses both facts, which is the whole
/// reason the checkpoint exists.
///
/// The control, and it asserts the *unsafe* behaviour deliberately: a change
/// that quietly re-derived retirement from somewhere else would break here and
/// have to say where it got it from. It is also the honest statement of the
/// window's shape — the checkpoint is what closes it, and nothing else can.
#[test]
fn a_rebuilt_driver_without_a_checkpoint_forgets_the_removal() {
    let reopened = ScriptedMembershipRuntime::new(&[1, 2], &[1, 2]);
    let (rebuilt, rebuilt_transport) =
        scripted_driver_authorizing(reopened, Nameable::all(), &[NodeId(2), NodeId(5)]);

    assert_eq!(
        rebuilt.control_plane_checkpoint().committed_id_high_water,
        Some(NodeId(2)),
        "the mark falls back to what the surviving configuration names, because \
         nothing was handed over"
    );
    assert!(
        !rebuilt_transport.retires(NodeId(5)),
        "so nothing retires node 5"
    );

    let _ = rebuilt.release_group().expect("the driver holds a group");
    rebuilt
        .adopt_group(
            RaftGroup::new(
                GROUP,
                NodeId(5),
                ScriptedMembershipRuntime::for_node(NodeId(5), &[1, 2], &[1, 2]),
                KvStateMachine::default(),
            ),
            Vec::new(),
        )
        .expect(
            "the spent identity is adoptable again — the window the checkpoint \
             closes",
        );
}

/// The persist trigger moves for the changes an embedder must not lose, and for
/// nothing else.
#[test]
fn the_checkpoint_epoch_moves_only_when_the_checkpoint_does() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let handle = runtime.handle();
    let (driver, transport) = scripted_driver(runtime, Nameable::all());
    transport.refuse_next_peer_updates(1);

    let after_construction = driver.control_plane_checkpoint_epoch();
    driver.tick().expect("the tick advances the protocol");
    assert_eq!(
        driver.control_plane_checkpoint_epoch(),
        after_construction,
        "a tick that moved no configuration changes nothing"
    );

    change_on_step(&handle, &[1, 2], &[1, 2]);
    driver.tick().expect("the tick advances the protocol");
    let after_removal = driver.control_plane_checkpoint_epoch();
    assert!(
        after_removal > after_construction,
        "a committed removal moves the mark and the current committed state"
    );

    // **And a publication the link finally accepts moves nothing.** That is the
    // change: the epoch used to advance when a fence was discharged, because
    // what the link layer had taken was part of the record. It no longer is —
    // the record says what this driver has *spent*, and a policy accepted or
    // refused says nothing about that.
    driver.tick().expect("the tick advances the protocol");
    assert!(
        transport.retires(NodeId(3)),
        "the retry landed: {:?}",
        transport.policies().last()
    );
    assert_eq!(
        driver.control_plane_checkpoint_epoch(),
        after_removal,
        "and the embedder is not asked to persist anything for it"
    );
}
