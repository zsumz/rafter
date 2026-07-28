#![allow(clippy::wildcard_imports)]

//! What a restart replays, and what it must not conclude from replaying it.
//!
//! Everything here is a **real recovery**: durable Raft state on one side, a
//! destroyed driver and runtime on the other, and `recover_*` in between. That
//! is the point of the file rather than a detail of it. A live `deliver` carries
//! its configurations forward from the membership this driver already holds, so
//! the crossings and the driver's state advance together and nothing can be
//! replayed against state that has moved past it. Recovery is the one path where
//! they can come apart: the recovered runtime reports its *final* committed
//! membership, and the crossings that produced it arrive afterwards as recovery
//! outputs — historical facts, every one of them older than the state they are
//! now being computed against.
//!
//! A driver that took each of those as an ordinary committed fact read every
//! configuration below the last one as a *removal* of everything the last one
//! added. A restart therefore fenced the replicas the cluster had most recently
//! admitted, permanently, and the spent filter meant the very next crossing —
//! the one that re-names them — could not give them back.
//!
//! So each crossing carries the transition the kernel computed where the
//! chronology is known, and a replayed one proves exactly the removals it
//! proved the first time — from any state, in any order, however many times.

mod support;

use rafter::{
    AppendEntries, ConfigurationEntry, ConfigurationId, LogEntry, MembershipSet, NodeConfig,
    SharedEntries,
};
use rafter_app::group::RaftGroup;
use rafter_runtime::DurableRaftNode;
use rafter_service::{
    AuthenticatedPeerEnvelope, ControlPlaneCheckpointError, CurrentCommittedState,
    DriverServiceState, ManagedDriverError, PeerControlPlaneCheckpoint, TransportDriverOptions,
    TransportRaftDriver,
};
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
    PersistedRaftLogEntry, RaftHardState, RaftHardStateStore, RaftLogSegment,
};
use support::transport::*;
use support::*;

fn stable(config_id: u64, node_ids: &[u64]) -> ConfigurationEntry {
    ConfigurationEntry::stable(
        ConfigurationId(config_id),
        MembershipSet::new(node_ids.iter().copied().map(NodeId).collect(), Vec::new())
            .expect("test membership is valid"),
    )
}

/// The applied floor this replica's state machine opens at.
///
/// One entry below the two configurations under test, so the recovery replay
/// starts *above* it and those two are the whole of what it replays.
const APPLIED_FLOOR: LogIndex = LogIndex(1);

/// The configuration the histories below start from.
///
/// **A configuration entry rather than the replica's bootstrap membership**, and
/// that is what makes these fixtures state a history a cluster produces. A
/// committed configuration is a transition from whatever stood immediately
/// before it, and with nothing beneath index 2 that predecessor would be the
/// static membership each [`NodeConfig`] declares — which always names the local
/// replica. The node-5 variants below would then open on a log whose first
/// committed configuration *removes* node 5, which is the opposite of the
/// history they mean to state.
const STARTS_FROM: &[u64] = &[1, 2, 3];

/// One replica's durable state: the starting configuration, then the two
/// committed configurations under test above it.
fn durable_state(
    configurations: [&[u64]; 2],
) -> (InMemoryRaftHardStateStore, InMemoryRaftLogSegment) {
    let mut hard_state_store = InMemoryRaftHardStateStore::new();
    hard_state_store
        .write_hard_state(RaftHardState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex(3),
            committed_configuration: None,
        })
        .expect("durable commit floor writes");
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[
            PersistedRaftLogEntry::configuration(LogIndex(1), Term(1), stable(0, STARTS_FROM)),
            PersistedRaftLogEntry::configuration(
                LogIndex(2),
                Term(1),
                stable(1, configurations[0]),
            ),
            PersistedRaftLogEntry::configuration(
                LogIndex(3),
                Term(1),
                stable(2, configurations[1]),
            ),
        ])
        .expect("committed entries persist");
    (hard_state_store, log_segment)
}

/// Rebuilds one replica's runtime from that durable state, with the outputs the
/// recovery released.
///
/// The assertion is the fixture checking itself: these tests mean nothing unless
/// recovery really does replay both configuration entries, and a change to the
/// applied floor or the commit floor could silently stop it.
fn recovered_runtime(configurations: [&[u64]; 2]) -> (DurableRaftNode, Vec<RaftOutput>) {
    recovered_runtime_for(NodeId(1), &[NodeId(2), NodeId(3)], configurations)
}

/// The same recovery under a different local replica.
///
/// The seam a *local* membership question needs: `service_state` asks what this
/// driver's own standing in the cluster is, so the replica the effective
/// narrowing drops has to be the one running the driver.
fn recovered_runtime_for(
    node_id: NodeId,
    peers: &[NodeId],
    configurations: [&[u64]; 2],
) -> (DurableRaftNode, Vec<RaftOutput>) {
    let (hard_state_store, log_segment) = durable_state(configurations);
    let (runtime, recovery_outputs) =
        DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
            NodeConfig::new(node_id, peers.to_vec(), 5).expect("test node config is valid"),
            hard_state_store,
            log_segment,
            InMemoryRaftSnapshotStore::new(),
            APPLIED_FLOOR,
        )
        .expect("the runtime recovers above the applied floor")
        .into_parts();

    assert_eq!(
        recovery_outputs
            .iter()
            .filter(|output| matches!(output, RaftOutput::ConfigurationCommitted { .. }))
            .count(),
        2,
        "the fixture only means anything if recovery really replays both \
         crossings: {recovery_outputs:?}"
    );
    (runtime, recovery_outputs)
}

/// The state machine a recovered replica opens at.
fn recovered_app() -> KvStateMachine {
    KvStateMachine {
        applied_index: APPLIED_FLOOR,
        ..KvStateMachine::default()
    }
}

/// Opens one replica over that durable state and hands it the checkpoint a
/// previous incarnation left behind.
///
/// The whole restart, in the order a process performs it: recover the runtime,
/// build the group at the application's own applied floor, then construct the
/// driver over both the recovery outputs and the durable checkpoint.
fn recover_with(
    checkpoint: PeerControlPlaneCheckpoint<u64>,
    configurations: [&[u64]; 2],
) -> (Driver, QueueTransport) {
    recover_node_with(
        NodeId(1),
        &[NodeId(2), NodeId(3)],
        checkpoint,
        configurations,
    )
}

/// The same restart under a chosen local replica.
fn recover_node_with(
    node_id: NodeId,
    peers: &[NodeId],
    checkpoint: PeerControlPlaneCheckpoint<u64>,
    configurations: [&[u64]; 2],
) -> (Driver, QueueTransport) {
    let (opened, transport) = try_recover_node_with(node_id, peers, checkpoint, configurations);
    (opened.expect("a recovered replica opens"), transport)
}

/// The same restart, with the refusal left for the caller to inspect.
///
/// The transport comes back either way, which is the point: a checkpoint refused
/// at the door must leave the link layer untouched, and that is only checkable
/// against the transport the failed open was given.
fn try_recover_node_with(
    node_id: NodeId,
    peers: &[NodeId],
    checkpoint: PeerControlPlaneCheckpoint<u64>,
    configurations: [&[u64]; 2],
) -> (Result<Driver, ManagedDriverError>, QueueTransport) {
    let (runtime, recovery_outputs) = recovered_runtime_for(node_id, peers, configurations);
    let transport = QueueTransport::default();
    let validator = Validator {
        transport: transport.clone(),
        authorized: [NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)]
            .into_iter()
            .filter(|authorized| *authorized != node_id)
            .collect(),
        nameable: Nameable::all(),
    };
    let group =
        RaftGroup::with_applied_index(GROUP, node_id, runtime, recovered_app(), APPLIED_FLOOR);
    let opened = TransportRaftDriver::with_control_plane_checkpoint(
        group,
        recovery_outputs,
        transport.clone(),
        validator,
        TransportDriverOptions::default(),
        checkpoint,
    );
    (opened, transport)
}

fn principals(node_ids: &[u64]) -> Vec<Principal> {
    node_ids
        .iter()
        .map(|node_id| Principal::for_node(NodeId(*node_id)))
        .collect()
}

/// One `AppendEntries` from a leader carrying a configuration it has **not**
/// committed.
///
/// `leader_commit` stays at the recovered commit floor, so the entry appends and
/// takes effect without the commit index crossing it. That is the whole shape of
/// a membership change in flight, and the only shape that can move a replica's
/// effective configuration *below* its committed one.
fn append_without_committing(
    to: NodeId,
    entry: LogEntry,
) -> AuthenticatedPeerEnvelope<u64, Principal> {
    AuthenticatedPeerEnvelope {
        group_id: GROUP,
        authenticated_peer: Principal::for_node(NodeId(2)),
        raft_from: NodeId(2),
        raft_to: to,
        message: Message::AppendEntries(AppendEntries {
            term: Term(1),
            leader_id: NodeId(2),
            prev_log_index: LogIndex(3),
            prev_log_term: Term(1),
            sequence: 1,
            entries: SharedEntries::from(vec![entry]),
            leader_commit: LogIndex(3),
        }),
    }
}

/// A frame from `from`, used only to ask whether this driver admits it at all.
fn a_vote(from: NodeId, to: NodeId) -> AuthenticatedPeerEnvelope<u64, Principal> {
    AuthenticatedPeerEnvelope {
        group_id: GROUP,
        authenticated_peer: Principal::for_node(from),
        raft_from: from,
        raft_to: to,
        message: Message::RequestVote(RequestVote {
            term: Term(1),
            candidate_id: from,
            last_log_index: LogIndex(3),
            last_log_term: Term(1),
        }),
    }
}

/// The narrowing an in-flight change appends over the recovered log.
fn narrowing_to_one_and_two() -> LogEntry {
    LogEntry::configuration(Term(1), stable(3, &[1, 2]))
}

/// The two configurations an addition-only history commits.
const ONLY_ADDITIONS: [&[u64]; 2] = [&[1, 2, 3, 4], &[1, 2, 3, 4, 5]];

/// A history that admits node 5 and then removes it, ending where it began.
const ADMIT_THEN_REMOVE: [&[u64]; 2] = [&[1, 2, 3, 5], &[1, 2, 3]];

/// A restart does not retire the replicas the cluster most recently admitted.
///
/// The reviewer's case, and the one a snapshot of the endpoint alone cannot
/// catch. The log admits node 4 and then node 5; the recovered runtime reports
/// `{1,2,3,4,5}`; and the two crossings that built it arrive afterwards as
/// recovery outputs. Taken as ordinary committed facts against the endpoint,
/// crossing 2 — `{1,2,3,4}` — reads as a removal of node 5, which spends the
/// identity and owes a permanent fence for a replica the cluster requires. The
/// crossing that re-names it cannot undo that: nothing un-spends an identity, so
/// the spent filter drops node 5 out of the very fact that would restore it.
///
/// The log only ever adds, so every retirement derived from it is manufactured —
/// which makes this a statement about the driver rather than about how carefully
/// the fixture was chosen.
#[test]
fn a_restart_does_not_retire_a_member_the_replayed_history_only_ever_added() {
    let (driver, transport) =
        recover_with(PeerControlPlaneCheckpoint::empty(GROUP), ONLY_ADDITIONS);

    let checkpoint = driver.control_plane_checkpoint();
    assert_eq!(
        live_of(&checkpoint),
        [NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)]
            .into_iter()
            .collect(),
        "every replica the log admitted is still live after the restart"
    );
    assert_eq!(
        checkpoint.committed_id_high_water,
        Some(NodeId(5)),
        "the mark still names the greatest identity the log committed"
    );
    assert!(
        !transport.retires(NodeId(5)),
        "the replica the cluster admitted last was retired by its own restart"
    );
    assert!(
        !transport.retires(NodeId(4)),
        "and neither was the one before it"
    );
    assert!(
        !driver.peer_policy_is_stale(),
        "and the link layer holds exactly the policy the group requires"
    );
}

/// Recovering twice from the same durable state changes nothing the second time.
///
/// A crash *during* recovery is an ordinary crash, so the second attempt reads
/// the same log and the same checkpoint the first one wrote. Replay is therefore
/// not a one-shot operation and cannot be made correct by ordering alone: a
/// crossing recomputed against a live set that has already moved past it always
/// manufactures a removal, whichever order the first pass ran in. The offset is
/// what makes an already-incorporated fact a no-op rather than news.
#[test]
fn a_second_recovery_from_the_same_durable_state_is_a_no_op() {
    let (first, _) = recover_with(PeerControlPlaneCheckpoint::empty(GROUP), ONLY_ADDITIONS);
    let persisted = first.control_plane_checkpoint();

    let (second, transport) = recover_with(persisted.clone(), ONLY_ADDITIONS);

    assert_eq!(
        second.control_plane_checkpoint(),
        persisted,
        "the second recovery re-derived a different retirement record from the \
         same durable state"
    );
    assert!(
        !transport.retires(NodeId(5)),
        "the second recovery retired a live member"
    );
    assert!(
        !second.peer_policy_is_stale(),
        "and its link layer holds exactly what the group requires"
    );
}

/// A second recovery keeps the committed floor an effective narrowing may not
/// cross.
///
/// **The cursor gates a fold, and it was gating an assignment with it.** The
/// endpoint publication an adoption performs stands at the runtime's commit
/// index, and on a second recovery that index is exactly where the restored
/// cursor already sits — so the whole call returned early and the raw committed
/// membership was never assigned at all. Nothing showed while the effective
/// configuration agreed with it: the peer set and the inbound check read the
/// *union* of the two facts, and a union with an empty half is the other half.
///
/// A new leader appending an uncommitted narrowing is what separates them. The
/// committed floor is the whole reason the union exists — an in-flight change
/// must not de-authorize a replica that is still committed, or the joiner it
/// needs cannot speak and the change cannot commit — and with the floor empty
/// the narrowing became authoritative. Nodes 3, 4, and 5 dropped out of the peer
/// set and were refused inbound while the cluster still had them committed.
///
/// The existing second-recovery case cannot catch this. It compares checkpoints
/// and counts fences, and the raw committed membership is in neither: it is not
/// a checkpoint field, and losing it retires nothing.
#[test]
fn a_second_recovery_keeps_the_committed_floor_an_effective_narrowing_cannot_cross() {
    let (first, _) = recover_with(PeerControlPlaneCheckpoint::empty(GROUP), ONLY_ADDITIONS);
    let persisted = first.control_plane_checkpoint();
    let (second, transport) = recover_with(persisted, ONLY_ADDITIONS);

    second
        .deliver(append_without_committing(
            NodeId(1),
            narrowing_to_one_and_two(),
        ))
        .expect("a leader's append is accepted");

    assert_eq!(
        transport.peer_sets().last().expect("a set was published"),
        &principals(&[2, 3, 4, 5]),
        "the narrowing has not committed, so the committed configuration is \
         still the floor and every replica it names stays authorized"
    );
    assert!(
        matches!(second.deliver(a_vote(NodeId(5), NodeId(1))), Ok(())),
        "and a frame from a replica the cluster still has committed is admitted"
    );
    assert_eq!(
        second.refused_non_member_frames(),
        0,
        "no committed member was turned away"
    );
}

/// A second recovery does not make a still-committed local replica a non-member.
///
/// The local half of the same loss, and the one a deployment feels first. With
/// the committed floor empty, an uncommitted narrowing that drops *this* replica
/// answers `service_state` on its own: the driver reports `NotMember` and every
/// client surface refuses, so a replica the cluster has not removed stops serving
/// while the change that would remove it may still be truncated back off the log.
///
/// `NotMember` rather than `Decommissioned`, which is the tell: nothing was
/// spent, no fence was owed, and the mark and live set are exactly right. Only
/// the raw committed membership went missing.
#[test]
fn a_second_recovery_does_not_make_a_still_committed_local_replica_a_non_member() {
    let peers = [NodeId(1), NodeId(2)];
    let (first, _) = recover_node_with(
        NodeId(5),
        &peers,
        PeerControlPlaneCheckpoint::empty(GROUP),
        ONLY_ADDITIONS,
    );
    let persisted = first.control_plane_checkpoint();
    let (second, _transport) = recover_node_with(NodeId(5), &peers, persisted, ONLY_ADDITIONS);

    assert_eq!(
        second.service_state(),
        DriverServiceState::Serving,
        "the recovered replica is a member of the configuration it recovered under"
    );

    second
        .deliver(append_without_committing(
            NodeId(5),
            narrowing_to_one_and_two(),
        ))
        .expect("a leader's append is accepted");

    assert_eq!(
        second.service_state(),
        DriverServiceState::Serving,
        "node 5 is still a committed member: an appended-and-uncommitted removal \
         is not a removal, and a replica that stops serving for one abandons the \
         work the cluster still expects of it"
    );
}

/// A retirement record with no current committed state is refused before it can
/// fence a live member.
///
/// **The replay bug, resurrected through a checkpoint shape.** The record here
/// is exactly what a migration of an older file would produce: the retirement
/// state this replica really reached — mark 5, every replica the log admitted
/// still live — beside `through: None`, meaning "no configuration history
/// consumed". Every field of it is individually plausible and the whole is a
/// lie, because the state proves history *was* consumed.
///
/// Absorbed, it does not merely lose a fact. With no offset to gate them, the
/// two crossings replay against a live set that already reflects both: the
/// configuration at index 2 — `{1,2,3,4}` — reads as a removal of node 5, spends
/// the identity, and owes a permanent fence for a replica the cluster requires.
/// The log only ever adds, so every retirement derived from it is manufactured.
///
/// So the assertion is about *when*. The record is refused at the door, ahead of
/// the replay and ahead of the first transport call, which is what makes a
/// damaged file a replica that will not start rather than one this process has
/// already helped destroy.
///
/// This is also the gap between the two layers worth naming: the reference
/// consumer's decoder refuses its own older file format for precisely this
/// reason, and until this clause existed the driver accepted the same semantic
/// shape from any embedder that reached it another way.
#[test]
fn a_retirement_record_with_no_current_state_is_refused_before_any_transport_call() {
    let mut final_state = PeerControlPlaneCheckpoint::empty(GROUP);
    final_state.committed_id_high_water = Some(NodeId(5));

    let (refused, transport) = try_recover_node_with(
        NodeId(1),
        &[NodeId(2), NodeId(3)],
        final_state,
        ONLY_ADDITIONS,
    );

    assert!(
        matches!(
            refused,
            Err(ManagedDriverError::InvalidControlPlaneCheckpoint {
                reason: ControlPlaneCheckpointError::RetirementWithoutCurrentState
            })
        ),
        "a record that says what it retired and names no committed membership to \
         read it against is not a record a driver wrote: got {:?}",
        refused.map(|_| "a driver")
    );
    assert!(
        transport.peer_sets().is_empty(),
        "the refusal landed before the link layer was told anything"
    );
    assert_eq!(
        transport.retirement_floor(),
        None,
        "and before any retirement floor was raised over a log that only ever \
         added"
    );
}

/// A current committed state with nothing retired beside it is refused too.
///
/// The opposite separation, and the quiet one. The record names a committed
/// membership it claims to have observed, and observing one raises a mark to at
/// least its greatest identity — so a record holding the observation and no mark
/// has had its retirement half truncated away. Here the history admits node 5
/// and removes it again, so the endpoint carries no trace of it either:
/// absorbed, this record starts a replica that has forgotten an identity the
/// cluster spent, with no fence owed and no later fact to re-derive one from.
///
/// Refusing costs nothing, because no driver can produce it: a committed
/// configuration names at least one replica, so an observation that assigned the
/// current state raised a mark in the same call.
#[test]
fn a_current_state_with_nothing_retired_beside_it_is_refused() {
    let mut orphaned = PeerControlPlaneCheckpoint::empty(GROUP);
    orphaned.current_committed = Some(CurrentCommittedState::new(
        LogIndex(3),
        [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect(),
    ));

    let (refused, transport) = try_recover_node_with(
        NodeId(1),
        &[NodeId(2), NodeId(3)],
        orphaned,
        ADMIT_THEN_REMOVE,
    );

    assert!(
        matches!(
            &refused,
            Err(ManagedDriverError::InvalidControlPlaneCheckpoint {
                reason: ControlPlaneCheckpointError::CurrentStateWithoutRetirement
            })
        ),
        "got {:?}",
        refused.map(|_| "a driver")
    );
    assert!(
        transport.peer_sets().is_empty(),
        "the refusal landed before the link layer was told anything"
    );
    assert_eq!(
        transport.retirement_floor(),
        None,
        "and before any retirement floor was raised"
    );
}

/// A restart still spends an identity its replayed history admitted and removed.
///
/// **The other direction, and the one the offset alone cannot rescue.** Here the
/// history ends where it began — `{1,2,3}` before node 5 and `{1,2,3}` after —
/// so the endpoint the recovered runtime reports carries no trace of node 5 at
/// all. A driver that folded the endpoint in first would set its offset to the
/// commit index, skip both crossings as already-consumed, and derive its whole
/// retirement record from a membership that never mentions the identity the
/// cluster spent.
///
/// That is why the offset is not a substitute for the order. The offset makes a
/// *replayed* fact a no-op; the order is what decides whether the driver reads
/// the history or only its endpoint. Both are needed, and this is the half the
/// addition-only case above cannot show — with only additions, the endpoint
/// happens to contain everything the history did.
#[test]
fn a_restart_still_spends_an_identity_the_replayed_history_admitted_and_removed() {
    let (driver, transport) =
        recover_with(PeerControlPlaneCheckpoint::empty(GROUP), ADMIT_THEN_REMOVE);

    let checkpoint = driver.control_plane_checkpoint();
    assert_eq!(
        checkpoint.committed_id_high_water,
        Some(NodeId(5)),
        "the admission the history carried raised the mark, even though the \
         endpoint does not name node 5"
    );
    assert!(
        !live_of(&checkpoint).contains(&NodeId(5)),
        "and the removal behind it spent the identity: {:?}",
        live_of(&checkpoint)
    );
    assert_eq!(
        live_of(&checkpoint),
        [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect()
    );
    assert!(
        transport.retires(NodeId(5)),
        "and the link layer was told to stop trusting the principal it retired"
    );
}

/// Adoption reads its recovery outputs the same way construction does.
///
/// The second producer of the same shape, and it needs its own case because it
/// is a second call site rather than a second path through one. A supervisor
/// that rebuilds a replica's runtime from durable storage and adopts it — a
/// takeover, or a driver re-armed from another process's persisted state —
/// reaches exactly this, and an adoption that folded the rebuilt runtime's
/// endpoint in before its history would set the offset past the crossings and
/// then skip them, deriving its retirement record from a membership that never
/// mentions the identity the cluster spent.
///
/// The driver here already holds a group and releases it first, so this is the
/// adoption entry point and not construction wearing its name.
#[test]
fn an_adoption_still_spends_an_identity_its_recovery_outputs_admitted_and_removed() {
    let (driver, transport) = driver_for(1, &[2, 3]);
    drop(driver.release_group().expect("the driver holds a group"));

    let (runtime, recovery_outputs) = recovered_runtime(ADMIT_THEN_REMOVE);
    let rebuilt =
        RaftGroup::with_applied_index(GROUP, NodeId(1), runtime, recovered_app(), APPLIED_FLOOR);
    driver
        .adopt_group_with_checkpoint(
            rebuilt,
            recovery_outputs,
            PeerControlPlaneCheckpoint::empty(GROUP),
        )
        .expect("a rebuilt runtime is adoptable under the same group");

    let checkpoint = driver.control_plane_checkpoint();
    assert_eq!(
        checkpoint.committed_id_high_water,
        Some(NodeId(5)),
        "the admission the recovery outputs carried raised the mark"
    );
    assert!(
        !live_of(&checkpoint).contains(&NodeId(5)),
        "and the removal behind it spent the identity: {:?}",
        live_of(&checkpoint)
    );
    assert!(
        transport.retires(NodeId(5)),
        "and the link layer took the fence the removal licensed"
    );
}
