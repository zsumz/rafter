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
//! So a checkpoint carries a consumer offset beside the retirement record, the
//! two move together, and a crossing at or below it is a fact this driver has
//! already incorporated rather than news.

mod support;

use rafter::{
    AppendEntries, ConfigurationEntry, ConfigurationId, LogEntry, MembershipSet, NodeConfig,
    SharedEntries,
};
use rafter_app::group::RaftGroup;
use rafter_runtime::DurableRaftNode;
use rafter_service::{
    AuthenticatedPeerEnvelope, DriverServiceState, PeerControlPlaneCheckpoint,
    TransportDriverOptions, TransportRaftDriver,
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
/// One application entry below the configurations, so the recovery replay starts
/// *above* it and the two configuration entries are the whole of what it
/// replays.
const APPLIED_FLOOR: LogIndex = LogIndex(1);

/// One replica's durable state: a seeded application entry, then two committed
/// configurations above it.
///
/// The application entry is what the applied floor sits on, so the two
/// configurations are the whole of what recovery replays.
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
            PersistedRaftLogEntry::application(LogIndex(1), Term(1), b"seed\nvalue".to_vec()),
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
    let driver = TransportRaftDriver::with_control_plane_checkpoint(
        group,
        recovery_outputs,
        transport.clone(),
        validator,
        TransportDriverOptions::default(),
        checkpoint,
    )
    .expect("a recovered replica opens");
    (driver, transport)
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
        checkpoint.live_committed_members,
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
        checkpoint.pending_fences.is_empty(),
        "a log that removed nobody owes no fence: {:?}",
        checkpoint.pending_fences
    );
    assert!(
        !transport.is_fenced(NodeId(5)),
        "the replica the cluster admitted last was fenced by its own restart"
    );
    assert!(
        !transport.is_fenced(NodeId(4)),
        "and neither was the one before it"
    );
    assert_eq!(driver.pending_peer_fences(), 0);
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
        !transport.is_fenced(NodeId(5)),
        "the second recovery fenced a live member"
    );
    assert_eq!(second.pending_peer_fences(), 0);
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
        !checkpoint.live_committed_members.contains(&NodeId(5)),
        "and the removal behind it spent the identity: {:?}",
        checkpoint.live_committed_members
    );
    assert_eq!(
        checkpoint.live_committed_members,
        [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect()
    );
    assert!(
        transport.is_fenced(NodeId(5)),
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
        !checkpoint.live_committed_members.contains(&NodeId(5)),
        "and the removal behind it spent the identity: {:?}",
        checkpoint.live_committed_members
    );
    assert!(
        transport.is_fenced(NodeId(5)),
        "and the link layer took the fence the removal licensed"
    );
}
