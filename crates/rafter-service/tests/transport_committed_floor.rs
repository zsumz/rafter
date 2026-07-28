#![allow(clippy::wildcard_imports)]

//! The committed floor a driver publishes, and why it is not part of the
//! retirement record.
//!
//! Sibling of [`transport_committed_transition`], split along the line between
//! the two questions a committed configuration answers. That file asks what a
//! restart may *conclude*: which identities a replayed history spends, and which
//! of two observations of the current membership this driver believes. This one
//! asks what the driver may *publish* — the raw committed membership the peer
//! set and the inbound check read as the floor an uncommitted change cannot
//! narrow past.
//!
//! The two came apart at the one lifecycle cell where a handed-over record
//! stands *ahead* of the runtime it is joined into. A rebuilt runtime's commit
//! index is volatile, so a supervisor handing a replica's durable record to a
//! runtime that recovered behind it is ordinary rather than exotic — the public
//! contract says a stale checkpoint is a legal input, and says nothing that
//! would make an ahead one illegal.
//!
//! Under such a record the retirement derivations correctly conclude nothing
//! from the runtime's older observations, and the raw floor used to ride along
//! with them. The floor is not a conclusion. It is the answer to "what does this
//! replica's own stream say the cluster has committed now", a durable record has
//! no opinion about that, and a floor left at the pre-catch-up configuration
//! de-authorized every replica the catch-up admitted the moment an uncommitted
//! narrowing arrived over it.
//!
//! **The eleventh round found the other half of the same cell.** Separating the
//! floor from the record fixed what the driver publishes *after* the catch-up and
//! left what it publishes *before* one wrong in the permanent direction: with the
//! runtime's `{1,2,3}` as the whole peer set and the record's mark of 4 as the
//! floor, node 4 is beneath the floor and outside the set, which is the wire
//! definition of retired. The record's own later observation names it live. So
//! authorization is the union of the two runtime facts *and* the register, while
//! the raw floor stays the runtime's alone — the fields keep their separate
//! meanings, and only the derivation over them widened.

mod support;

use rafter::{
    AppendEntries, ConfigurationEntry, ConfigurationId, LogEntry, MembershipSet, NodeConfig,
    SharedEntries,
};
use rafter_app::group::RaftGroup;
use rafter_runtime::DurableRaftNode;
use rafter_service::{
    AuthenticatedPeerEnvelope, CurrentCommittedState, DriverServiceState,
    PeerControlPlaneCheckpoint, TransportDriverOptions, TransportRaftDriver,
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
/// **At the configuration entry rather than beneath it**, so recovery replays
/// nothing and this file measures only what the driver *publishes*. Replaying
/// the entry at index 2 would state a history it does not mean to: a committed
/// configuration is a transition from the membership before it, and for a
/// replica bootstrapped as node 4 that predecessor is the static membership its
/// own [`NodeConfig`] declares — which names node 4 and would make the entry a
/// committed removal of it. Whether a replayed transition retires anybody is
/// [`transport_committed_transition`]'s question; this one starts after it.
const APPLIED_FLOOR: LogIndex = LogIndex(2);

/// The configuration the recovered runtime opens under.
const RECOVERED: &[u64] = &[1, 2, 3];

/// The configuration the catch-up commits over it.
const CAUGHT_UP: &[u64] = &[1, 2, 3, 4];

/// Where the handed-over checkpoint's current state stands.
///
/// Far above every index in the fixture's log, which is the whole point: it
/// stands for an incarnation that observed the committed configuration well past
/// what this runtime rebuilt, so every committed fact the runtime produces is an
/// older observation.
const AHEAD_OF_RUNTIME: LogIndex = LogIndex(10);

/// One replica's durable state: a seeded application entry, then the one
/// committed configuration it recovered under.
///
/// The commit floor stops at the configuration, so the catch-up below is a real
/// commit this driver watches happen rather than more replay.
fn durable_state() -> (InMemoryRaftHardStateStore, InMemoryRaftLogSegment) {
    let mut hard_state_store = InMemoryRaftHardStateStore::new();
    hard_state_store
        .write_hard_state(RaftHardState {
            current_term: Term(1),
            voted_for: None,
            commit_index: LogIndex(2),
            committed_configuration: None,
        })
        .expect("durable commit floor writes");
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&[
            PersistedRaftLogEntry::application(LogIndex(1), Term(1), b"seed\nvalue".to_vec()),
            PersistedRaftLogEntry::configuration(LogIndex(2), Term(1), stable(1, RECOVERED)),
        ])
        .expect("committed entries persist");
    (hard_state_store, log_segment)
}

/// The checkpoint a supervisor hands over: a retirement record that has read
/// further than this runtime has.
///
/// Every clause of the record is honest. Node 4 is named live because the
/// incarnation that wrote this had watched the cluster admit it; the mark covers
/// it; nothing is owed. What it does *not* say is anything about the runtime it
/// is about to be joined into, and it is not supposed to.
fn handed_over_checkpoint() -> PeerControlPlaneCheckpoint<u64> {
    let mut checkpoint = PeerControlPlaneCheckpoint::empty(GROUP);
    checkpoint.committed_id_high_water = Some(NodeId(4));
    checkpoint.current_committed = Some(CurrentCommittedState::new(
        AHEAD_OF_RUNTIME,
        [NodeId(1), NodeId(2), NodeId(3), NodeId(4)]
            .into_iter()
            .collect(),
    ));
    checkpoint
}

/// Opens one replica over that durable state under the handed-over checkpoint.
fn recover_under_ahead_record(node_id: NodeId, peers: &[NodeId]) -> (Driver, QueueTransport) {
    let (hard_state_store, log_segment) = durable_state();
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
    let transport = QueueTransport::default();
    let validator = Validator {
        transport: transport.clone(),
        authorized: [NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)]
            .into_iter()
            .filter(|authorized| *authorized != node_id)
            .collect(),
        nameable: Nameable::all(),
    };
    let app = KvStateMachine {
        applied_index: APPLIED_FLOOR,
        ..KvStateMachine::default()
    };
    let group = RaftGroup::with_applied_index(GROUP, node_id, runtime, app, APPLIED_FLOOR);
    let driver = TransportRaftDriver::with_control_plane_checkpoint(
        group,
        recovery_outputs,
        transport.clone(),
        validator,
        TransportDriverOptions::default(),
        handed_over_checkpoint(),
    )
    .expect("a recovered replica opens under a checkpoint that has read further");
    (driver, transport)
}

/// One `AppendEntries` from the leader, carrying `entry` at index 3.
///
/// `leader_commit` decides which of the two shapes this is: at 3 the entry
/// commits in the same frame, and at 2 it appends and stays uncommitted.
fn append(to: NodeId, entry: LogEntry, leader_commit: LogIndex) -> Envelope {
    AuthenticatedPeerEnvelope {
        group_id: GROUP,
        authenticated_peer: Principal::for_node(NodeId(2)),
        raft_from: NodeId(2),
        raft_to: to,
        message: Message::AppendEntries(AppendEntries {
            term: Term(1),
            leader_id: NodeId(2),
            prev_log_index: LogIndex(2),
            prev_log_term: Term(1),
            sequence: 1,
            entries: SharedEntries::from(vec![entry]),
            leader_commit,
        }),
    }
}

/// The narrowing an in-flight change appends over the caught-up log.
fn append_narrowing(to: NodeId) -> Envelope {
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
            sequence: 2,
            entries: SharedEntries::from(vec![LogEntry::configuration(
                Term(1),
                stable(3, &[1, 2]),
            )]),
            leader_commit: LogIndex(3),
        }),
    }
}

/// A frame from `from`, used only to ask whether this driver admits it at all.
fn a_vote(from: NodeId, to: NodeId) -> Envelope {
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

type Envelope = AuthenticatedPeerEnvelope<u64, Principal>;

fn principals(node_ids: &[u64]) -> Vec<Principal> {
    node_ids
        .iter()
        .map(|node_id| Principal::for_node(NodeId(*node_id)))
        .collect()
}

/// Construction under an ahead record publishes the runtime's configuration and
/// the record's together.
///
/// The baseline the four cases below are read against, and **the tenth round's
/// answer here was the wrong half of the truth.** The raw floor does come from
/// the runtime — `{1,2,3}` is what this replica's own stream says the cluster has
/// committed — and the record's live set was withheld from the peer set on the
/// grounds that a retirement record says what has been *spent* rather than what
/// is committed now.
///
/// That reading published a policy retiring node 4: the floor is the mark, the
/// mark is 4, and an identity at or below the floor that the peer set does not
/// name is retired by definition. So the driver stated permanently that a replica
/// its own durable record calls live may never speak again — and if node 4 were
/// the leader, the frames that would have advanced this runtime past the record
/// were exactly the frames being refused.
///
/// The record's positioned observation is the later one, so it is the better
/// evidence about who is committed, and authorization takes all three sets in
/// union. What the record still does not do is become the raw floor: the two
/// questions stay separate, which the cases below depend on.
#[test]
fn construction_under_an_ahead_record_publishes_the_recovered_configuration() {
    let (driver, transport) = recover_under_ahead_record(NodeId(1), &[NodeId(2), NodeId(3)]);

    assert_eq!(
        transport.peer_sets().last().expect("a set was published"),
        &principals(&[2, 3, 4]),
        "the runtime recovered under {RECOVERED:?} and the record observed node 4 \
         committed later, so the link layer is owed both"
    );
    assert!(
        !transport.retires(NodeId(4)),
        "and above all node 4 is not retired, which is what publishing the \
         runtime's set alone beneath a mark of 4 amounts to: {:?}",
        transport.policies().last()
    );
    assert_eq!(driver.service_state(), DriverServiceState::Serving);
    assert_eq!(
        driver
            .control_plane_checkpoint()
            .current_committed
            .expect("the driver holds a current state")
            .membership,
        [NodeId(1), NodeId(2), NodeId(3), NodeId(4)]
            .into_iter()
            .collect(),
        "and the handed-over retirement record survives the construction intact"
    );
}

/// A catch-up that commits an addition beneath the record publishes it.
///
/// **The defect, at its first observable moment.** The crossing stands at index
/// 3 and the record at 10, so it is an older observation and does not become the
/// driver's current state — correctly, since the record looked later. The raw
/// floor was skipped with it, so the driver kept publishing `{1,2,3}` for a
/// cluster that had committed `{1,2,3,4}` while it watched.
///
/// The union hid it here and only here: the same step moves the *effective*
/// membership to `{1,2,3,4}`, and a union with one stale half is the other half.
/// So the peer set below is right for the wrong reason, and the case that
/// separates them is the next one.
#[test]
fn a_catch_up_that_commits_an_addition_beneath_the_record_publishes_it() {
    let (driver, transport) = recover_under_ahead_record(NodeId(1), &[NodeId(2), NodeId(3)]);

    driver
        .deliver(append(
            NodeId(1),
            LogEntry::configuration(Term(1), stable(2, CAUGHT_UP)),
            LogIndex(3),
        ))
        .expect("a leader's committing append is accepted");

    assert_eq!(
        transport.peer_sets().last().expect("a set was published"),
        &principals(&[2, 3, 4]),
        "the cluster committed node 4 while this driver watched"
    );
}

/// An uncommitted narrowing does not de-authorize a replica the catch-up
/// committed.
///
/// **The reviewer's case.** Everything above is still true — the crossing did
/// not move the register, the floor stayed at `{1,2,3}` — and now the effective
/// half moves out
/// from under it. A new leader appends a narrowing to `{1,2}` and does not
/// commit it, which is the ordinary shape of a membership change in flight and
/// the one shape that puts the effective configuration *below* the committed
/// one.
///
/// With a stale floor the union collapses to `{1,2,3}` and node 4 — committed,
/// unspent, and required by the change that is trying to commit — drops out of
/// the peer set and is refused inbound. The change that needs it can then never
/// commit, which is the deadlock the committed floor exists to prevent.
#[test]
fn an_uncommitted_narrowing_does_not_de_authorize_the_replica_the_catch_up_committed() {
    let (driver, transport) = recover_under_ahead_record(NodeId(1), &[NodeId(2), NodeId(3)]);
    driver
        .deliver(append(
            NodeId(1),
            LogEntry::configuration(Term(1), stable(2, CAUGHT_UP)),
            LogIndex(3),
        ))
        .expect("a leader's committing append is accepted");

    driver
        .deliver(append_narrowing(NodeId(1)))
        .expect("a leader's uncommitted append is accepted");

    assert_eq!(
        transport.peer_sets().last().expect("a set was published"),
        &principals(&[2, 3, 4]),
        "the narrowing has not committed, so the committed configuration is \
         still the floor and node 4 stays authorized"
    );
}

/// The newly committed replica stays authorized on the inbound path too.
///
/// The peer set and the inbound check are derived from the same union, so a lost
/// floor takes both. They are asserted apart because they fail apart in a
/// deployment: a stale peer set stops this replica *sending*, and a failed
/// inbound check drops what the peer sends *here*, which is what turns a
/// recoverable lag into a replica the cluster cannot reach.
#[test]
fn the_newly_committed_replica_is_still_admitted_inbound() {
    let (driver, _transport) = recover_under_ahead_record(NodeId(1), &[NodeId(2), NodeId(3)]);
    driver
        .deliver(append(
            NodeId(1),
            LogEntry::configuration(Term(1), stable(2, CAUGHT_UP)),
            LogIndex(3),
        ))
        .expect("a leader's committing append is accepted");
    driver
        .deliver(append_narrowing(NodeId(1)))
        .expect("a leader's uncommitted append is accepted");

    assert!(
        matches!(driver.deliver(a_vote(NodeId(4), NodeId(1))), Ok(())),
        "a frame from a replica the cluster has committed is admitted"
    );
    assert_eq!(
        driver.refused_non_member_frames(),
        0,
        "no committed member was turned away"
    );
}

/// The same scenario with the caught-up replica local: it keeps serving.
///
/// The local half, and the one a deployment feels first. Node 4 recovers under a
/// configuration that does not name it — correctly `NotMember`, since as far as
/// its own runtime knows it has not been admitted yet — then watches the cluster
/// commit its admission and begins serving. The uncommitted narrowing that
/// follows may still be truncated back off the log, so a replica that stops
/// serving for one abandons work the cluster still expects of it.
///
/// `NotMember` rather than `Decommissioned` is the tell: nothing was spent, no
/// fence is owed, and the mark and the current state are right. Only the floor
/// went missing.
#[test]
fn a_local_replica_the_catch_up_committed_keeps_serving_under_a_narrowing() {
    let (driver, _transport) = recover_under_ahead_record(NodeId(4), &[NodeId(1), NodeId(2)]);
    assert_eq!(
        driver.service_state(),
        DriverServiceState::NotMember { node_id: NodeId(4) },
        "the recovered configuration does not name node 4 yet"
    );

    driver
        .deliver(append(
            NodeId(4),
            LogEntry::configuration(Term(1), stable(2, CAUGHT_UP)),
            LogIndex(3),
        ))
        .expect("a leader's committing append is accepted");
    assert_eq!(
        driver.service_state(),
        DriverServiceState::Serving,
        "the cluster committed this replica's admission"
    );

    driver
        .deliver(append_narrowing(NodeId(4)))
        .expect("a leader's uncommitted append is accepted");

    assert_eq!(
        driver.service_state(),
        DriverServiceState::Serving,
        "an appended-and-uncommitted removal is not a removal"
    );
}
