#![allow(clippy::wildcard_imports)]

//! Folding the committed configuration stream from any state, in any order.
//!
//! A driver hears each committed configuration twice: once as the cluster
//! commits it, and again as a suffix the runtime replays from durable storage
//! after a restart. Deriving removals by subtracting a replayed membership from
//! the driver's own is right only when the driver's membership stands exactly
//! where the replayed fact does — so a process holding a *later* state read
//! every historical configuration as a removal of everything the later ones had
//! added, and an addition-only history permanently retired the replicas it
//! added.
//!
//! This suite used to be about the two consumer offsets that skipped such facts.
//! Both are gone. An offset claims a *prefix* has been consumed and nothing a
//! driver observes is a prefix — a snapshot-recovered process that folds a
//! crossing at index 8 has consumed neither 6 nor 7, and an offset reading 8
//! says it has. The repair is at the source: a crossing arrives as the
//! transition the kernel computed where the chronology is known, so its removal
//! set is exact wherever it is folded, and the current committed membership is a
//! versioned register that an older observation cannot pull backwards.
//!
//! What is pinned here is that both halves hold: a replayed history proves
//! exactly what it proved the first time, and no more.

mod support;

use std::collections::BTreeSet;

use rafter::{ConfigurationEntry, ConfigurationId, MembershipSet, NodeConfig};
use rafter_app::group::RaftGroup;
use rafter_runtime::DurableRaftNode;
use rafter_service::{
    ControlPlaneCheckpointError, CurrentCommittedState, DriverServiceState, ManagedDriverError,
    PeerControlPlaneCheckpoint, TransportDriverOptions, TransportRaftDriver,
};
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
    PersistedRaftLogEntry, RaftHardState, RaftHardStateStore, RaftLogSegment,
};
use support::scripted::*;
use support::transport::*;
use support::*;

fn stable(config_id: u64, node_ids: &[u64]) -> ConfigurationEntry {
    ConfigurationEntry::stable(
        ConfigurationId(config_id),
        MembershipSet::new(node_ids.iter().copied().map(NodeId).collect(), Vec::new())
            .expect("test membership is valid"),
    )
}

fn ids(node_ids: &[u64]) -> BTreeSet<NodeId> {
    node_ids.iter().copied().map(NodeId).collect()
}

/// The applied floor this replica's state machine opens at.
///
/// Directly beneath the two configuration entries, so the recovery replay is
/// exactly the two committed moves the fixtures name.
const APPLIED_FLOOR: LogIndex = LogIndex(5);

/// Where the first of the two configuration entries sits.
const FIRST_AT: LogIndex = LogIndex(6);

/// Where the second sits, and the runtime's commit index.
const SECOND_AT: LogIndex = LogIndex(7);

/// A position above both configuration entries.
///
/// The whole scenario for the records below: a process that recovered from a
/// snapshot honestly reports having observed the committed membership *there*,
/// and knows nothing about what committed and was superseded beneath it.
const ABOVE_HISTORY: LogIndex = LogIndex(10);

/// The membership this replica bootstraps with, which is what stands before the
/// first configuration entry and therefore the `previous` half of the first
/// transition.
const BOOTSTRAP: [u64; 3] = [1, 2, 3];

/// A history that admits node 5 and then removes it, ending where it began.
///
/// The endpoint carries no trace of node 5 at all, which is what makes it the
/// history no endpoint observation can stand in for.
const ADMIT_THEN_REMOVE: [&[u64]; 2] = [&[1, 2, 3, 5], &[1, 2, 3]];

/// A history that only ever adds.
///
/// Every retirement derivable from it is manufactured, which is what makes it
/// the fixture that catches a fold reading a state difference where the fact
/// carries a transition.
const ONLY_ADDITIONS: [&[u64]; 2] = [&[1, 2, 3, 4], &[1, 2, 3, 4, 5]];

/// A history whose second entry removes node 4 by an exact committed transition.
///
/// The transition is the strongest evidence this driver can hold — the kernel
/// computed `{1,4} − {1}` where the chronology was known — and it is replayed
/// beneath a record that has already moved past it. The record's own mark
/// already covers node 4, which is the condition under which the old derivation
/// discarded the fact.
const REMOVE_FOUR: [&[u64]; 2] = [&[1, 4], &[1]];

/// One replica's durable state: filler beneath the applied floor, then the two
/// configurations `configurations` names.
fn durable_state(
    configurations: [&[u64]; 2],
) -> (InMemoryRaftHardStateStore, InMemoryRaftLogSegment) {
    let mut hard_state_store = InMemoryRaftHardStateStore::new();
    hard_state_store
        .write_hard_state(RaftHardState {
            current_term: Term(1),
            voted_for: None,
            commit_index: SECOND_AT,
            committed_configuration: None,
        })
        .expect("durable commit floor writes");
    let mut entries = (1..=APPLIED_FLOOR.0)
        .map(|index| {
            PersistedRaftLogEntry::application(LogIndex(index), Term(1), b"seed\nvalue".to_vec())
        })
        .collect::<Vec<_>>();
    entries.push(PersistedRaftLogEntry::configuration(
        FIRST_AT,
        Term(1),
        stable(1, configurations[0]),
    ));
    entries.push(PersistedRaftLogEntry::configuration(
        SECOND_AT,
        Term(1),
        stable(2, configurations[1]),
    ));
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&entries)
        .expect("committed entries persist");
    (hard_state_store, log_segment)
}

/// Rebuilds the runtime above `applied_floor`, asserting the fixture replays the
/// crossings it means to and that each one carries the transition it should.
fn recovered_runtime(
    configurations: [&[u64]; 2],
    applied_floor: LogIndex,
) -> (DurableRaftNode, Vec<RaftOutput>) {
    let (hard_state_store, log_segment) = durable_state(configurations);
    let (runtime, recovery_outputs) =
        DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
            NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 5)
                .expect("test node config is valid"),
            hard_state_store,
            log_segment,
            InMemoryRaftSnapshotStore::new(),
            applied_floor,
        )
        .expect("the runtime recovers above the applied floor")
        .into_parts();
    let crossings = recovery_outputs
        .iter()
        .filter_map(|output| match output {
            RaftOutput::ConfigurationCommitted {
                index, previous, ..
            } => Some((*index, previous.replica_ids().into_iter().collect())),
            _ => None,
        })
        .collect::<Vec<(LogIndex, BTreeSet<NodeId>)>>();
    // The fixture only means anything if recovery really replays the moves it
    // names, and the transition only means anything if the kernel walked the
    // chronology rather than sampling the state the replay ends at. The first
    // entry's predecessor is the bootstrap membership — there is no
    // configuration entry beneath it — and the second's is the first.
    let expected = if applied_floor < FIRST_AT {
        vec![
            (FIRST_AT, ids(&BOOTSTRAP)),
            (SECOND_AT, ids(configurations[0])),
        ]
    } else {
        vec![(SECOND_AT, ids(configurations[0]))]
    };
    assert_eq!(
        crossings, expected,
        "recovery must replay the crossings above the applied floor, each \
         carrying the membership that stood immediately before it: \
         {recovery_outputs:?}"
    );
    (runtime, recovery_outputs)
}

/// A record standing at `through` with `live` as its committed membership.
fn record_at(mark: u64, live: &[u64], through: LogIndex) -> PeerControlPlaneCheckpoint<u64> {
    let mut checkpoint = PeerControlPlaneCheckpoint::empty(GROUP);
    checkpoint.committed_id_high_water = Some(NodeId(mark));
    checkpoint.current_committed = Some(CurrentCommittedState::new(through, ids(live)));
    checkpoint
}

/// Opens a driver over the replayable history under that record.
fn adopt_under(
    checkpoint: PeerControlPlaneCheckpoint<u64>,
    configurations: [&[u64]; 2],
) -> (Driver, QueueTransport) {
    adopt_above(checkpoint, configurations, APPLIED_FLOOR)
}

/// The same, with the applied floor chosen by the caller.
fn adopt_above(
    checkpoint: PeerControlPlaneCheckpoint<u64>,
    configurations: [&[u64]; 2],
    applied_floor: LogIndex,
) -> (Driver, QueueTransport) {
    let (opened, transport) = try_adopt_above(checkpoint, configurations, applied_floor);
    (opened.expect("a recovered replica opens"), transport)
}

/// The same, returning the refusal rather than panicking on it.
fn try_adopt_under(
    checkpoint: PeerControlPlaneCheckpoint<u64>,
    configurations: [&[u64]; 2],
) -> Result<Driver, ManagedDriverError> {
    try_adopt_above(checkpoint, configurations, APPLIED_FLOOR).0
}

/// The one opener every fixture above is a shape of.
///
/// The transport comes back beside the refusal, which is what a "published
/// nothing" assertion needs: a constructor that returns no driver still had a
/// link layer, and the only way to ask what it was told is to keep a handle on
/// it.
fn try_adopt_above(
    checkpoint: PeerControlPlaneCheckpoint<u64>,
    configurations: [&[u64]; 2],
    applied_floor: LogIndex,
) -> (Result<Driver, ManagedDriverError>, QueueTransport) {
    let (runtime, recovery_outputs) = recovered_runtime(configurations, applied_floor);
    let transport = QueueTransport::default();
    let validator = Validator {
        transport: transport.clone(),
        authorized: [NodeId(2), NodeId(3), NodeId(5)].into_iter().collect(),
        nameable: Nameable::all(),
    };
    let app = KvStateMachine {
        applied_index: applied_floor,
        ..KvStateMachine::default()
    };
    let group = RaftGroup::with_applied_index(GROUP, NodeId(1), runtime, app, applied_floor);
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

/// A record that disagrees with the kernel's own transition at that transition's
/// index refuses to open.
///
/// **The tie arm, reached by a crossing rather than by a record pair, and the one
/// place the routing path has to hand a refusal back.** The record says the
/// committed membership at index 7 is `{1,2,3,5}`; the configuration entry *at*
/// index 7 says it is `{1,2,3}`, and the crossing that replays it carries the
/// kernel's own `previous` to prove the transition. Two claims about one
/// committed configuration, and neither side's spent-ness explains the gap —
/// node 5 is live in the record, so the record does not spend it.
///
/// `route_membership_event` has nowhere to return that, because it runs from
/// every step outcome including a failing one. It records instead, and the
/// constructor asks once the replay is done: a driver that opened here would be
/// serving from inputs it has already declared untrustworthy.
///
/// A merge that normalized the comparison by the crossing's own removal set
/// would let the transition quietly overrule the record, which is the one pair
/// such a normalization can change — a crossing's removals are disjoint from the
/// configuration it carries, so on every honest input the term is a no-op and on
/// this one it is a hole.
#[test]
fn a_record_disagreeing_with_a_transition_at_its_own_index_refuses_to_open() {
    let refused = try_adopt_under(record_at(5, &[1, 2, 3, 5], SECOND_AT), ADMIT_THEN_REMOVE);

    assert!(
        matches!(
            refused.as_ref().map(|_| ()),
            Err(ManagedDriverError::InvalidControlPlaneCheckpoint {
                reason: ControlPlaneCheckpointError::ContradictoryCurrentState { through }
            }) if *through == SECOND_AT
        ),
        "got {:?}",
        refused.map(|_| "a driver")
    );
}

/// The membership a driver's record calls live.
fn live_of(driver: &Driver) -> BTreeSet<NodeId> {
    driver
        .control_plane_checkpoint()
        .current_committed
        .map(|current| current.membership)
        .unwrap_or_default()
}

/// Where a driver's record stands.
fn standing_at(driver: &Driver) -> LogIndex {
    driver
        .control_plane_checkpoint()
        .current_committed
        .expect("the driver holds a current state")
        .through
}

/// An addition-only history retires nobody, whatever record it is replayed
/// against.
///
/// **The reviewer's counterexample, and it is not about positions at all.** The
/// record is the honest shape a snapshot-recovered process produces: it observed
/// `{1,2,3,4,5}` at commit 10 and folded no configuration entry. The history it
/// is joined into only ever *adds* — `+{1,2,3,4}` at index 6, `+{1,2,3,4,5}` at
/// index 7 — so no committed removal exists anywhere in this scenario.
///
/// Folded chronologically the two crossings retire nobody. Folded as membership
/// *states* against the later record, the first one reads
/// `previous_live − crossing` = `{1,2,3,4,5} − {1,2,3,4}` = `{5}`, spends node 5
/// and owes a permanent fence for it. The crossing at index 7 cannot give it
/// back: node 5 is spent by then, so the spent filter drops it out of the live
/// set it would have restored.
///
/// Carrying the transition is what makes the difference. `previous − new` is
/// empty for both entries whatever state they are folded into.
#[test]
fn an_addition_only_history_retires_nobody_against_a_later_record() {
    let (driver, transport) = adopt_under(
        record_at(5, &[1, 2, 3, 4, 5], ABOVE_HISTORY),
        ONLY_ADDITIONS,
    );

    assert!(
        live_of(&driver).contains(&NodeId(5)),
        "the history only ever added node 5, so nothing here spends it: {:?}",
        live_of(&driver)
    );
    assert!(
        !transport.retires(NodeId(5)),
        "and the policy this driver published does not retire a replica no \
         committed configuration removed: {:?}",
        transport.policies().last()
    );
}

/// A removal beneath a later record is still spent and still owes its fence.
///
/// The other direction of the same fixture, and the one that shows the fold has
/// not simply been switched off. The record recovered from a snapshot: it never
/// saw `+5` at index 6 or `−5` at index 7, and honestly reports observing
/// `{1,2,3}` at commit 10. The history beneath it really does spend node 5, and
/// nothing about the record standing later may hide that.
///
/// Under the deleted cursors this was the case a single shared position broke:
/// `max` carried the endpoint's 10 into the crossing gate and skipped both
/// entries as already-consumed.
#[test]
fn a_removal_beneath_a_later_record_is_still_spent_and_retired() {
    let (driver, transport) =
        adopt_under(record_at(3, &[1, 2, 3], ABOVE_HISTORY), ADMIT_THEN_REMOVE);

    let checkpoint = driver.control_plane_checkpoint();
    assert_eq!(
        checkpoint.committed_id_high_water,
        Some(NodeId(5)),
        "the admission at index {} raised the mark, and the record the snapshot \
         process carried never named node 5 at all",
        FIRST_AT.0
    );
    assert!(
        !live_of(&driver).contains(&NodeId(5)),
        "and the removal at index {} spent the identity: {:?}",
        SECOND_AT.0,
        live_of(&driver)
    );
    assert!(
        transport.retires(NodeId(5)),
        "and the policy this driver published retires it: {:?}",
        transport.policies().last()
    );
}

/// An exact removal transition beneath a later record still denies the identity
/// at the link layer.
///
/// **The tenth reviewer's first counterexample, and it is about what `is_spent`
/// cannot answer.** The record is honest and snapshot-derived: `{1,6}` observed
/// at commit 10, with a mark of 6. Beneath it the history really does remove
/// node 4, and it says so with the strongest evidence there is — the kernel's
/// own transition `{1,4} → {1}` at index 7.
///
/// The record's mark already covers node 4 and its live set does not name it, so
/// `is_spent(4)` is true before the crossing is folded. Read as "has this
/// identity been retired", that is correct. Read as "has this identity's link
/// layer been told", it is a guess — and it was the guess that suppressed the
/// obligation. **A fence is a statement to a link layer, and this process's link
/// layer is new**: `published_peers` deliberately does not survive a restart
/// precisely because a new process has a link layer that has accepted nothing.
///
/// So the driver watched a committed removal it could prove exactly, and its
/// link layer was never told anything about node 4.
#[test]
fn an_exact_removal_beneath_a_later_record_still_denies_the_identity() {
    let (driver, transport) = adopt_under(record_at(6, &[1, 6], ABOVE_HISTORY), REMOVE_FOUR);

    assert!(
        !live_of(&driver).contains(&NodeId(4)),
        "the transition at index {} proves node 4 removed: {:?}",
        SECOND_AT.0,
        live_of(&driver)
    );
    assert!(
        transport.retires(NodeId(4)),
        "and this process's link layer must be told so: {:?}",
        transport.policies().last()
    );
}

/// A record whose position sits above history it never saw still folds that
/// history.
///
/// **The second expression of the same defect, and the one deletion fixes by
/// construction.** A process that recovered from a snapshot and then observed a
/// crossing at index 8 held a crossing offset of 8 without ever having seen 6 or
/// 7 — a maximum over observed positions is not a contiguous prefix. Any later
/// merge then skipped the retained crossings at 6 and 7 as history, so the
/// identity the removal at 7 spent was never spent and its fence was never owed.
///
/// The record here is exactly that shape: standing at index 8, naming a
/// membership node 5 has already left, with no knowledge of how it left. With no
/// cursor there is nothing to skip against, and the crossings beneath prove what
/// they prove.
#[test]
fn a_record_above_unseen_history_still_folds_it() {
    let (driver, transport) =
        adopt_under(record_at(4, &[1, 2, 3, 4], LogIndex(8)), ADMIT_THEN_REMOVE);

    let checkpoint = driver.control_plane_checkpoint();
    assert_eq!(
        checkpoint.committed_id_high_water,
        Some(NodeId(5)),
        "the crossing at index {} named node 5 and raised the mark past it",
        FIRST_AT.0
    );
    assert!(
        !live_of(&driver).contains(&NodeId(5)),
        "and node 5 is spent: {:?}",
        live_of(&driver)
    );
    assert!(
        transport.retires(NodeId(5)),
        "and the published policy retires it: {:?}",
        transport.policies().last()
    );
    assert_eq!(
        standing_at(&driver),
        LogIndex(8),
        "and the record's own position still stands, because a crossing beneath \
         it is evidence about identities rather than about the present"
    );
}

/// Replaying the same history over its own record reaches the same record.
///
/// **Idempotence, which the cursors used to buy and the transition now gives for
/// free.** Re-running the same recovery over the same durable state must not
/// manufacture on the second pass what it did not on the first — a crash during
/// recovery is an ordinary crash, so the second pass is a state a correct
/// embedder reaches.
///
/// The history is addition-only, so every retirement derivable from it is
/// manufactured and shows up immediately.
#[test]
fn replaying_a_history_over_its_own_record_changes_nothing() {
    let (first, _) = adopt_under(PeerControlPlaneCheckpoint::empty(GROUP), ONLY_ADDITIONS);
    let persisted = first.control_plane_checkpoint();
    assert_eq!(
        standing_at(&first),
        SECOND_AT,
        "the first recovery ends standing at the last fact it folded: {persisted:?}"
    );

    let (second, transport) = adopt_under(persisted.clone(), ONLY_ADDITIONS);

    assert_eq!(
        second.control_plane_checkpoint(),
        persisted,
        "the second recovery re-derived a different record from the same durable \
         state"
    );
    assert!(
        !transport.retires(NodeId(5)),
        "a log that removed nobody retires nobody, however many times it is \
         replayed: {:?}",
        transport.policies().last()
    );
}

/// A replayed removal reaches the same published policy, not a second statement.
///
/// **Idempotence at the link layer, which is what the obligation ledger used to
/// be responsible for.** A driver that derived a fresh fence on every replay
/// would ask its link layer to fence the same replica once per recovery; a
/// driver that suppressed the derivation once the identity tested as spent lost
/// the statement entirely on a fresh link layer, which is the tenth reviewer's
/// first counterexample.
///
/// A floor is neither. The second recovery derives exactly the policy the first
/// one did, publishes it once because the transport already holds it, and the
/// replica stays retired because the floor covers it and the peer set does not
/// name it.
#[test]
fn a_replayed_removal_reaches_the_same_published_policy() {
    let (first, first_transport) =
        adopt_under(PeerControlPlaneCheckpoint::empty(GROUP), ADMIT_THEN_REMOVE);
    let persisted = first.control_plane_checkpoint();
    assert!(
        first_transport.retires(NodeId(5)),
        "the first recovery watched the removal at index {} and retired node 5: \
         {:?}",
        SECOND_AT.0,
        first_transport.policies().last()
    );
    let settled = first_transport
        .policies()
        .last()
        .cloned()
        .expect("a policy was published");

    let (second, second_transport) = adopt_under(persisted.clone(), ADMIT_THEN_REMOVE);

    assert_eq!(
        second.control_plane_checkpoint(),
        persisted,
        "the second recovery reached the same record"
    );
    assert_eq!(
        second_transport.policies().last(),
        Some(&settled),
        "and told its own link layer the same thing, rather than a statement \
         that depends on how many times the history has been replayed"
    );
}

/// An observation behind the register manufactures no removal.
///
/// A commit index is volatile: a rebuilt runtime can legitimately report a lower
/// one than the incarnation that wrote the record reached. An older observation
/// may only contribute what it names and the later one does not — it may never
/// be subtracted from in the other direction, which is what would retire
/// everything the newer configurations added.
///
/// The record here says so directly: `{1,2,3,4,5}` observed at commit 10, over a
/// runtime standing at commit 7, with both crossings replayed beneath it. That
/// is where a state-difference fold finds its manufactured removals, and where
/// an ungated endpoint fold finds its own.
#[test]
fn an_observation_behind_the_register_manufactures_no_removal() {
    let (driver, transport) = adopt_under(
        record_at(5, &[1, 2, 3, 4, 5], ABOVE_HISTORY),
        ONLY_ADDITIONS,
    );

    assert_eq!(
        live_of(&driver),
        ids(&[1, 2, 3, 4, 5]),
        "nothing beneath the record's position removed anybody, so the membership \
         it holds is untouched"
    );
    assert_eq!(
        standing_at(&driver),
        ABOVE_HISTORY,
        "and the older observations did not pull the register backwards"
    );
    assert!(
        !transport.retires(NodeId(5)),
        "and the link layer was told to retire nobody the history kept: {:?}",
        transport.policies().last()
    );
}

/// A removal replayed *alone* is spent and fenced on its own evidence.
///
/// **The case the transition is load-bearing for, and the one a mutation check
/// found missing.** Everywhere the admission crossing is replayed too, the
/// removal is already derivable by comparing that crossing's membership against
/// a record that no longer names the identity — so neutralizing the transition
/// and leaning entirely on that inference passes every other case here.
///
/// It does not pass this one. The applied floor sits *between* the admission at
/// index 6 and the removal at index 7, which is ordinary rather than contrived:
/// a state machine's floor lands wherever it landed. Only the removal is
/// replayed, its membership `{1,2,3}` is exactly what the record already names,
/// and the inference has nothing to compare. Without `previous` the identity
/// still becomes spent — the crossing names node 5, so the mark rises past it and
/// the record does not name it — but **no fence is ever owed**, which is the
/// quietest way this whole mechanism can fail.
#[test]
fn a_removal_replayed_without_its_admission_is_still_retired() {
    let (driver, transport) = adopt_above(
        record_at(3, &[1, 2, 3], ABOVE_HISTORY),
        ADMIT_THEN_REMOVE,
        FIRST_AT,
    );

    let checkpoint = driver.control_plane_checkpoint();
    assert_eq!(
        checkpoint.committed_id_high_water,
        Some(NodeId(5)),
        "the crossing named node 5 in the membership it replaced, so the mark \
         covers it"
    );
    assert!(
        !live_of(&driver).contains(&NodeId(5)),
        "and it is spent: {:?}",
        live_of(&driver)
    );
    assert!(
        transport.retires(NodeId(5)),
        "and the published policy retires it, which only the transition \
         proves here: {:?}",
        transport.policies().last()
    );
}

/// A removal is absorbed even when a later record still names the identity.
///
/// **The trap this design had to answer, and the case that proves the answer is
/// load-bearing.** If a crossing only ever narrowed the register when it was the
/// later observation, a removal proved beneath a later record would leave the
/// identity in `membership` — and `spent(id) = id ≤ mark ∧ id ∉ membership`
/// cannot see it there. The fence would be owed for a replica the driver still
/// called live: the peer set would publish it and the flush would permanently
/// fence it, which is two contradictory statements about one replica and a
/// record its own validator refuses.
///
/// So the removal is subtracted from the register whatever the fact's position,
/// because a removal is not an observation of the present — it is a permanent
/// fact about an identity. No holding set is needed: the crossing that proves
/// the removal also *named* the identity, so the mark already covers it, and
/// every later assignment filters its incoming membership through the spent test
/// so it can never re-enter.
///
/// The record here names node 5 at position 10 while the history removed it at
/// index 7, which is the single-use contract already broken — the one condition
/// under which the gap is reachable at all. The driver's answer is to keep the
/// identity spent and refuse it.
#[test]
fn a_removal_is_absorbed_even_when_a_later_record_still_names_it() {
    let (driver, transport) = adopt_under(
        record_at(5, &[1, 2, 3, 4, 5], ABOVE_HISTORY),
        ADMIT_THEN_REMOVE,
    );

    let checkpoint = driver.control_plane_checkpoint();
    assert!(
        !live_of(&driver).contains(&NodeId(5)),
        "the crossing at index {} proved node 5 removed, and a record standing \
         later does not make that provisional: {:?}",
        SECOND_AT.0,
        live_of(&driver)
    );
    assert!(
        transport.retires(NodeId(5)),
        "the published policy retires it: {:?}",
        transport.policies().last()
    );
    assert!(
        !transport
            .peer_sets()
            .last()
            .expect("a set was published")
            .iter()
            .any(|principal| principal_node(principal) == Some(NodeId(5))),
        "and it is not also published to the link layer, which is the \
         contradiction a record that kept it live would produce: {:?}",
        transport.peer_sets().last()
    );
    // And the record the driver would persist is one a driver accepts back. A
    // fence naming an identity the same record calls live is refused at restore
    // — which is what a removal left unabsorbed would produce, and the reason
    // the gap is a correctness bug rather than an untidiness.
    let group = driver.release_group().expect("the driver holds a group");
    driver
        .adopt_group_with_checkpoint(group, Vec::new(), checkpoint)
        .expect("a driver's own record is a record it can be handed back");
}

/// A history whose second entry declares a predecessor the record cannot be.
///
/// `{1,2,4}` at index 6 and `{1,2}` at index 7, joined into a record standing at
/// index 6 and calling the committed membership `{1,2,3}` there. No index exists
/// between 6 and 7, so the transition's own `previous` and the record's
/// membership are two claims about the *same* committed configuration.
const PREDECESSOR_DISAGREES: [&[u64]; 2] = [&[1, 2, 4], &[1, 2]];

/// A crossing whose declared predecessor contradicts the record it lands on
/// refuses to open.
///
/// **The transition's most useful executable evidence, which used to be thrown
/// away.** `MembershipEvent::Applied` carries the membership the kernel computed
/// as standing immediately before the entry, and the driver reduced it to a
/// removed set and a named set — so the predecessor itself never met the state it
/// is a claim about.
///
/// Here it disagrees, and the disagreement is not reconcilable by position: the
/// record stands at index 6, the crossing at index 7, and there is no entry
/// between them for the difference to have happened at. Folded anyway, the merge
/// retires node 3 by the record-minus-transition inference *and* node 4 by the
/// exact removal — two identities consumed on the strength of a pair that proves
/// the record and the log are not one chain.
///
/// The refusal lands before any mark, register, or policy moves, which is what
/// separates it from a driver that opens and then reports itself sick.
#[test]
fn a_crossing_whose_predecessor_contradicts_the_record_refuses_to_open() {
    let (opened, transport) = try_adopt_above(
        record_at(3, &[1, 2, 3], FIRST_AT),
        PREDECESSOR_DISAGREES,
        FIRST_AT,
    );

    assert!(
        matches!(
            opened.as_ref().map(|_| ()),
            Err(ManagedDriverError::InvalidControlPlaneCheckpoint {
                reason: ControlPlaneCheckpointError::ContradictoryTransitionPredecessor { through }
            }) if *through == FIRST_AT
        ),
        "the transition at index {} declares `{{1,2,4}}` stood at index {}, and \
         the record calls it `{{1,2,3}}`: got {:?}",
        SECOND_AT.0,
        FIRST_AT.0,
        opened.map(|_| "a driver")
    );
    assert!(
        transport.policies().is_empty(),
        "and nothing was published on the way to the refusal: {:?}",
        transport.policies()
    );
}

/// The same refusal on the live path, where the crossing arrives as an event.
///
/// A second call site rather than a second path: `route_membership_events` runs
/// from every step outcome and has nowhere to return a refusal, so it records one
/// and the driver stops serving. What must hold either way is that the transition
/// is checked against the state it is a claim about *before* anything moves — the
/// mark, the register, and the policy are all downstream of the same fold.
///
/// The register stands at index 5 after construction, so the crossing at index 6
/// is adjacent to it and its `previous` is a claim about index 5. The driver holds
/// `{1,2,3}` there and the crossing says `{1,2,4}`.
#[test]
fn a_live_crossing_whose_predecessor_contradicts_the_register_stops_the_driver() {
    let runtime = ScriptedMembershipRuntime::new(&[1, 2, 3], &[1, 2, 3]);
    let handle = runtime.handle();
    let (driver, transport) = scripted_driver(runtime, Nameable::all());
    let policies_before = transport.policies();
    let checkpoint_before = driver.control_plane_checkpoint();

    outputs_on_step(
        &handle,
        vec![RaftOutput::ConfigurationCommitted {
            index: LogIndex(6),
            term: Term(1),
            previous: config_of(&[1, 2, 4]),
            configuration: stable(3, &[1, 2]),
        }],
    );
    driver.tick().expect("the protocol still advances");

    assert_eq!(
        driver.service_state(),
        DriverServiceState::ContradictoryTransitionPredecessor {
            through: LogIndex(5)
        },
        "the crossing at index 6 declares `{{1,2,4}}` stood at index 5, and this \
         driver's register calls it `{{1,2,3}}`"
    );
    assert_eq!(
        transport.policies(),
        policies_before,
        "and the link layer was told nothing: {:?}",
        transport.policies()
    );
    let after = driver.control_plane_checkpoint();
    assert_eq!(
        (after.committed_id_high_water, after.current_committed),
        (
            checkpoint_before.committed_id_high_water,
            checkpoint_before.current_committed
        ),
        "nor did the retirement record move — neither node 3 by inference nor \
         node 4 by the transition's own removal was retired"
    );
    assert_eq!(
        after.contradicted_at,
        Some(LogIndex(5)),
        "the one thing that did move is the marker, which is what makes the \
         refusal survive a restart"
    );
}
