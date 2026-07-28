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
    CurrentCommittedState, PeerControlPlaneCheckpoint, TransportDriverOptions, TransportRaftDriver,
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

/// Rebuilds the runtime, asserting the fixture really replays both crossings and
/// that each one carries the transition it should.
fn recovered_runtime(configurations: [&[u64]; 2]) -> (DurableRaftNode, Vec<RaftOutput>) {
    let (hard_state_store, log_segment) = durable_state(configurations);
    let (runtime, recovery_outputs) =
        DurableRaftNode::recover_with_storage_and_snapshot_store_applied_through(
            NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 5)
                .expect("test node config is valid"),
            hard_state_store,
            log_segment,
            InMemoryRaftSnapshotStore::new(),
            APPLIED_FLOOR,
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
    // The fixture only means anything if recovery really replays both moves, and
    // the transition only means anything if the kernel walked the chronology
    // rather than sampling the state the replay ends at. The first entry's
    // predecessor is the bootstrap membership — there is no configuration entry
    // beneath it — and the second's is the first.
    assert_eq!(
        crossings,
        vec![
            (FIRST_AT, ids(&BOOTSTRAP)),
            (SECOND_AT, ids(configurations[0])),
        ],
        "recovery must replay both crossings, each carrying the membership that \
         stood immediately before it: {recovery_outputs:?}"
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
    let (runtime, recovery_outputs) = recovered_runtime(configurations);
    let transport = QueueTransport::default();
    let validator = Validator {
        transport: transport.clone(),
        authorized: [NodeId(2), NodeId(3), NodeId(5)].into_iter().collect(),
        nameable: Nameable::all(),
    };
    let app = KvStateMachine {
        applied_index: APPLIED_FLOOR,
        ..KvStateMachine::default()
    };
    let group = RaftGroup::with_applied_index(GROUP, NodeId(1), runtime, app, APPLIED_FLOOR);
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
        driver.control_plane_checkpoint().pending_fences.is_empty(),
        "and no fence is owed for a replica no committed configuration removed"
    );
    assert!(
        transport.fence_attempts().is_empty(),
        "and the link layer was asked to fence nobody: {:?}",
        transport.fence_attempts()
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
fn a_removal_beneath_a_later_record_is_still_spent_and_fenced() {
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
        transport.is_fenced(NodeId(5)) || checkpoint.pending_fences.contains(&NodeId(5)),
        "and the fence the removal licensed is installed or still owed: fenced \
         {:?}, owed {:?}",
        transport.fence_attempts(),
        checkpoint.pending_fences
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
        transport.is_fenced(NodeId(5)) || checkpoint.pending_fences.contains(&NodeId(5)),
        "and its fence is installed or owed: fenced {:?}, owed {:?}",
        transport.fence_attempts(),
        checkpoint.pending_fences
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
        transport.fence_attempts().is_empty(),
        "a log that removed nobody owes no fence, however many times it is \
         replayed: {:?}",
        transport.fence_attempts()
    );
}

/// A replayed removal owes exactly one fence.
///
/// The removal half of idempotence, and the property a bare union of removal
/// sets would not give: the obligation is derived when the identity is *news*
/// and not again once it is spent, so a second recovery over a record that has
/// already absorbed the removal asks the link layer for nothing.
#[test]
fn a_replayed_removal_owes_exactly_one_fence() {
    let (first, first_transport) =
        adopt_under(PeerControlPlaneCheckpoint::empty(GROUP), ADMIT_THEN_REMOVE);
    let persisted = first.control_plane_checkpoint();
    assert_eq!(
        first_transport.fence_attempts().len(),
        1,
        "the first recovery watched the removal at index {} and fenced once: {:?}",
        SECOND_AT.0,
        first_transport.fence_attempts()
    );
    assert!(first_transport.is_fenced(NodeId(5)), "and it fenced node 5");
    assert!(
        persisted.pending_fences.is_empty(),
        "this link accepted the fence, so nothing is still owed: {persisted:?}"
    );

    let (second, second_transport) = adopt_under(persisted.clone(), ADMIT_THEN_REMOVE);

    assert_eq!(
        second.control_plane_checkpoint(),
        persisted,
        "the second recovery reached the same record"
    );
    assert!(
        second_transport.fence_attempts().is_empty(),
        "and asked the link layer for nothing, because node 5 was already spent \
         when the removal was replayed: {:?}",
        second_transport.fence_attempts()
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

    let checkpoint = driver.control_plane_checkpoint();
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
        checkpoint.pending_fences.is_empty(),
        "and nothing is owed: {:?}",
        checkpoint.pending_fences
    );
    assert!(
        transport.fence_attempts().is_empty(),
        "and the link layer was asked to fence nobody: {:?}",
        transport.fence_attempts()
    );
}
