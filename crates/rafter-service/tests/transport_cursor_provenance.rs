#![allow(clippy::wildcard_imports)]

//! What a consumer offset is evidence *of*, and why one number could not say.
//!
//! A driver's position in the committed configuration stream is read as a
//! licence to skip: a fact at or below it has been folded in, so folding it
//! again would compute a difference between a historical membership and a
//! present one and call everything the present added a removal. That licence is
//! sound only when the position was earned by actually consuming the history it
//! covers.
//!
//! Two different facts were advancing one position. An **exact crossing** is a
//! configuration entry the commit index crossed, and it carries that entry's own
//! index — consuming it really does cover that point in the stream. An
//! **endpoint observation** is a comparison against the committed membership the
//! runtime holds, stamped with the commit index, and it covers nothing beneath
//! itself: a replica that installed a snapshot at commit 10 observes the
//! boundary configuration and learns nothing about the configurations that
//! committed and were superseded below it. The checkpoint type says so in its
//! own words, under "What a snapshot cannot give back".
//!
//! So an endpoint-derived position claimed coverage it never had, and the join's
//! premise — a greater cursor means everything beneath it is already reduced —
//! was false for exactly those positions. The two are separate fields now, each
//! advanced and gated only by its own kind of fact, and an endpoint position
//! never suppresses an exact crossing.

mod support;

use rafter::{ConfigurationEntry, ConfigurationId, MembershipSet, NodeConfig};
use rafter_app::group::RaftGroup;
use rafter_runtime::DurableRaftNode;
use rafter_service::{PeerControlPlaneCheckpoint, TransportDriverOptions, TransportRaftDriver};
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
/// Directly beneath the two configuration entries, so the recovery replay is
/// exactly the admission of node 5 and the removal that spends it.
const APPLIED_FLOOR: LogIndex = LogIndex(5);

/// Where the admission of node 5 sits in the log.
const ADMITTED_AT: LogIndex = LogIndex(6);

/// Where the removal that spends it sits.
const REMOVED_AT: LogIndex = LogIndex(7);

/// The position an endpoint-derived record carries.
///
/// Above both configuration entries, which is the whole scenario: a process that
/// recovered from a snapshot at commit 10 honestly reports having observed the
/// committed membership *there*, and dishonestly implies it consumed indices 6
/// and 7 on the way.
const SNAPSHOT_COMMIT: LogIndex = LogIndex(10);

/// A history that admits node 5 and then removes it, ending where it began.
///
/// The endpoint carries no trace of node 5 at all, which is what makes it the
/// history an endpoint-derived position cannot stand in for.
const ADMIT_THEN_REMOVE: [&[u64]; 2] = [&[1, 2, 3, 5], &[1, 2, 3]];

/// A history that only ever adds.
///
/// Every retirement derivable from it is manufactured, which is what makes it
/// the history that shows a *crossing* position still doing its job: re-folding
/// the entry at index 6 against a live set that already reflects index 7 reads
/// as a removal of node 5.
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
            commit_index: REMOVED_AT,
            committed_configuration: None,
        })
        .expect("durable commit floor writes");
    let mut entries = (1..=APPLIED_FLOOR.0)
        .map(|index| {
            PersistedRaftLogEntry::application(LogIndex(index), Term(1), b"seed\nvalue".to_vec())
        })
        .collect::<Vec<_>>();
    entries.push(PersistedRaftLogEntry::configuration(
        ADMITTED_AT,
        Term(1),
        stable(1, configurations[0]),
    ));
    entries.push(PersistedRaftLogEntry::configuration(
        REMOVED_AT,
        Term(1),
        stable(2, configurations[1]),
    ));
    let mut log_segment = InMemoryRaftLogSegment::new();
    log_segment
        .append_entries(&entries)
        .expect("committed entries persist");
    (hard_state_store, log_segment)
}

/// Rebuilds the runtime, asserting the fixture really replays both crossings.
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
    assert_eq!(
        recovery_outputs
            .iter()
            .filter(|output| matches!(output, RaftOutput::ConfigurationCommitted { .. }))
            .count(),
        2,
        "the fixture only means anything if recovery really replays the \
         admission and the removal: {recovery_outputs:?}"
    );
    (runtime, recovery_outputs)
}

/// The record a snapshot-recovered process honestly produces.
///
/// It observed the committed membership `{1,2,3}` at commit 10 and folded that
/// one endpoint in. Its mark covers what the boundary configuration named and
/// nothing more; node 5 committed and was superseded below the boundary, so this
/// process has no way to know the identity was ever spent — the checkpoint type
/// says exactly this under "What a snapshot cannot give back".
///
/// Every field is true. The lie is only in what the *position* implies.
fn endpoint_only_checkpoint() -> PeerControlPlaneCheckpoint<u64> {
    let mut checkpoint = PeerControlPlaneCheckpoint::empty(GROUP);
    checkpoint.committed_id_high_water = Some(NodeId(3));
    checkpoint.live_committed_members = [NodeId(1), NodeId(2), NodeId(3)].into_iter().collect();
    checkpoint.committed_endpoint_through = Some(SNAPSHOT_COMMIT);
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

/// An endpoint-derived position does not suppress the crossings beneath it.
///
/// **The reviewer's case, and the one that refutes the join's premise.** Process
/// A recovers from a snapshot: it never saw `+5` at index 6 or `−5` at index 7,
/// and it honestly reports observing the committed membership at commit 10. The
/// join takes the greater position. Process B's recovery outputs for 6 and 7 are
/// then skipped as already-consumed — so node 5 is never spent, its fence is
/// never owed, and the identity a committed removal consumed is allocatable
/// again by any replica that asks.
///
/// The premise the join was proved under is "whichever side held the higher
/// position had consumed that configuration, so its spent-ness is already in the
/// union". That is true of a crossing and false of an endpoint, which covers
/// nothing beneath itself. Splitting the position is what makes the premise true
/// again for the field the proof actually reads.
#[test]
fn an_endpoint_position_does_not_suppress_the_crossings_beneath_it() {
    let (driver, transport) = adopt_under(endpoint_only_checkpoint(), ADMIT_THEN_REMOVE);

    let checkpoint = driver.control_plane_checkpoint();
    assert_eq!(
        checkpoint.committed_id_high_water,
        Some(NodeId(5)),
        "the admission at index {} raised the mark, and the endpoint the \
         snapshot record carried never named node 5 at all",
        ADMITTED_AT.0
    );
    assert!(
        !checkpoint.live_committed_members.contains(&NodeId(5)),
        "and the removal at index {} spent the identity: {:?}",
        REMOVED_AT.0,
        checkpoint.live_committed_members
    );
    assert!(
        transport.is_fenced(NodeId(5)) || checkpoint.pending_fences.contains(&NodeId(5)),
        "and the fence the removal licensed is installed or still owed: fenced \
         {:?}, owed {:?}",
        transport.fence_attempts(),
        checkpoint.pending_fences
    );
}

/// A crossing position still suppresses the crossings beneath it.
///
/// **The other direction of the same gate, and the property the split must not
/// have cost.** Splitting the position would be a poor trade if it re-opened the
/// replay: a crossing this driver really has folded must still be history on the
/// next recovery, or re-running the same recovery over the same durable state
/// manufactures a removal on the second pass that it did not on the first.
///
/// So this is the first recovery's own record fed back in, over a history that
/// only ever *adds*. Every retirement derivable from it is manufactured, so a
/// crossing position that stopped gating shows up immediately: re-folding the
/// entry at index 6 against a live set that already reflects index 7 reads as a
/// removal of node 5 and permanently fences the replica the cluster admitted
/// last.
#[test]
fn a_crossing_position_still_suppresses_the_crossings_beneath_it() {
    let (first, _) = adopt_under(PeerControlPlaneCheckpoint::empty(GROUP), ONLY_ADDITIONS);
    let persisted = first.control_plane_checkpoint();
    assert_eq!(
        persisted.committed_crossings_through,
        Some(REMOVED_AT),
        "the first recovery earned a crossing position at the last entry it \
         folded: {persisted:?}"
    );

    let (second, transport) = adopt_under(persisted.clone(), ONLY_ADDITIONS);

    assert_eq!(
        second.control_plane_checkpoint(),
        persisted,
        "the second recovery re-derived a different retirement record from the \
         same durable state"
    );
    assert!(
        transport.fence_attempts().is_empty(),
        "a log that removed nobody owes no fence, however many times it is \
         replayed: {:?}",
        transport.fence_attempts()
    );
}

/// An endpoint position still suppresses the endpoint observations beneath it.
///
/// The gate the endpoint position keeps for itself, and it is not decoration. A
/// commit index is volatile: a rebuilt runtime can legitimately report a lower
/// one than the incarnation that wrote the record reached. An ungated endpoint
/// fold therefore diffs the runtime's committed configuration against a live set
/// that has already moved past it and retires everything the newer
/// configurations added.
///
/// The record here says so directly — live `{1,2,3,4,5}` observed at commit 10,
/// with the crossings beneath already folded — while the runtime it is joined
/// into holds `{1,2,3}` at commit 7. Ungated, the endpoint reads as a removal of
/// nodes 4 and 5 and owes two permanent fences for replicas the cluster never
/// removed.
#[test]
fn an_endpoint_position_still_suppresses_the_endpoints_beneath_it() {
    let mut ahead = PeerControlPlaneCheckpoint::empty(GROUP);
    ahead.committed_id_high_water = Some(NodeId(5));
    ahead.live_committed_members = [NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)]
        .into_iter()
        .collect();
    ahead.committed_crossings_through = Some(REMOVED_AT);
    ahead.committed_endpoint_through = Some(SNAPSHOT_COMMIT);

    let (driver, transport) = adopt_under(ahead, ADMIT_THEN_REMOVE);

    let checkpoint = driver.control_plane_checkpoint();
    assert_eq!(
        checkpoint.live_committed_members,
        [NodeId(1), NodeId(2), NodeId(3), NodeId(4), NodeId(5)]
            .into_iter()
            .collect(),
        "the runtime's endpoint stands beneath the position this record already \
         holds, so folding it again would only manufacture removals"
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
