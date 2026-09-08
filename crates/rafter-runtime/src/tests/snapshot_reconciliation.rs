//! Snapshot suffix retention must survive every durable installation boundary.

use super::*;
use snapshot::{persisted_entry, raft_snapshot};

mod batched_suffix;
mod file_backed;
use super::mutation_cuts::{Cut, Mutation, Observed};

type MemoryImage = DurableRaftNodeStorage<
    InMemoryRaftHardStateStore,
    InMemoryRaftLogSegment,
    InMemoryRaftSnapshotStore,
>;

#[test]
fn single_step_snapshot_reconciliation_survives_every_crash_cut() {
    assert_memory_cuts(false);
}

#[test]
fn batched_snapshot_reconciliation_survives_every_crash_cut() {
    assert_memory_cuts(true);
}

#[test]
fn snapshot_reconciliation_after_promotion_reopens() {
    assert_specific_cut(Mutation::Promote);
}

#[test]
fn snapshot_reconciliation_after_compaction_discards_conflicting_suffix() {
    assert_specific_cut(Mutation::Compact);
}

fn assert_specific_cut(mutation: Mutation) {
    for batched in [false, true] {
        let (mutations, _) = memory_install(false, batched, None);
        let after = mutations
            .iter()
            .position(|value| *value == mutation)
            .unwrap()
            + 1;
        let (_, image) = memory_install(false, batched, Some(after));
        let mut reopened = DurableRaftNode::with_storage_and_snapshot_store(
            raft_config(2, &[1, 3]),
            image.hard_state_store,
            image.log_segment,
            image.snapshot_store,
        )
        .expect("interrupted snapshot installation reopens");
        assert_recovered(&mut reopened, false);
    }
}

fn assert_memory_cuts(batched: bool) {
    for matching in [false, true] {
        let (mutations, _) = memory_install(matching, batched, None);
        assert!(mutations.contains(&Mutation::Stage));
        assert!(mutations.contains(&Mutation::Promote));
        assert!(mutations.contains(&Mutation::Compact));
        assert_eq!(mutations.last(), Some(&Mutation::HardState));
        for after in 1..=mutations.len() {
            let (observed, image) = memory_install(matching, batched, Some(after));
            assert_eq!(observed, mutations[..after]);
            if after < mutations.len() {
                assert_eq!(
                    image.hard_state_store.current().commit_index,
                    LogIndex::ZERO
                );
                assert_eq!(
                    image.hard_state_store.current().committed_configuration,
                    None
                );
            }
            let mut reopened = DurableRaftNode::with_storage_and_snapshot_store(
                raft_config(2, &[1, 3]),
                image.hard_state_store,
                image.log_segment,
                image.snapshot_store,
            )
            .unwrap_or_else(|error| panic!("matching={matching}, cut {observed:?}: {error}"));
            if after == 1 {
                assert_eq!(reopened.snapshot_index(), LogIndex::ZERO);
                assert_eq!(reopened.last_log_index(), LogIndex(3));
                assert!(reopened.drain_committed_outputs().is_empty());
            } else {
                assert_recovered(&mut reopened, matching);
                // Recovery writes must survive another independent reopen.
                let image = reopened.into_storage();
                let mut again = DurableRaftNode::with_storage_and_snapshot_store(
                    raft_config(2, &[1, 3]),
                    image.hard_state_store,
                    image.log_segment,
                    image.snapshot_store,
                )
                .expect("recovery is durable and idempotent");
                assert_recovered(&mut again, matching);
            }
        }
    }
}

fn memory_install(
    matching: bool,
    batched: bool,
    after: Option<usize>,
) -> (Vec<Mutation>, MemoryImage) {
    let mut hard_state = InMemoryRaftHardStateStore::new();
    let mut log = InMemoryRaftLogSegment::new();
    initialize(&mut hard_state, &mut log);
    let cut = Cut::new(after);
    let mut runtime = DurableRaftNode::with_storage_and_snapshot_store(
        raft_config(2, &[1, 3]),
        cut.wrap(hard_state),
        cut.wrap(log),
        cut.wrap(InMemoryRaftSnapshotStore::new()),
    )
    .expect("initial follower opens");
    drive(&mut runtime, matching, batched, after.is_some());
    let image = runtime.into_storage();
    (
        cut.mutations(),
        DurableRaftNodeStorage {
            hard_state_store: image.hard_state_store.inner,
            log_segment: image.log_segment.inner,
            snapshot_store: image.snapshot_store.inner,
        },
    )
}

fn initialize<H: RaftHardStateStore, L: RaftLogSegment>(hard_state: &mut H, log: &mut L) {
    hard_state
        .write_hard_state(hard_state_store(1, None).current())
        .expect("initial term persists");
    log.append_entries(&[
        persisted_entry(1, 1, b"shared"),
        persisted_entry(2, 1, b"stale-boundary"),
        persisted_entry(3, 1, b"stale-tail"),
    ])
    .expect("initial log persists");
}

fn incoming_snapshot(matching: bool) -> PersistedRaftSnapshot {
    raft_snapshot(2, if matching { 1 } else { 2 }, 3, b"snapshot state")
}

fn install_input(matching: bool) -> RaftInput {
    let snapshot = incoming_snapshot(matching);
    let descriptor =
        RaftSnapshot::from_payload(snapshot.metadata.clone(), &snapshot.application_payload);
    RaftInput::Message {
        from: RaftNodeId(1),
        message: Message::InstallSnapshotChunk(rafter::InstallSnapshotChunk {
            term: Term(3),
            leader_id: RaftNodeId(1),
            transfer_id: descriptor.transfer_id(),
            metadata: snapshot.metadata,
            total_payload_len: descriptor.application_payload_len,
            application_payload_crc32: descriptor.application_payload_crc32,
            offset: 0,
            chunk: snapshot.application_payload,
            done: true,
        }),
    }
}

fn drive<H: RaftHardStateStore, L: RaftLogSegment, S: RaftSnapshotStore + SnapshotChunkSource>(
    runtime: &mut DurableRaftNode<Observed<H>, Observed<L>, Observed<S>>,
    matching: bool,
    batched: bool,
    failing: bool,
) {
    let input = install_input(matching);
    let result = if batched {
        runtime.step_batch(vec![input, RaftInput::Tick])
    } else {
        runtime.step(input)
    };
    if failing {
        assert!(result.is_err(), "armed crash cut must fire");
        assert!(matches!(
            runtime.step(RaftInput::Tick),
            Err(RaftRuntimeError::Poisoned { .. })
        ));
    } else {
        let outputs = result.expect("snapshot installs");
        assert!(outputs.iter().any(|output| matches!(output,
            RaftOutput::Send { message: Message::InstallSnapshotResponse(response), .. }
                if response.success && response.last_included_index == LogIndex(2)
        )));
        assert_recovered(runtime, matching);
    }
}

fn assert_recovered<
    H: RaftHardStateStore,
    L: RaftLogSegment,
    S: RaftSnapshotStore + SnapshotChunkSource,
>(
    runtime: &mut DurableRaftNode<H, L, S>,
    matching: bool,
) {
    assert_eq!(runtime.current_term(), Term(3));
    assert_eq!(runtime.snapshot_index(), LogIndex(2));
    assert_eq!(
        runtime.term_at_index(LogIndex(2)),
        Some(Term(if matching { 1 } else { 2 }))
    );
    assert_eq!(runtime.commit_index(), LogIndex(2));
    assert_eq!(
        runtime.last_log_index(),
        LogIndex(if matching { 3 } else { 2 })
    );
    let expected = if matching {
        vec![LogEntry::application(Term(1), b"stale-tail".to_vec())]
    } else {
        Vec::new()
    };
    assert_eq!(runtime.log_entries_from(LogIndex(3)), expected);
    let persisted = runtime
        .log_segment
        .replay_entries()
        .into_iter()
        .filter(|entry| entry.index > LogIndex(2))
        .collect::<Vec<_>>();
    assert_eq!(
        persisted,
        if matching {
            vec![persisted_entry(3, 1, b"stale-tail")]
        } else {
            Vec::new()
        }
    );
    assert!(runtime
        .snapshot_store
        .current_pending_snapshot_transfer()
        .is_none());
    assert!(runtime
        .drain_committed_outputs()
        .iter()
        .all(|output| !matches!(output, RaftOutput::Apply { .. })));
}
