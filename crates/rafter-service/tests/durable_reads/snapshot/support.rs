//! Recovery from an actual committed and compacted in-memory log.
use crate::read_tests::support::*;
use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion,
    RaftSnapshotMetadata, SnapshotGroupId,
};
use rafter_runtime::{application::DurableApplication, service::ServiceOptions, DurableRaftNode};
use rafter_service::durable::DurableClientOwner;
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
    PersistedRaftSnapshot,
};
use std::{io, num::NonZeroUsize};

pub(super) const FLOOR: LogIndex = LogIndex(2);
pub(super) type SnapshotOwner = DurableClientOwner<
    InMemoryRaftHardStateStore,
    InMemoryRaftLogSegment,
    InMemoryRaftSnapshotStore,
    Entry,
    SnapshotApp,
    u8,
>;

#[derive(Debug)]
pub(super) struct SnapshotApp {
    pub floor: LogIndex,
    pub value: u8,
    pub panic_floor: bool,
}
impl DurableApplication<Entry> for SnapshotApp {
    type Outcome = crate::write_support::Outcome;
    type Error = io::Error;
    fn applied_through(&self) -> LogIndex {
        assert!(!self.panic_floor, "injected snapshot floor panic");
        self.floor
    }
    fn apply(&mut self, entries: &[Entry]) -> io::Result<Vec<Self::Outcome>> {
        self.floor = entries.last().unwrap().index;
        self.value = entries.last().unwrap().value;
        Ok(entries
            .iter()
            .map(|e| Self::Outcome { value: e.value })
            .collect())
    }
}

pub(super) fn handoff() -> (SnapshotOwner, SnapshotApp) {
    let config = NodeConfig::new(NodeId(1), vec![], 1).unwrap();
    let mut node = DurableRaftNode::new(config.clone(), InMemoryRaftHardStateStore::new()).unwrap();
    node.step(Input::Tick).unwrap();
    node.step(Input::ClientProposal { payload: vec![9] })
        .unwrap();
    assert_eq!(node.commit_index(), FLOOR);
    let metadata = RaftSnapshotMetadata::new(
        SnapshotGroupId::new("durable-client").unwrap(),
        node.id(),
        FLOOR,
        node.term_at_index(FLOOR).unwrap(),
        node.current_term(),
        ApplicationSnapshotMetadata::new(
            ApplicationSnapshotKind::new("counter").unwrap(),
            ApplicationSnapshotVersion::new(1).unwrap(),
        ),
    )
    .unwrap();
    node.compact_log_with_snapshot(PersistedRaftSnapshot {
        metadata,
        application_payload: vec![9],
    })
    .unwrap();
    let limit = NonZeroUsize::new(4).unwrap();
    let bytes = NonZeroUsize::new(64).unwrap();
    let mut owner = SnapshotOwner::recover_with_reads(
        config,
        node.into_storage(),
        SnapshotApp {
            floor: LogIndex::ZERO,
            value: 0,
            panic_floor: false,
        },
        DurableClientOptions::new(
            ServiceOptions::new(),
            DurableWriteLimits {
                writes: limit,
                payload_bytes: bytes,
            },
            DurableReadLimits {
                reads: limit,
                payload_bytes: bytes,
            },
        ),
        |command| {
            Ok(Entry {
                first: command.first_index,
                index: command.index,
                value: command.payload[0],
            })
        },
        |app, _| app.value,
        || {},
    )
    .unwrap();
    let ServicePoll::Event(ServiceEvent::InstallSnapshot {
        snapshot,
        application,
    }) = owner.poll_next().unwrap()
    else {
        panic!("missing snapshot handoff")
    };
    assert_eq!(snapshot.metadata.last_included_index, FLOOR);
    (owner, application)
}
