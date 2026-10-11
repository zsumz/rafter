//! Small application store and channel gates for deterministic owner tests.

use super::super::*;
use crate::application::{ApplicationEntry, DurableApplication};
use crate::{DurableRaftNode, DurableRaftNodeStorage};
use rafter::{Input, LocalProposalId, LogIndex, NodeConfig, NodeId};
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
};
use std::{
    io,
    sync::mpsc,
    time::{Duration, Instant},
};

pub(super) type Node =
    DurableRaftNode<InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore>;
pub(super) type Owner = DurableService<
    InMemoryRaftHardStateStore,
    InMemoryRaftLogSegment,
    InMemoryRaftSnapshotStore,
    Entry,
    Store,
>;
pub(super) type Event = ServiceEvent<Entry, Store>;

#[derive(Debug)]
pub(super) struct Entry {
    pub first: LogIndex,
    pub last: LogIndex,
    pub id: Option<LocalProposalId>,
}
impl ApplicationEntry for Entry {
    fn first_log_index(&self) -> LogIndex {
        self.first
    }
    fn log_index(&self) -> LogIndex {
        self.last
    }
    fn retained_bytes(&self) -> usize {
        64
    }
}
#[allow(
    clippy::unnecessary_wraps,
    reason = "the public adapter contract is fallible"
)]
pub(super) fn adapt(command: CommittedCommand) -> Result<Entry, ServiceAdapterError> {
    drop(command.payload);
    Ok(Entry {
        first: command.first_index,
        last: command.index,
        id: command.local_proposal_id,
    })
}
#[derive(Debug, Default)]
pub(super) struct Store {
    pub floor: LogIndex,
    pub applied: Vec<LogIndex>,
    pub gate: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    pub fail: bool,
    pub panic: bool,
    pub wrong_floor: bool,
}
impl DurableApplication<Entry> for Store {
    type Outcome = Option<LocalProposalId>;
    type Error = io::Error;
    fn applied_through(&self) -> LogIndex {
        self.floor
    }
    fn apply(&mut self, entries: &[Entry]) -> io::Result<Vec<Self::Outcome>> {
        if let Some((started, release)) = self.gate.take() {
            started.send(()).unwrap();
            release
                .recv_timeout(Duration::from_secs(10))
                .expect("gate released");
        }
        assert!(!self.panic, "injected application panic");
        if self.fail {
            return Err(io::Error::other("injected application failure"));
        }
        for entry in entries {
            assert_eq!(entry.first.0, self.floor.0 + 1);
            self.floor = entry.last;
            self.applied.push(entry.last);
        }
        if self.wrong_floor {
            self.floor = LogIndex::ZERO;
        }
        Ok(entries.iter().map(|entry| entry.id).collect())
    }
}
/// Releases on unwind too, so a failed assertion cannot strand a joined worker.
pub(super) struct Release(pub Option<mpsc::SyncSender<()>>);
impl Release {
    pub fn now(&mut self) {
        if let Some(tx) = self.0.take() {
            let _ = tx.send(());
        }
    }
}
impl Drop for Release {
    fn drop(&mut self) {
        self.now();
    }
}
pub(super) fn gate(store: &mut Store) -> (mpsc::Receiver<()>, Release) {
    let (started, wait) = mpsc::sync_channel(1);
    let (release, resume) = mpsc::sync_channel(1);
    store.gate = Some((started, resume));
    (wait, Release(Some(release)))
}
pub(super) fn config() -> NodeConfig {
    NodeConfig::new(NodeId(1), vec![], 1).unwrap()
}
pub(super) fn log_with_commands(count: usize) -> Node {
    let mut node = DurableRaftNode::new(config(), InMemoryRaftHardStateStore::new()).unwrap();
    node.step(Input::Tick).unwrap();
    for n in 0..count {
        node.step(Input::ClientProposal {
            payload: n.to_string().into_bytes(),
        })
        .unwrap();
    }
    node
}
pub(super) fn owner(node: Node, store: Store, credits: usize) -> Owner {
    let mut options = ServiceOptions::new();
    options.application = options
        .application
        .with_limits(credits, credits * 64, 1, 64);
    Owner::recover(config(), node.into_storage(), store, options, adapt).unwrap()
}
pub(super) fn empty() -> Owner {
    Owner::recover(
        config(),
        DurableRaftNodeStorage {
            hard_state_store: InMemoryRaftHardStateStore::new(),
            log_segment: InMemoryRaftLogSegment::new(),
            snapshot_store: InMemoryRaftSnapshotStore::new(),
        },
        Store::default(),
        ServiceOptions::new(),
        adapt,
    )
    .unwrap()
}
pub(super) fn until(owner: &mut Owner, mut done: impl FnMut(&Owner) -> bool) -> Vec<Event> {
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut events = Vec::new();
    while !done(owner) {
        assert!(
            Instant::now() < deadline,
            "owner made no progress: {owner:?}"
        );
        if let Some(event) = owner.poll().unwrap() {
            events.push(event);
        }
        std::thread::yield_now();
    }
    events
}
