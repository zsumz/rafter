//! Real workers with bounded admission and injected application faults.
pub use rafter::{Input, LocalProposalId, LogIndex, NodeConfig, NodeId};
use rafter_runtime::{
    application::{ApplicationEntry, DurableApplication},
    service::{CommittedCommand, ServiceOptions, ServicePoll, ServiceState},
    DurableRaftNodeStorage,
};
pub use rafter_service::{
    durable::{DurableClientOwner, DurableClientStepError, DurableWriteLimits, DurableWriteSender},
    DriverFuture, WriteError, WriteFate, WriteOptions,
};
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
};
use std::{io, num::NonZeroUsize, task::Wake, time::Instant};
pub use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc, Arc,
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

#[derive(Debug)]
pub struct Entry {
    first: LogIndex,
    index: LogIndex,
    value: u8,
}
impl ApplicationEntry for Entry {
    fn first_log_index(&self) -> LogIndex {
        self.first
    }
    fn log_index(&self) -> LogIndex {
        self.index
    }
    fn retained_bytes(&self) -> usize {
        128
    }
}
#[derive(Debug)]
pub struct Outcome {
    pub value: u8,
}
impl Clone for Outcome {
    fn clone(&self) -> Self {
        assert_ne!(self.value, 255, "injected outcome clone panic");
        Self { value: self.value }
    }
}
#[derive(Debug)]
pub struct App {
    pub floor: LogIndex,
    gate: Option<(mpsc::SyncSender<()>, mpsc::Receiver<()>)>,
    dropped: Arc<AtomicBool>,
}
impl DurableApplication<Entry> for App {
    type Outcome = Outcome;
    type Error = io::Error;
    fn applied_through(&self) -> LogIndex {
        self.floor
    }
    fn apply(&mut self, entries: &[Entry]) -> io::Result<Vec<Outcome>> {
        if let Some((entered, release)) = self.gate.take() {
            entered.send(()).unwrap();
            release.recv().unwrap();
        }
        if entries.iter().any(|entry| entry.value == 254) {
            return Err(io::Error::other("injected application failure"));
        }
        self.floor = entries.last().unwrap().index;
        Ok(entries
            .iter()
            .map(|entry| Outcome { value: entry.value })
            .collect())
    }
}
impl Drop for App {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::Release);
    }
}
pub type Owner = DurableClientOwner<
    InMemoryRaftHardStateStore,
    InMemoryRaftLogSegment,
    InMemoryRaftSnapshotStore,
    Entry,
    App,
>;
pub struct Release(Option<mpsc::SyncSender<()>>);
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

pub fn setup(
    limit: usize,
    paused: bool,
) -> (
    Owner,
    DurableWriteSender<Outcome>,
    mpsc::Receiver<()>,
    Release,
    Arc<AtomicBool>,
) {
    let (entered, wait) = mpsc::sync_channel(1);
    let (release, resume) = mpsc::sync_channel(1);
    let dropped = Arc::new(AtomicBool::new(false));
    let application = App {
        floor: LogIndex::ZERO,
        gate: paused.then_some((entered, resume)),
        dropped: Arc::clone(&dropped),
    };
    let mut owner = Owner::recover(
        NodeConfig::new(NodeId(1), vec![], 1).unwrap(),
        DurableRaftNodeStorage {
            hard_state_store: InMemoryRaftHardStateStore::new(),
            log_segment: InMemoryRaftLogSegment::new(),
            snapshot_store: InMemoryRaftSnapshotStore::new(),
        },
        application,
        ServiceOptions::new(),
        DurableWriteLimits {
            writes: NonZeroUsize::new(limit).unwrap(),
            payload_bytes: NonZeroUsize::new(64).unwrap(),
        },
        |command: CommittedCommand| {
            Ok(Entry {
                first: command.first_index,
                index: command.index,
                value: command.payload.as_slice()[0],
            })
        },
        || {},
    )
    .unwrap();
    until(&mut owner, |owner| owner.state() == ServiceState::Ready);
    owner.try_step(Input::Tick).unwrap();
    drain(&mut owner);
    let sender = owner.sender();
    (owner, sender, wait, Release(Some(release)), dropped)
}
pub fn until(owner: &mut Owner, mut done: impl FnMut(&Owner) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(5);
    while !done(owner) {
        let _ = owner.poll_next().unwrap();
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
}
pub fn drain(owner: &mut Owner) {
    while !matches!(owner.poll_next().unwrap(), ServicePoll::Pending) {}
}
struct Noop;
impl Wake for Noop {
    fn wake(self: Arc<Self>) {}
}
pub fn poll<T>(future: &mut DriverFuture<T>) -> Poll<T> {
    future
        .as_mut()
        .poll(&mut Context::from_waker(&Waker::from(Arc::new(Noop))))
}
pub struct CheckReleased(pub Arc<AtomicBool>);
impl Wake for CheckReleased {
    fn wake(self: Arc<Self>) {
        assert!(
            self.0.load(Ordering::Acquire),
            "client awakened before backend release"
        );
    }
}
