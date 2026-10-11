//! Real kernel and workers with an observable in-memory link.
pub(super) use crate::reads_support::{App, Entry, Release};
pub(super) use rafter::{Input, LogIndex, NodeId, Role};
pub(super) use rafter_runtime::service::{ServicePoll, ServiceState};
use rafter_service::{
    durable::DurableRaftDriver, PeerEnvelope, PeerPolicy, RaftTransport, SnapshotChunkEnvelope,
};
pub(super) use rafter_service::{
    DriverCommandSender, ReadConsistency, ReadError, ReadOptions, ShutdownError,
    TransferLeadershipError, WriteError, WriteOptions,
};
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
};
use std::{
    collections::VecDeque,
    sync::{mpsc, Mutex},
    time::{Duration, Instant},
};
pub(super) use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    task::{Context, Poll, Waker},
};
pub const GROUP: u64 = 42;
pub type Driver = DurableRaftDriver<
    u64,
    InMemoryRaftHardStateStore,
    InMemoryRaftLogSegment,
    InMemoryRaftSnapshotStore,
    Entry,
    App,
    Link,
    u8,
>;
#[derive(Clone, Default)]
pub struct Link {
    pub network: Arc<Mutex<VecDeque<PeerEnvelope<u64>>>>,
    pub refusing: Arc<AtomicBool>,
    pub panicking: Arc<AtomicBool>,
    pub sends: Arc<AtomicUsize>,
}
impl RaftTransport<u64> for Link {
    type PeerPrincipal = NodeId;
    type Error = std::io::Error;
    fn send(&self, message: PeerEnvelope<u64>) -> Result<(), Self::Error> {
        self.sends.fetch_add(1, Ordering::AcqRel);
        assert!(
            !self.panicking.load(Ordering::Acquire),
            "injected transport panic"
        );
        if self.refusing.load(Ordering::Acquire) {
            return Err(std::io::Error::other("injected link refusal"));
        }
        self.network.lock().unwrap().push_back(message);
        Ok(())
    }
    fn send_snapshot_chunk(&self, _: SnapshotChunkEnvelope<u64>) -> Result<(), Self::Error> {
        panic!("durable runtime resolves chunks");
    }
    fn update_peers(&self, _: &u64, _: PeerPolicy<NodeId>) -> Result<(), Self::Error> {
        panic!("fixture owns existing membership policy");
    }
}
pub struct Replica {
    pub release: Release,
    pub driver: Driver,
    pub entered: mpsc::Receiver<()>,
    pub dropped: Arc<AtomicBool>,
    pub queries: Arc<AtomicUsize>,
    pub reads: rafter_service::durable::DurableReadSender<u8>,
    pub writes: rafter_service::durable::DurableWriteSender<crate::write_support::Outcome>,
}
impl Replica {
    pub fn attach(node: crate::reads_support::Replica, link: Link) -> Self {
        let writes = node.owner.sender();
        Self {
            release: node.release,
            driver: Driver::attach(GROUP, node.owner, link).unwrap(),
            entered: node.entered,
            dropped: node.dropped,
            queries: node.queries,
            reads: node.sender,
            writes,
        }
    }
    pub fn single(paused: bool) -> Self {
        Self::attach(
            crate::reads_support::Replica::single(4, paused),
            Link::default(),
        )
    }
    pub fn pump(&mut self) -> bool {
        !matches!(self.driver.poll_next().unwrap(), ServicePoll::Pending)
    }
    pub fn until(&mut self, mut done: impl FnMut(&Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done(self) {
            self.pump();
            assert!(Instant::now() < deadline, "driver stalled");
            std::thread::yield_now();
        }
    }
    pub fn drain(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.pump() {
            assert!(Instant::now() < deadline);
        }
    }
}
pub struct Cluster {
    pub nodes: Vec<Replica>,
    pub link: Link,
}
impl Cluster {
    pub fn new() -> Self {
        let base = crate::reads_support::Cluster::new(4);
        assert!(base.network.is_empty());
        let link = Link::default();
        let nodes = base
            .nodes
            .into_iter()
            .map(|n| Replica::attach(n, link.clone()))
            .collect();
        Self { nodes, link }
    }
    pub fn turn(&mut self) -> bool {
        let mut progress = false;
        for node in &mut self.nodes {
            progress |= node.pump();
        }
        let count = self.link.network.lock().unwrap().len();
        for _ in 0..count {
            let frame = self.link.network.lock().unwrap().pop_front().unwrap();
            assert_eq!(frame.group_id, GROUP);
            let node = &mut self.nodes[usize::try_from(frame.to.0).unwrap() - 1];
            if node.driver.state() == ServiceState::Ready {
                node.driver
                    .try_step(Input::Message {
                        from: frame.from,
                        message: frame.message,
                    })
                    .unwrap();
                progress = true;
                break;
            }
            self.link.network.lock().unwrap().push_back(frame);
        }
        progress
    }
    pub fn until(&mut self, mut done: impl FnMut(&Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done(self) {
            self.turn();
            assert!(Instant::now() < deadline, "group cluster stalled");
            std::thread::yield_now();
        }
    }
}
pub fn poll<F: Future + ?Sized>(future: &mut Pin<Box<F>>) -> Poll<F::Output> {
    future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
}
