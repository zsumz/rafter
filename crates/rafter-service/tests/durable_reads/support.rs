//! Each fixture owns real persistence and application workers.

pub(super) use crate::write_support::{
    drain, poll, until, App, CheckReleased, Entry, Owner, Release,
};
pub(super) use rafter::{Input, LogIndex, Message, NodeConfig, NodeId, Output, ReadId, Role};
pub(super) use rafter_app::read::ReadConsistency;
pub(super) use rafter_runtime::service::{ServiceEvent, ServicePoll, ServiceState};
use rafter_runtime::{service::ServiceOptions, DurableRaftNodeStorage};
pub(super) use rafter_service::{
    durable::{DurableClientOptions, DurableReadLimits, DurableReadSender, DurableWriteLimits},
    DriverFuture, QueryReceipt, ReadError, ReadOptions, WriteOptions,
};
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
};
pub(super) use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc,
    },
    task::{Context, Poll, Waker},
    time::Duration,
};
use std::{num::NonZeroUsize, time::Instant};

pub(super) struct Replica {
    // Release a paused worker before dropping its owner, including on assertion failure.
    pub release: Release,
    pub owner: Owner<u8>,
    pub sender: DurableReadSender<u8>,
    pub entered: mpsc::Receiver<()>,
    pub dropped: Arc<AtomicBool>,
    pub queries: Arc<AtomicUsize>,
}
impl Replica {
    pub fn new(id: u64, peers: Vec<NodeId>, limit: usize, paused: bool) -> Self {
        Self::new_with_wake(id, peers, limit, paused, || {})
    }
    pub fn new_with_wake(
        id: u64,
        peers: Vec<NodeId>,
        limit: usize,
        paused: bool,
        wake: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        let (entered, wait) = mpsc::sync_channel(1);
        let (release, resume) = mpsc::sync_channel(1);
        let dropped = Arc::new(AtomicBool::new(false));
        let queries = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&queries);
        let mut owner = Owner::<u8>::recover_with_reads(
            NodeConfig::new(NodeId(id), peers, 2)
                .unwrap()
                .with_pre_vote(false),
            DurableRaftNodeStorage {
                hard_state_store: InMemoryRaftHardStateStore::new(),
                log_segment: InMemoryRaftLogSegment::new(),
                snapshot_store: InMemoryRaftSnapshotStore::new(),
            },
            App {
                floor: LogIndex::ZERO,
                value: 0,
                gate: paused.then_some((entered, resume)),
                dropped: Arc::clone(&dropped),
            },
            DurableClientOptions::new(
                ServiceOptions::new(),
                DurableWriteLimits {
                    writes: NonZeroUsize::new(4).unwrap(),
                    payload_bytes: NonZeroUsize::new(64).unwrap(),
                },
                DurableReadLimits {
                    reads: NonZeroUsize::new(limit).unwrap(),
                    payload_bytes: NonZeroUsize::new(64).unwrap(),
                },
            ),
            |command| {
                Ok(Entry {
                    first: command.first_index,
                    index: command.index,
                    value: command.payload.as_slice()[0],
                })
            },
            move |app, bytes| {
                observed.fetch_add(1, Ordering::AcqRel);
                assert_ne!(bytes[0], 255, "injected query panic");
                app.value
            },
            wake,
        )
        .unwrap();
        until(&mut owner, |o| o.state() == ServiceState::Ready);
        let sender = owner.read_sender().unwrap();
        Self {
            owner,
            sender,
            entered: wait,
            release: Release(Some(release)),
            dropped,
            queries,
        }
    }
    pub fn single(limit: usize, paused: bool) -> Self {
        let mut node = Self::new(1, vec![], limit, paused);
        for _ in 0..3 {
            node.owner.try_step(Input::Tick).unwrap();
            drain(&mut node.owner);
        }
        assert_eq!(node.owner.ready_node().unwrap().role(), Role::Leader);
        node
    }
    pub fn read(
        &self,
        consistency: ReadConsistency,
    ) -> DriverFuture<Result<QueryReceipt<(), u8>, ReadError>> {
        self.sender
            .read(vec![0], consistency, ReadOptions::default())
    }
}

pub(super) struct Cluster {
    pub nodes: Vec<Replica>,
    pub network: VecDeque<(NodeId, NodeId, Message)>,
    pub grants: Vec<ReadId>,
}
impl Cluster {
    pub fn new(limit: usize) -> Self {
        Self::new_with_paused_leader(limit, false)
    }
    pub fn new_with_paused_leader(limit: usize, paused: bool) -> Self {
        let mut cluster = Self {
            nodes: (1..=3)
                .map(|id| {
                    Replica::new(
                        id,
                        (1..=3).filter(|n| *n != id).map(NodeId).collect(),
                        limit,
                        paused && id == 1,
                    )
                })
                .collect(),
            network: VecDeque::new(),
            grants: Vec::new(),
        };
        for _ in 0..3 {
            cluster.nodes[0].owner.try_step(Input::Tick).unwrap();
            cluster.settle();
        }
        cluster.until(|c| {
            c.nodes[0]
                .owner
                .ready_node()
                .is_some_and(|n| n.role() == Role::Leader && n.commit_index() >= LogIndex(1))
        });
        cluster.settle();
        cluster
    }
    pub fn pump(&mut self, i: usize) -> bool {
        match self.nodes[i].owner.poll_next().unwrap() {
            ServicePoll::Event(ServiceEvent::Output(Output::Send { to, message })) => {
                self.network.push_back((NodeId(i as u64 + 1), to, message));
                true
            }
            ServicePoll::Event(ServiceEvent::Output(Output::ReadIndexGranted {
                read_id, ..
            })) => {
                self.grants.push(read_id);
                true
            }
            ServicePoll::Pending => false,
            _ => true,
        }
    }
    pub fn turn(&mut self) -> bool {
        let mut progress = false;
        for i in 0..self.nodes.len() {
            progress |= self.pump(i);
        }
        for _ in 0..self.network.len() {
            let (from, to, message) = self.network.pop_front().unwrap();
            let node = &mut self.nodes[usize::try_from(to.0).unwrap() - 1];
            if node.owner.state() == ServiceState::Ready {
                node.owner
                    .try_step(Input::Message { from, message })
                    .unwrap();
                progress = true;
                break;
            }
            self.network.push_back((from, to, message));
        }
        progress
    }
    pub fn until(&mut self, mut done: impl FnMut(&Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !done(self) {
            self.turn();
            assert!(Instant::now() < deadline);
            std::thread::yield_now();
        }
    }
    pub fn settle(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while self.turn() {
            assert!(Instant::now() < deadline);
        }
    }
}
