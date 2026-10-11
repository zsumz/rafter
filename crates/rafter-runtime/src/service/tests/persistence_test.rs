//! Real quorum responses cannot bypass a stalled or failed persistence thread.

use super::{
    super::*,
    support::{adapt, Entry, Release, Store},
};
use crate::{DurableRaftNode, DurableRaftNodeStorage};
use rafter::{ClientProposalInput, Input, LogIndex, Message, NodeConfig, NodeId, Output};
use rafter_storage::{
    InMemoryRaftHardStateStore, InMemoryRaftLogSegment, InMemoryRaftSnapshotStore,
    PersistedRaftLogEntry, RaftLogSegment, RaftLogSegmentAppendError, RaftLogSegmentCompactError,
    RaftLogSegmentTruncateError,
};
use std::{
    collections::VecDeque,
    sync::{mpsc, Arc, Mutex},
    time::{Duration, Instant},
};

type Gate = (mpsc::SyncSender<()>, mpsc::Receiver<()>, bool);
#[derive(Debug, Default)]
struct GatedLog {
    inner: InMemoryRaftLogSegment,
    gate: Arc<Mutex<Option<Gate>>>,
}
impl RaftLogSegment for GatedLog {
    fn append_entries(
        &mut self,
        entries: &[PersistedRaftLogEntry],
    ) -> Result<(), RaftLogSegmentAppendError> {
        let gate = self.gate.lock().unwrap().take();
        if let Some((started, release, panic)) = gate {
            started.send(()).unwrap();
            release.recv_timeout(Duration::from_secs(10)).unwrap();
            assert!(!panic, "injected storage panic");
        }
        self.inner.append_entries(entries)
    }
    fn truncate_suffix(&mut self, i: LogIndex) -> Result<(), RaftLogSegmentTruncateError> {
        self.inner.truncate_suffix(i)
    }
    fn compact_prefix_through(&mut self, i: LogIndex) -> Result<(), RaftLogSegmentCompactError> {
        self.inner.compact_prefix_through(i)
    }
    fn next_index(&self) -> LogIndex {
        self.inner.next_index()
    }
    fn compacted_through(&self) -> LogIndex {
        self.inner.compacted_through()
    }
    fn replay_entries(&self) -> Vec<PersistedRaftLogEntry> {
        self.inner.replay_entries()
    }
}
type Owner =
    DurableService<InMemoryRaftHardStateStore, GatedLog, InMemoryRaftSnapshotStore, Entry, Store>;
struct Cluster {
    owner: Owner,
    follower: DurableRaftNode,
    responses: VecDeque<Input>,
    gate: Arc<Mutex<Option<Gate>>>,
}
impl Cluster {
    fn new() -> Self {
        let config = |id| {
            NodeConfig::new(
                NodeId(id),
                (1..=3).filter(|n| *n != id).map(NodeId).collect(),
                1,
            )
            .unwrap()
        };
        let log = GatedLog::default();
        let gate = Arc::clone(&log.gate);
        let mut owner = Owner::recover(
            config(1),
            DurableRaftNodeStorage {
                hard_state_store: InMemoryRaftHardStateStore::new(),
                log_segment: log,
                snapshot_store: InMemoryRaftSnapshotStore::new(),
            },
            Store::default(),
            ServiceOptions::new(),
            adapt,
        )
        .unwrap();
        owner.poll().unwrap();
        let follower = DurableRaftNode::new(config(2), InMemoryRaftHardStateStore::new()).unwrap();
        let mut cluster = Self {
            owner,
            follower,
            responses: VecDeque::new(),
            gate,
        };
        cluster
            .owner
            .try_step(ServiceInput::from(Input::Tick))
            .unwrap();
        cluster.drive();
        assert_eq!(
            cluster.owner.ready_node().unwrap().role(),
            rafter::Role::Leader
        );
        assert_eq!(cluster.owner.progress().committed, LogIndex(1));
        cluster
    }
    fn route(&mut self, output: Output) {
        if let Output::Send {
            to: NodeId(2),
            message,
        } = output
        {
            for output in self
                .follower
                .step(Input::Message {
                    from: NodeId(1),
                    message,
                })
                .unwrap()
            {
                if let Output::Send {
                    to: NodeId(1),
                    message,
                } = output
                {
                    self.responses.push_back(Input::Message {
                        from: NodeId(2),
                        message,
                    });
                }
            }
        }
    }
    fn drive(&mut self) -> Vec<LogIndex> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let mut applied = Vec::new();
        while !self.owner.idle() || !self.responses.is_empty() {
            assert!(Instant::now() < deadline);
            match self.owner.poll().unwrap() {
                Some(ServiceEvent::Output(output)) => self.route(output),
                Some(ServiceEvent::Applied(done)) => {
                    applied.extend(done.entries().iter().map(|e| e.last));
                }
                None => {}
                other => panic!("unexpected event: {other:?}"),
            }
            if self.owner.state() == ServiceState::Ready {
                if let Some(input) = self.responses.pop_front() {
                    self.owner.try_step(ServiceInput::from(input)).unwrap();
                }
            }
            std::thread::yield_now();
        }
        applied
    }
    fn delay(&mut self, panic: bool) -> (mpsc::Receiver<()>, Release) {
        let (started, wait) = mpsc::sync_channel(1);
        let (release, resume) = mpsc::sync_channel(1);
        *self.gate.lock().unwrap() = Some((started, resume, panic));
        (wait, Release(Some(release)))
    }
    fn propose(&mut self) {
        self.owner
            .try_step(ServiceInput::Proposals(vec![ClientProposalInput {
                proposal_id: None,
                payload: b"command".to_vec(),
            }]))
            .unwrap();
    }
}

#[test]
fn speculative_replication_overlaps_fsync_but_quorum_and_client_success_stay_fenced() {
    let mut cluster = Cluster::new();
    let (started, mut release) = cluster.delay(false);
    cluster.propose();
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    for _ in 0..100 {
        match cluster.owner.poll().unwrap() {
            Some(ServiceEvent::Output(output @ Output::Send { .. })) => cluster.route(output),
            None => {}
            other => panic!("dependent output escaped: {other:?}"),
        }
    }
    assert!(matches!(
        cluster
            .owner
            .compact_snapshot_with::<InMemoryRaftSnapshotStore>(|_, _| panic!(
                "persistence still owns node"
            )),
        Err(ServiceError::NotReady(ServiceState::Busy))
    ));
    assert_eq!(cluster.follower.last_log_index(), LogIndex(2));
    assert_eq!(cluster.owner.progress().durable, LogIndex(1));
    assert_eq!(cluster.owner.durable_through(), Some(LogIndex::ZERO));
    let input = ServiceInput::from(cluster.responses.front().unwrap().clone());
    assert!(
        matches!(cluster.owner.try_step(input.clone()), Err(ServiceStepError::Unavailable { input: returned, .. }) if returned == input)
    );
    release.now();
    assert_eq!(cluster.drive(), [LogIndex(2)]);
    assert_eq!(cluster.owner.progress().committed, LogIndex(2));
}

#[test]
fn shutdown_during_persistence_drains_owned_work_but_does_not_claim_quorum_success() {
    let mut cluster = Cluster::new();
    let (started, mut release) = cluster.delay(false);
    cluster.propose();
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    cluster.owner.begin_shutdown();
    assert!(matches!(
        cluster.owner.finish_shutdown(),
        Err(ServiceError::NotReady(ServiceState::Draining))
    ));
    release.now();
    let deadline = Instant::now() + Duration::from_secs(5);
    while !cluster.owner.idle() {
        assert!(Instant::now() < deadline);
        assert!(!matches!(
            cluster.owner.poll().unwrap(),
            Some(ServiceEvent::Applied(_))
        ));
        std::thread::yield_now();
    }
    assert_eq!(
        cluster.owner.finish_shutdown().unwrap().floor,
        LogIndex::ZERO
    );
    assert_eq!(cluster.owner.progress().durable, LogIndex(2));
    assert_eq!(cluster.owner.progress().committed, LogIndex(1));
}

#[test]
fn persistence_panic_poison_suppresses_dependent_outputs() {
    let mut cluster = Cluster::new();
    let (started, mut release) = cluster.delay(true);
    cluster.propose();
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    release.now();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        assert!(Instant::now() < deadline);
        match cluster.owner.poll() {
            Err(ServiceError::Persistence(_)) => break,
            Ok(
                Some(ServiceEvent::Output(Output::Send {
                    message: Message::AppendEntries(_),
                    ..
                }))
                | None,
            ) => {}
            other => panic!("dependent output escaped: {other:?}"),
        }
    }
    assert_eq!(cluster.owner.state(), ServiceState::Failed);
    assert!(cluster.owner.poll().is_err());
}
