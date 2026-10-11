//! Managed client adoption through only public package APIs and real TLS.
use super::{
    application::{adapt, Application, Gate},
    config, link,
};
use rafter::{Input, LogIndex, NodeConfig, NodeId, Output, Role};
use rafter_app::state_machine::ReplicatedStateMachine;
use rafter_reference_ledger::{
    store::LedgerStore, ApplyDisposition, DurableLedgerStateMachine, LedgerApplicationEntry,
    LedgerStateMachine,
};
use rafter_runtime::{
    service::{ServiceError, ServiceEvent, ServiceOptions, ServicePoll, ServiceState},
    DurableRaftNodeStorage,
};
use rafter_service::{
    durable::{DurableClientOwner, DurableWriteLimits},
    DriverFuture, PeerEnvelope, RaftTransport, WriteError, WriteOptions,
};
use rafter_storage::{
    FileRaftHardStateStore, FileRaftLogSegment, FileRaftNodeStores, FileRaftSnapshotStore,
};
use rafter_transport_tls::{EndpointBook, TlsTransportError};
use std::{
    num::NonZeroUsize,
    path::Path,
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

type Owner = DurableClientOwner<
    FileRaftHardStateStore,
    FileRaftLogSegment,
    FileRaftSnapshotStore,
    LedgerApplicationEntry,
    Application,
>;
struct Replica {
    owner: Owner,
    link: link::Link,
    endpoints: EndpointBook,
    id: u64,
}
impl Replica {
    fn open(root: &Path, id: u64, fresh: bool, gate: Option<Gate>) -> Self {
        std::fs::create_dir_all(root.join("raft")).unwrap();
        std::fs::create_dir_all(root.join("application")).unwrap();
        let (hard_state_store, log_segment, snapshot_store) =
            FileRaftNodeStores::open(root.join("raft"))
                .unwrap()
                .into_parts();
        let application = Application {
            ledger: DurableLedgerStateMachine::new(
                LedgerStore::open(&root.join("application"), config()).unwrap(),
                root.join("raft/snapshots"),
            ),
            gate,
        };
        let mut options = ServiceOptions::new();
        options.application = options.application.with_limits(1, 4096, 1, 4096);
        let owner = Owner::recover(
            NodeConfig::new(
                NodeId(id),
                (1..=3).filter(|n| *n != id).map(NodeId).collect(),
                10,
            )
            .unwrap(),
            DurableRaftNodeStorage {
                hard_state_store,
                log_segment,
                snapshot_store,
            },
            application,
            options,
            DurableWriteLimits {
                writes: NonZeroUsize::new(2).unwrap(),
                payload_bytes: NonZeroUsize::new(8192).unwrap(),
            },
            adapt,
            || {},
        )
        .unwrap();
        let (link, endpoints) = link::bind(root, id, fresh);
        Self {
            owner,
            link,
            endpoints,
            id,
        }
    }
    fn ready(&self) -> bool {
        self.owner.state() == ServiceState::Ready
    }
    fn pump(&mut self) {
        for _ in 0..64 {
            match self.owner.poll_next().unwrap() {
                ServicePoll::Event(ServiceEvent::Output(Output::Send { to, message })) => {
                    match self.link.sender().send(PeerEnvelope {
                        group_id: link::GROUP,
                        from: NodeId(self.id),
                        to,
                        message,
                    }) {
                        Ok(()) | Err(TlsTransportError::QueueFull { .. }) => {}
                        other => panic!("transport admission: {other:?}"),
                    }
                }
                ServicePoll::Progress
                | ServicePoll::Event(
                    ServiceEvent::Applied(_)
                    | ServiceEvent::Output(Output::LocalProposalAppended { .. }),
                ) => {}
                ServicePoll::Pending => break,
                other => panic!("unexpected managed event: {other:?}"),
            }
        }
        if self.ready() {
            for frame in self.link.inbound().drain(1).unwrap() {
                assert_eq!(frame.group_id, link::GROUP);
                assert_eq!(frame.raft_to, NodeId(self.id));
                assert_eq!(frame.authenticated_peer, link::peer(frame.raft_from.0));
                self.owner
                    .try_step(Input::Message {
                        from: frame.raft_from,
                        message: frame.message,
                    })
                    .unwrap();
            }
        }
    }
    fn close(mut self) {
        let diagnostics = self.link.diagnostics();
        assert!(
            diagnostics.tls_handshakes > 0
                && diagnostics.frames_received > 0
                && diagnostics.frames_sent > 0
        );
        assert_eq!(diagnostics.identity_mismatches, 0);
        assert_eq!(diagnostics.unknown_certificates, 0);
        assert_eq!(diagnostics.session_store_failures, 0);
        self.owner.begin_shutdown();
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            self.pump();
            match self.owner.finish_shutdown() {
                Ok(app) => {
                    drop(app);
                    break;
                }
                Err(ServiceError::NotReady(_)) => {}
                other => panic!("shutdown: {other:?}"),
            }
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }
        self.link.join().unwrap();
    }
}
struct Cluster(Vec<Replica>);
impl Cluster {
    fn open(root: &Path, fresh: bool, mut gate: Option<Gate>) -> Self {
        let nodes: Vec<_> = (1..=3)
            .map(|id| Replica::open(&root.join(format!("node-{id}")), id, fresh, gate.take()))
            .collect();
        for node in &nodes {
            for other in &nodes {
                if node.id != other.id {
                    link::endpoint(&node.endpoints, other.id, other.link.local_addr());
                }
            }
            node.link.start().unwrap();
        }
        let mut cluster = Self(nodes);
        cluster.until(|c| c.0.iter().all(Replica::ready));
        cluster
    }
    fn until(&mut self, mut done: impl FnMut(&Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while !done(self) {
            for node in &mut self.0 {
                node.pump();
            }
            assert!(Instant::now() < deadline, "managed TLS cluster stalled");
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    fn tick(&mut self, leader: usize) {
        self.until(|c| c.0[leader].ready());
        self.0[leader].owner.try_step(Input::Tick).unwrap();
        self.until(|c| c.0[leader].ready());
    }
    fn elect(&mut self, leader: usize) {
        for _ in 0..20 {
            if self.0[leader].owner.ready_node().unwrap().role() == Role::Leader {
                break;
            }
            self.tick(leader);
        }
        self.until(|c| {
            c.0[leader].owner.ready_node().is_some_and(|n| {
                n.role() == Role::Leader
                    && n.term_at_index(n.commit_index()) == Some(n.current_term())
            })
        });
    }
    fn converge(&mut self, leader: usize, floor: LogIndex) {
        for _ in 0..10 {
            self.tick(leader);
        }
        self.until(|c| {
            c.0.iter()
                .all(|n| n.ready() && n.owner.durable_through() == Some(floor))
        });
    }
    fn close(self) {
        for node in self.0 {
            node.close();
        }
    }
}
fn poll<T>(future: &mut DriverFuture<T>) -> Poll<T> {
    future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
}

#[test]
fn managed_write_waits_for_tls_quorum_and_application_durability_then_reopens() {
    let scratch = crate::scratch::ScratchDir::new("managed-owner-tls");
    let (pause, entered, release) = super::gate();
    let mut cluster = Cluster::open(scratch.path(), true, Some(pause));
    let release = release; // Unblock application before joining on assertion unwind.
    cluster.elect(0);
    let sender = cluster.0[0].owner.sender();
    let payload = LedgerStateMachine::new(config())
        .encode_command(&crate::support::open_session(0, 1))
        .unwrap();
    let mut write = sender.write(payload.clone(), WriteOptions::default());
    // Withhold follower owner turns: TLS alone cannot acknowledge a Raft append.
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        cluster.0[0].pump();
        if cluster.0[0]
            .owner
            .ready_node()
            .is_some_and(|n| n.last_log_index() == LogIndex(2))
        {
            break;
        }
        assert!(Instant::now() < deadline);
        std::thread::yield_now();
    }
    assert_eq!(
        cluster.0[0].owner.ready_node().unwrap().commit_index(),
        LogIndex(1)
    );
    assert!(poll(&mut write).is_pending());
    assert!(entered.try_recv().is_err());
    cluster.until(|_| entered.try_recv().is_ok());
    assert!(poll(&mut write).is_pending());
    assert_eq!(cluster.0[0].owner.durable_through(), Some(LogIndex::ZERO));
    let canceled = sender.write(payload.clone(), WriteOptions::default());
    assert_eq!(sender.retained().writes, 2);
    drop(canceled);
    cluster.until(|_| sender.retained().writes == 1);
    release.now();
    let mut receipt = None;
    cluster.until(|_| {
        if let Poll::Ready(result) = poll(&mut write) {
            receipt = Some(result.unwrap());
            true
        } else {
            false
        }
    });
    let receipt = receipt.unwrap();
    assert_eq!(receipt.index, LogIndex(2));
    assert_eq!(receipt.result.index, receipt.index);
    assert_eq!(receipt.result.term, receipt.term);
    assert_eq!(
        receipt.result.result.disposition,
        ApplyDisposition::SessionOpened
    );
    assert_eq!(sender.retained().writes, 0);
    cluster.converge(0, receipt.index);
    cluster.close();
    assert!(matches!(
        poll(&mut sender.write(payload.clone(), WriteOptions::default())),
        Poll::Ready(Err(WriteError::ShuttingDown))
    ));

    let mut cluster = Cluster::open(scratch.path(), false, None);
    assert!(cluster
        .0
        .iter()
        .all(|n| n.owner.durable_through() == Some(LogIndex(2))));
    cluster.elect(1);
    let mut retry = cluster.0[1]
        .owner
        .sender()
        .write(payload, WriteOptions::default());
    let mut receipt = None;
    cluster.until(|_| {
        if let Poll::Ready(result) = poll(&mut retry) {
            receipt = Some(result.unwrap());
            true
        } else {
            false
        }
    });
    let receipt = receipt.unwrap();
    assert_eq!(
        receipt.result.result.disposition,
        ApplyDisposition::SessionAlreadyOpen
    );
    cluster.converge(1, receipt.index);
    cluster.close();
}
