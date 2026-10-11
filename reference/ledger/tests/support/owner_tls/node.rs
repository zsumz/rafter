//! One bounded owner turn: persist/apply first, then authenticated peer input.
use super::{
    application::{adapt, Application, Gate},
    link::{self, Link},
};
use crate::support::config;
use rafter::{Input, LocalProposalId, LogIndex, Message, NodeConfig, NodeId, Output, ReadId};
use rafter_app::state_machine::{ReadBarrier, ReplicatedStateMachine};
use rafter_reference_ledger::{
    store::LedgerStore, DurableLedgerStateMachine, LedgerApplicationEntry, LedgerQuery,
    LedgerQueryResult,
};
use rafter_runtime::{
    application::ApplicationReadRejection,
    service::{
        DurableService, ServiceError, ServiceEvent, ServiceOptions, ServicePoll,
        ServiceReadRejection, ServiceState,
    },
    DurableRaftNodeStorage,
};
use rafter_service::{PeerEnvelope, RaftTransport};
use rafter_storage::{
    FileRaftHardStateStore, FileRaftLogSegment, FileRaftNodeStores, FileRaftSnapshotStore,
};
use rafter_transport_tls::{EndpointBook, TlsTransportError, TransportHealth};
use std::{
    path::Path,
    time::{Duration, Instant},
};

type Owner = DurableService<
    FileRaftHardStateStore,
    FileRaftLogSegment,
    FileRaftSnapshotStore,
    LedgerApplicationEntry,
    Application,
>;
#[derive(Debug)]
pub struct Replica {
    pub owner: Owner,
    pub link: Link,
    pub endpoints: EndpointBook,
    pub id: u64,
    // The fixture admits at most one read and eight writes per replica lifetime.
    pub writes: Vec<LocalProposalId>,
    pub read: Option<(ReadId, LedgerQueryResult)>,
    pending_read: Option<(ReadId, LogIndex)>,
    pub read_round: Option<u64>,
    pub read_responses: usize,
    pub blocked_read_polls: usize,
}
impl Replica {
    pub fn open(root: &Path, id: u64, fresh: bool, gate: Option<Gate>) -> Self {
        std::fs::create_dir_all(root.join("raft")).unwrap();
        std::fs::create_dir_all(root.join("application")).unwrap();
        let (hard_state_store, log_segment, snapshot_store) =
            FileRaftNodeStores::open(root.join("raft"))
                .unwrap()
                .into_parts();
        let application = Application {
            ledger: DurableLedgerStateMachine::new(
                LedgerStore::open(&root.join("application"), config(1, 2)).unwrap(),
                root.join("raft/snapshots"),
            ),
            gate,
        };
        let mut options = ServiceOptions::new();
        options.application = options.application.with_limits(1, 4096, 1, 4096);
        let owner = Owner::recover(
            NodeConfig::new(
                NodeId(id),
                (1..=3).filter(|node| *node != id).map(NodeId).collect(),
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
            adapt,
        )
        .unwrap();
        let (link, endpoints) = link::bind(root, id, fresh);
        Self {
            owner,
            link,
            endpoints,
            id,
            writes: Vec::new(),
            read: None,
            pending_read: None,
            read_round: None,
            read_responses: 0,
            blocked_read_polls: 0,
        }
    }
    pub fn ready(&self) -> bool {
        self.owner.state() == ServiceState::Ready
    }
    pub fn input(&mut self, input: Input) {
        assert!(self.ready());
        self.owner.try_step(input.into()).unwrap();
    }
    pub fn request_read(&mut self, id: ReadId) {
        assert!(self.pending_read.is_none());
        self.read = None;
        self.read_round = Some(0);
        self.read_responses = 0;
        self.blocked_read_polls = 0;
        self.input(Input::ReadIndex { read_id: id });
    }
    pub fn pump(&mut self) {
        for _ in 0..64 {
            match self.owner.poll_next().unwrap() {
                ServicePoll::Event(ServiceEvent::Output(Output::Send { to, message })) => {
                    if let Message::AppendEntries(append) = &message {
                        if self.read_round == Some(0) {
                            self.read_round = Some(append.sequence);
                        }
                    }
                    match self.link.sender().send(PeerEnvelope {
                        group_id: link::GROUP,
                        from: NodeId(self.id),
                        to,
                        message,
                    }) {
                        Ok(()) | Err(TlsTransportError::QueueFull { .. }) => {}
                        other => panic!("unexpected transport admission: {other:?}"),
                    }
                }
                ServicePoll::Event(ServiceEvent::Output(Output::LocalProposalAppended {
                    ..
                }))
                | ServicePoll::Progress => {}
                ServicePoll::Event(ServiceEvent::Output(Output::ReadIndexGranted {
                    read_id,
                    read_index,
                })) => {
                    assert!(self.pending_read.is_none());
                    let required = self
                        .owner
                        .ready_node()
                        .unwrap()
                        .committed_application_index_through(read_index);
                    assert!(self.owner.durable_through().unwrap() >= required);
                    self.pending_read = Some((read_id, required));
                }
                ServicePoll::Event(ServiceEvent::Applied(done)) => {
                    for outcome in done.outcomes() {
                        assert!(self.owner.durable_through().unwrap() >= outcome.index);
                        if let Some(id) = outcome.local_proposal_id {
                            assert!(self.writes.len() < 8);
                            assert!(!self.writes.contains(&id));
                            self.writes.push(id);
                        }
                    }
                }
                ServicePoll::Pending => {
                    if self.read_responses > 0 && self.owner.state() == ServiceState::Busy {
                        self.blocked_read_polls += 1;
                    }
                    break;
                }
                other => panic!("unexpected owner event: {other:?}"),
            }
        }
        if self.ready() {
            self.finish_read();
            // Do not remove frames while the owner cannot accept them. The TLS
            // inbox is bounded; no unbounded intermediate VecDeque is needed.
            for frame in self.link.inbound().drain(1).unwrap() {
                assert_eq!(frame.group_id, link::GROUP);
                assert_eq!(frame.raft_to, NodeId(self.id));
                assert_eq!(frame.authenticated_peer, link::peer(frame.raft_from.0));
                let read_response = match &frame.message {
                    Message::AppendEntriesResponse(response) => {
                        response.success
                            && self
                                .read_round
                                .is_some_and(|round| round != 0 && response.sequence >= round)
                    }
                    _ => false,
                };
                self.input(Input::Message {
                    from: frame.raft_from,
                    message: frame.message,
                });
                if read_response {
                    self.read_responses += 1;
                }
            }
        }
        let depth = self.link.queue_depths().unwrap();
        assert!(depth.outbound_frames <= 32 && depth.outbound_bytes <= 128 * 1024);
        assert!(depth.inbound_frames <= 32 && depth.inbound_bytes <= 128 * 1024);
        assert!(depth.inbound_memory_bytes <= 8 * 1024 * 1024);
        assert_ne!(self.link.health(), TransportHealth::Failed);
    }
    fn finish_read(&mut self) {
        if let Some((read_id, required)) = self.pending_read {
            match self.owner.try_read_application(required, |app| {
                app.ledger.read(
                    LedgerQuery::GetAccount {
                        account_id: super::ACCOUNT,
                    },
                    ReadBarrier {
                        required_applied_index: required,
                        local_applied_index: app.ledger.store().applied_index(),
                    },
                )
            }) {
                Ok(read) => {
                    assert!(read.durable_through() >= required);
                    self.read = Some((read_id, read.into_output().unwrap()));
                    self.pending_read = None;
                }
                Err(error) => assert_eq!(
                    error.rejection(),
                    ServiceReadRejection::Application(ApplicationReadRejection::Busy)
                ),
            }
        }
    }
    pub fn close(mut self) {
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
                other => panic!("unexpected shutdown: {other:?}"),
            }
            assert!(Instant::now() < deadline);
            std::thread::sleep(Duration::from_millis(1));
        }
        self.link.join().unwrap();
    }
}
