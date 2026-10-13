//! One managed durable owner, real files, and an authenticated TLS link.
use super::super::{
    application::{adapt, Application, Gate},
    config, link,
};
use super::ConfigurationCommit;
use rafter::{Input, NodeConfig, NodeId, Output};
use rafter_app::state_machine::{ReadBarrier, ReplicatedStateMachine};
use rafter_reference_ledger::{
    store::LedgerStore, DurableLedgerStateMachine, LedgerApplicationEntry, LedgerQuery,
    LedgerQueryResult,
};
use rafter_runtime::{
    service::{ServiceError, ServiceEvent, ServiceOptions, ServicePoll, ServiceState},
    DurableRaftNodeStorage,
};
use rafter_service::{
    durable::{DurableClientOptions, DurableClientOwner, DurableReadLimits, DurableWriteLimits},
    PeerEnvelope, RaftTransport,
};
use rafter_storage::{
    FileRaftHardStateStore, FileRaftLogSegment, FileRaftNodeStores, FileRaftSnapshotStore,
};
use rafter_transport_tls::{EndpointBook, TlsTransportError};
use std::{
    num::NonZeroUsize,
    path::Path,
    time::{Duration, Instant},
};

type Owner = DurableClientOwner<
    FileRaftHardStateStore,
    FileRaftLogSegment,
    FileRaftSnapshotStore,
    LedgerApplicationEntry,
    Application,
    LedgerQueryResult,
>;
pub(super) struct Replica {
    pub(super) owner: Owner,
    pub(super) link: link::Link,
    pub(super) endpoints: EndpointBook,
    pub(super) id: u64,
    pub(super) commits: Vec<ConfigurationCommit>,
}
impl Replica {
    pub(super) fn open(root: &Path, id: u64, fresh: bool, gate: Option<Gate>) -> Self {
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
        options.max_proposals = 1;
        options.persistence = options.persistence.with_storage_telemetry(true);
        options.application = options.application.with_limits(1, 4096, 1, 4096);
        let owner = Owner::recover_with_reads(
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
            DurableClientOptions::new(
                options,
                DurableWriteLimits {
                    writes: NonZeroUsize::new(2).unwrap(),
                    payload_bytes: NonZeroUsize::new(8192).unwrap(),
                },
                DurableReadLimits {
                    reads: NonZeroUsize::new(2).unwrap(),
                    payload_bytes: NonZeroUsize::new(8192).unwrap(),
                },
            ),
            adapt,
            |app, query| {
                assert_eq!(query, [0], "fixture summary query encoding");
                let floor = app.ledger.store().applied_index();
                app.ledger
                    .read(
                        LedgerQuery::GetLedgerSummary,
                        ReadBarrier {
                            required_applied_index: floor,
                            local_applied_index: floor,
                        },
                    )
                    .unwrap()
            },
            || {},
        )
        .unwrap();
        let (link, endpoints) = link::bind(root, id, fresh);
        Self {
            owner,
            link,
            endpoints,
            id,
            commits: Vec::new(),
        }
    }
    pub(super) fn ready(&self) -> bool {
        self.owner.state() == ServiceState::Ready
    }
    pub(super) fn pump(&mut self) {
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
                ServicePoll::Event(ServiceEvent::Output(Output::ConfigurationCommitted {
                    index,
                    term,
                    configuration,
                    ..
                })) => self.commits.push(ConfigurationCommit {
                    node: NodeId(self.id),
                    index,
                    term,
                    configuration: configuration.membership_config(),
                }),
                ServicePoll::Progress
                | ServicePoll::Event(
                    ServiceEvent::Applied(_)
                    | ServiceEvent::Output(
                        Output::LocalProposalAppended { .. } | Output::ReadIndexGranted { .. },
                    ),
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
    pub(super) fn close(mut self) {
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
