//! Existing `RaftHandle` exercised over the durable owner and authenticated TLS.
use super::super::{application::Application, config, link};
use super::{cluster::Cluster, poll, ConfigurationCommit};
use rafter::{Input, LogIndex, Message, NodeId, Output, Role};
use rafter_app::state_machine::ReplicatedStateMachine;
use rafter_reference_ledger::{
    LedgerApplicationEntry, LedgerQueryResult, LedgerStateMachine, Mutation,
};
use rafter_runtime::service::{ServiceEvent, ServicePoll, ServiceState};
use rafter_service::{
    durable::{DurableMembershipLimits, DurableRaftDriver},
    DriverCommandSender, ReadConsistency, ReadError, ReadErrorKind, ReadOptions, WriteError,
    WriteErrorKind, WriteFate, WriteOptions,
};
use rafter_storage::{FileRaftHardStateStore, FileRaftLogSegment, FileRaftSnapshotStore};
use std::{
    num::NonZeroUsize,
    path::Path,
    task::Poll,
    time::{Duration, Instant},
};
mod compaction;
mod membership;
mod mixed_load;
mod snapshot;

type Driver = DurableRaftDriver<
    u64,
    FileRaftHardStateStore,
    FileRaftLogSegment,
    FileRaftSnapshotStore,
    LedgerApplicationEntry,
    Application,
    rafter_transport_tls::TlsSender<u64, link::Codec>,
    LedgerQueryResult,
>;
#[derive(Default)]
struct Managed {
    nodes: Vec<(Driver, link::Link)>,
    installs: Vec<(NodeId, LogIndex)>,
    chunks: Vec<(NodeId, u64)>,
    commits: Vec<ConfigurationCommit>,
}
impl Managed {
    fn open(root: &Path, fresh: bool, leader: usize) -> Self {
        let cluster = Cluster::open(root, fresh, None);
        let mut commits = Vec::new();
        let mut managed = Self {
            nodes: cluster
                .0
                .into_iter()
                .map(|n| {
                    commits.extend(n.commits);
                    (
                        Driver::attach_with_membership(
                            link::GROUP,
                            n.owner,
                            n.link.sender(),
                            DurableMembershipLimits::new(NonZeroUsize::new(64).unwrap()),
                        )
                        .unwrap(),
                        n.link,
                    )
                })
                .collect(),
            commits,
            ..Self::default()
        };
        // Route every election output through the managed driver, including
        // snapshot chunks sent to a replica behind the compacted majority.
        managed.elect(leader);
        managed
    }
    fn elect(&mut self, leader: usize) {
        for _ in 0..20 {
            self.until(|c| c.nodes[leader].0.state() == ServiceState::Ready);
            if self.nodes[leader].0.ready_node().unwrap().role() == Role::Leader {
                break;
            }
            self.nodes[leader].0.try_step(Input::Tick).unwrap();
        }
        self.until(|c| {
            c.nodes[leader].0.ready_node().is_some_and(|node| {
                node.role() == Role::Leader
                    && node.term_at_index(node.commit_index()) == Some(node.current_term())
            })
        });
    }
    fn converge(&mut self, leader: usize, floor: LogIndex) {
        for _ in 0..10 {
            self.until(|c| c.nodes[leader].0.state() == ServiceState::Ready);
            self.nodes[leader].0.try_step(Input::Tick).unwrap();
        }
        self.until(|c| {
            c.nodes.iter().all(|(d, _)| {
                d.state() == ServiceState::Ready && d.durable_through() == Some(floor)
            })
        });
    }
    fn stop_last(&mut self) {
        assert_eq!(self.nodes.len(), 3);
        let node = self.nodes.pop().unwrap();
        Self {
            nodes: vec![node],
            ..Self::default()
        }
        .close();
    }
    fn turn(&mut self, polls: usize) {
        for (driver, link) in &mut self.nodes {
            for _ in 0..polls {
                match driver.poll_next().unwrap() {
                    ServicePoll::Pending => break,
                    ServicePoll::Event(ServiceEvent::InstallSnapshot {
                        snapshot,
                        application,
                    }) => {
                        let id = driver.handle().metrics().unwrap().current().node_id;
                        let floor = snapshot.metadata.last_included_index;
                        snapshot::install(driver, snapshot, application);
                        self.installs.push((id, floor));
                    }
                    ServicePoll::Event(ServiceEvent::Output(Output::ConfigurationCommitted {
                        index,
                        term,
                        configuration,
                        ..
                    })) => self.commits.push(ConfigurationCommit {
                        node: driver.handle().metrics().unwrap().current().node_id,
                        index,
                        term,
                        configuration: configuration.membership_config(),
                    }),
                    ServicePoll::Event(ServiceEvent::ApplicationFailed(failure)) => {
                        panic!("managed application failed: {failure:?}")
                    }
                    _ => {}
                }
            }
            if driver.state() == ServiceState::Ready {
                for frame in link.inbound().drain(1).unwrap() {
                    assert_eq!(frame.group_id, link::GROUP);
                    assert_eq!(frame.raft_to, driver.ready_node().unwrap().id());
                    assert_eq!(frame.authenticated_peer, link::peer(frame.raft_from.0));
                    if let Message::InstallSnapshotChunk(chunk) = &frame.message {
                        assert!(chunk.chunk.len() <= 64 * 1024);
                        self.chunks.push((frame.raft_to, chunk.offset));
                    }
                    driver
                        .try_step(Input::Message {
                            from: frame.raft_from,
                            message: frame.message,
                        })
                        .unwrap();
                }
            }
        }
    }
    fn until(&mut self, mut done: impl FnMut(&Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while !done(self) {
            self.turn(64);
            assert!(
                Instant::now() < deadline,
                "managed handle TLS cluster stalled"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    fn complete<T>(&mut self, future: impl std::future::Future<Output = T> + Send + 'static) -> T {
        let mut future: rafter_service::DriverFuture<T> = Box::pin(future);
        let mut outcome = None;
        self.until(|_| {
            if let Poll::Ready(value) = poll(&mut future) {
                outcome = Some(value);
                true
            } else {
                false
            }
        });
        outcome.unwrap()
    }
    fn check_admission(&mut self) {
        let driver = &self.nodes[0].0;
        let before = driver.ready_node().unwrap().commit_index();
        let sender = driver.sender();
        let payload = LedgerStateMachine::new(config())
            .encode_command(&crate::support::open_session(0, 1))
            .unwrap();
        let writes: Vec<_> = (0..2)
            .map(|_| sender.write(link::GROUP, payload.clone(), WriteOptions::default()))
            .collect();
        let reads: Vec<_> = (0..2)
            .map(|_| {
                sender.read(
                    link::GROUP,
                    vec![0],
                    ReadConsistency::Local,
                    ReadOptions::default(),
                )
            })
            .collect();
        let retained = sender.retained();
        assert_eq!(retained.writes.writes, 2);
        assert_eq!(retained.writes.payload_bytes, 2 * payload.len());
        assert_eq!(retained.reads.unwrap().reads, 2);
        assert_eq!(retained.controls, 0);
        let Poll::Ready(Err(write)) =
            poll(&mut sender.write(link::GROUP, payload, WriteOptions::default()))
        else {
            panic!("full write budget did not refuse");
        };
        assert!(matches!(write, WriteError::Backpressure));
        assert_eq!(write.kind(), WriteErrorKind::Backpressure);
        assert_eq!(write.fate(), WriteFate::NotAppended);
        let Poll::Ready(Err(read)) = poll(&mut sender.read(
            link::GROUP,
            vec![0],
            ReadConsistency::Linearizable,
            ReadOptions::default(),
        )) else {
            panic!("full read budget did not refuse");
        };
        assert!(matches!(read, ReadError::Backpressure));
        assert_eq!(read.kind(), ReadErrorKind::Backpressure);
        // None reached the backend: cancellation releases queued work on the
        // next owner turn, after which the same incarnation serves real work.
        drop((writes, reads));
        self.nodes[0].0.poll_next().unwrap();
        self.until(|c| {
            let observed = c.nodes[0].0.handle().metrics().unwrap().current();
            c.nodes[0].0.state() == ServiceState::Ready
                && observed.pending_proposals == 0
                && observed.pending_query_reads == 0
        });
        assert_eq!(self.nodes[0].0.ready_node().unwrap().commit_index(), before);
        let retained = sender.retained();
        assert_eq!(retained.writes.writes, 0);
        assert_eq!(retained.writes.payload_bytes, 0);
        assert_eq!(retained.reads.unwrap().reads, 0);
        for consistency in [ReadConsistency::Local, ReadConsistency::Linearizable] {
            let mut oversized = Vec::with_capacity(8193);
            oversized.push(0);
            assert!(matches!(
                poll(&mut sender.read(link::GROUP, oversized, consistency, ReadOptions::default())),
                Poll::Ready(Err(ReadError::Backpressure))
            ));
        }
        let mut oversized = Vec::with_capacity(8193);
        oversized.push(0);
        assert!(matches!(
            poll(&mut sender.write(link::GROUP, oversized, WriteOptions::default())),
            Poll::Ready(Err(WriteError::Backpressure))
        ));
    }

    fn close(mut self) {
        let mut stops: Vec<_> = self
            .nodes
            .iter()
            .map(|(d, _)| d.sender().shutdown(link::GROUP))
            .collect();
        self.until(|c| {
            c.nodes
                .iter()
                .all(|(d, _)| d.state() == ServiceState::Stopped)
        });
        for stop in &mut stops {
            assert!(matches!(poll(stop), Poll::Ready(Ok(()))));
        }
        for (_, link) in self.nodes {
            let diagnostics = link.diagnostics();
            assert!(
                diagnostics.tls_handshakes > 0
                    && diagnostics.frames_sent > 0
                    && diagnostics.frames_received > 0
            );
            assert_eq!(diagnostics.identity_mismatches, 0);
            assert_eq!(diagnostics.unknown_certificates, 0);
            link.join().unwrap();
        }
    }
}

#[test]
fn durable_raft_handle_routes_tls_writes_reads_transfer_and_shutdown_across_reopen() {
    let scratch = crate::scratch::ScratchDir::new("durable-handle-tls");
    let mut cluster = Managed::open(scratch.path(), true, 0);
    cluster.check_admission();
    let handle = cluster.nodes[0].0.handle();
    let mut floor = LogIndex::ZERO;
    let commands = [
        crate::support::open_session(0, 1),
        crate::support::execute(
            0,
            1,
            1,
            Mutation::OpenAccount {
                account_id: super::super::ACCOUNT,
            },
        ),
        crate::support::execute(
            0,
            1,
            2,
            Mutation::Deposit {
                account_id: super::super::ACCOUNT,
                amount: crate::support::amount(61),
            },
        ),
    ];
    for command in commands {
        let payload = LedgerStateMachine::new(config())
            .encode_command(&command)
            .unwrap();
        let handle = handle.clone();
        floor = cluster
            .complete(async move { handle.write(payload).await })
            .unwrap()
            .index;
    }
    let query_handle = handle.clone();
    let queried = cluster
        .complete(async move {
            query_handle
                .read_with_options(
                    vec![0],
                    ReadConsistency::Linearizable,
                    ReadOptions::default().with_min_applied_index(floor),
                )
                .await
        })
        .unwrap();
    let proof = queried.proof.unwrap();
    assert_eq!(proof.group_id, link::GROUP);
    assert!(proof.required_applied_index >= floor && proof.local_applied_index >= floor);
    let LedgerQueryResult::Summary(summary) = queried.result else {
        panic!("expected summary");
    };
    assert_eq!(summary.open_accounts, 1);
    assert_eq!(summary.total_balance, 61);
    assert_eq!(handle.metrics().unwrap().current().group_id, link::GROUP);
    cluster.until(|c| {
        c.nodes
            .iter()
            .all(|(d, _)| d.durable_through().is_some_and(|i| i >= floor))
    });
    cluster.compact_all(floor);
    cluster
        .complete(async move { handle.transfer_leadership(NodeId(2)).await })
        .unwrap();
    cluster.until(|c| {
        c.nodes[1]
            .0
            .ready_node()
            .is_some_and(|n| n.role() == Role::Leader)
    });
    cluster.close();

    let mut cluster = Managed::open(scratch.path(), false, 1);
    let handle = cluster.nodes[1].0.handle();
    let queried = cluster
        .complete(async move {
            handle
                .read_with_options(
                    vec![0],
                    ReadConsistency::Linearizable,
                    ReadOptions::default().with_min_applied_index(floor),
                )
                .await
        })
        .unwrap();
    assert_eq!(queried.result, LedgerQueryResult::Summary(summary));
    assert_eq!(queried.proof.unwrap().group_id, link::GROUP);
    cluster.close();
}
