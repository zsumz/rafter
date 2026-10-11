//! Existing `RaftHandle` exercised over the durable owner and authenticated TLS.
use super::super::{application::Application, config, link};
use super::{cluster::Cluster, poll};
use rafter::{Input, LogIndex, NodeId, Role};
use rafter_app::state_machine::ReplicatedStateMachine;
use rafter_reference_ledger::{
    LedgerApplicationEntry, LedgerQueryResult, LedgerStateMachine, Mutation,
};
use rafter_runtime::service::{ServicePoll, ServiceState};
use rafter_service::{
    durable::DurableRaftDriver, DriverCommandSender, ReadConsistency, ReadOptions,
};
use rafter_storage::{FileRaftHardStateStore, FileRaftLogSegment, FileRaftSnapshotStore};
use std::{
    path::Path,
    task::Poll,
    time::{Duration, Instant},
};

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
struct Managed(Vec<(Driver, link::Link)>);
impl Managed {
    fn open(root: &Path, fresh: bool, leader: usize) -> Self {
        let mut cluster = Cluster::open(root, fresh, None);
        cluster.elect(leader);
        Self(
            cluster
                .0
                .into_iter()
                .map(|n| {
                    (
                        Driver::attach(link::GROUP, n.owner, n.link.sender()).unwrap(),
                        n.link,
                    )
                })
                .collect(),
        )
    }
    fn until(&mut self, mut done: impl FnMut(&Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(15);
        while !done(self) {
            for (driver, link) in &mut self.0 {
                for _ in 0..64 {
                    if matches!(driver.poll_next().unwrap(), ServicePoll::Pending) {
                        break;
                    }
                }
                if driver.state() == ServiceState::Ready {
                    for frame in link.inbound().drain(1).unwrap() {
                        assert_eq!(frame.group_id, link::GROUP);
                        assert_eq!(frame.raft_to, driver.ready_node().unwrap().id());
                        assert_eq!(frame.authenticated_peer, link::peer(frame.raft_from.0));
                        driver
                            .try_step(Input::Message {
                                from: frame.raft_from,
                                message: frame.message,
                            })
                            .unwrap();
                    }
                }
            }
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
    fn close(mut self) {
        let mut stops: Vec<_> = self
            .0
            .iter()
            .map(|(d, _)| d.sender().shutdown(link::GROUP))
            .collect();
        self.until(|c| c.0.iter().all(|(d, _)| d.state() == ServiceState::Stopped));
        for stop in &mut stops {
            assert!(matches!(poll(stop), Poll::Ready(Ok(()))));
        }
        for (_, link) in self.0 {
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
    let handle = cluster.0[0].0.handle();
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
        c.0.iter()
            .all(|(d, _)| d.durable_through().is_some_and(|i| i >= floor))
    });
    cluster
        .complete(async move { handle.transfer_leadership(NodeId(2)).await })
        .unwrap();
    cluster.until(|c| {
        c.0[1]
            .0
            .ready_node()
            .is_some_and(|n| n.role() == Role::Leader)
    });
    cluster.close();

    let mut cluster = Managed::open(scratch.path(), false, 1);
    let handle = cluster.0[1].0.handle();
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
