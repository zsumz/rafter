//! Managed clients remain fenced while TLS snapshot installation owns the app.
use super::{config, link, poll, Application, Driver, Managed};
use crate::support::{amount, execute, open_session};
use rafter::{Input, LogIndex, NodeId, RaftSnapshot};
use rafter_app::state_machine::{ApplicationSnapshot, ReadBarrier, ReplicatedStateMachine};
use rafter_reference_ledger::{
    Command, LedgerQuery, LedgerQueryResult, LedgerStateMachine, Mutation,
};
use rafter_runtime::service::{ServicePoll, ServiceState, ServiceStepError};
use rafter_service::{
    durable::DurableClientStepError, DriverCommandSender, ReadConsistency, ReadOptions,
};
use std::{
    task::Poll,
    time::{Duration, Instant},
};

pub(super) fn install(driver: &mut Driver, snapshot: RaftSnapshot, mut application: Application) {
    let floor = snapshot.metadata.last_included_index;
    let watch = driver.handle().metrics().unwrap();
    let previous = watch.current().applied_index;
    assert_eq!(driver.state(), ServiceState::InstallingSnapshot);
    assert_eq!(driver.durable_through(), None);
    assert!(previous < floor);
    let mut read = driver.sender().read(
        link::GROUP,
        vec![0],
        ReadConsistency::Local,
        ReadOptions::default().with_min_applied_index(floor),
    );
    assert!(matches!(
        driver.try_step(Input::Tick),
        Err(DurableClientStepError::Service(
            ServiceStepError::Unavailable {
                state: ServiceState::InstallingSnapshot,
                ..
            }
        ))
    ));
    for _ in 0..3 {
        assert!(matches!(driver.poll_next().unwrap(), ServicePoll::Pending));
        assert!(poll(&mut read).is_pending());
        assert_eq!(watch.current().applied_index, previous);
    }
    application
        .ledger
        .install_snapshot(ApplicationSnapshot {
            applied_index: floor,
            payload: Vec::new(),
            raft_snapshot: Some(snapshot),
        })
        .unwrap();
    let expected = application
        .ledger
        .read(
            LedgerQuery::GetLedgerSummary,
            ReadBarrier {
                required_applied_index: floor,
                local_applied_index: floor,
            },
        )
        .unwrap();
    driver.resume_snapshot(application).unwrap();
    assert_eq!(watch.current().applied_index, floor);
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Poll::Ready(result) = poll(&mut read) {
            let receipt = result.unwrap();
            assert!(receipt.proof.is_none());
            assert_eq!(receipt.result, expected);
            break;
        }
        assert!(matches!(
            driver.poll_next().unwrap(),
            ServicePoll::Pending | ServicePoll::Progress
        ));
        assert!(Instant::now() < deadline, "installed query did not resume");
        std::thread::yield_now();
    }
}

fn write(cluster: &mut Managed, leader: usize, command: &Command) -> LogIndex {
    let payload = LedgerStateMachine::new(config())
        .encode_command(command)
        .unwrap();
    let handle = cluster.nodes[leader].0.handle();
    cluster
        .complete(async move { handle.write(payload).await })
        .unwrap()
        .index
}

fn check_summary(cluster: &mut Managed, leader: usize, floor: LogIndex) {
    let handle = cluster.nodes[leader].0.handle();
    let receipt = cluster
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
    let proof = receipt.proof.unwrap();
    assert_eq!(proof.group_id, link::GROUP);
    assert!(proof.local_applied_index >= floor && proof.required_applied_index >= floor);
    let LedgerQueryResult::Summary(summary) = receipt.result else {
        panic!("missing summary");
    };
    assert_eq!(summary.open_accounts, 4097);
    assert_eq!(summary.total_balance, 14);
}

#[test]
fn managed_snapshot_handoff_fences_queries_and_restores_retry_after_tls_catchup() {
    let scratch = crate::scratch::ScratchDir::new("managed-snapshot-tls");
    crate::owner_tls::seed_genesis(scratch.path());
    let mut cluster = Managed::open(scratch.path(), true, 0);
    write(&mut cluster, 0, &open_session(0, 2));
    let floor = write(
        &mut cluster,
        0,
        &execute(
            0,
            2,
            1,
            Mutation::Deposit {
                account_id: crate::owner_tls::ACCOUNT,
                amount: amount(9),
            },
        ),
    );
    cluster.converge(0, floor);
    cluster.stop_last();
    let deposit = execute(
        0,
        2,
        2,
        Mutation::Deposit {
            account_id: crate::owner_tls::ACCOUNT,
            amount: amount(5),
        },
    );
    let floor = write(&mut cluster, 0, &deposit);
    assert_eq!(floor, LogIndex(4));
    cluster.converge(0, floor);
    cluster.compact_all(floor);
    assert!(
        cluster.nodes[0]
            .0
            .ready_node()
            .unwrap()
            .snapshot()
            .unwrap()
            .application_payload_len
            > 64 * 1024
    );
    // Reopen all links so no pre-compaction append can bypass snapshot catch-up.
    cluster.close();
    let mut cluster = Managed::open(scratch.path(), false, 0);
    cluster.converge(0, floor);
    assert_eq!(cluster.installs, [(NodeId(3), floor)]);
    assert!(cluster.chunks.contains(&(NodeId(3), 0)));
    assert!(cluster
        .chunks
        .iter()
        .any(|(id, offset)| *id == NodeId(3) && *offset >= 64 * 1024));
    assert_eq!(
        cluster.nodes[2].0.ready_node().unwrap().snapshot_index(),
        floor
    );
    check_summary(&mut cluster, 0, floor);
    cluster.close();

    let mut cluster = Managed::open(scratch.path(), false, 2);
    check_summary(&mut cluster, 2, floor);
    let retry = write(&mut cluster, 2, &deposit);
    check_summary(&mut cluster, 2, retry);
    cluster.close();
}
