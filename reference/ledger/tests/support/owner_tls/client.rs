//! Managed client adoption through only public package APIs and real TLS.
mod cluster;
mod configuration;
mod managed;
mod replica;
use super::config;
use cluster::Cluster;
use configuration::ConfigurationCommit;
use rafter::LogIndex;
use rafter_app::{read::ReadConsistency, state_machine::ReplicatedStateMachine};
use rafter_reference_ledger::{ApplyDisposition, LedgerQueryResult, LedgerStateMachine, Mutation};
use rafter_service::{DriverFuture, ReadOptions, WriteError, WriteOptions};
use std::{
    task::{Context, Poll, Waker},
    time::{Duration, Instant},
};

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
    let stalled = cluster.0[0].owner.metrics().unwrap();
    assert_eq!(stalled.commit_index, LogIndex(2));
    assert_eq!(stalled.applied_index, LogIndex::ZERO);
    assert_eq!(stalled.pending_proposals, 1);
    let reader = cluster.0[0].owner.read_sender().unwrap();
    let mut read = reader.read(
        vec![0],
        ReadConsistency::Linearizable,
        ReadOptions::default(),
    );
    cluster.0[0].pump();
    assert!(poll(&mut read).is_pending());
    let canceled = sender.write(payload.clone(), WriteOptions::default());
    assert_eq!(sender.retained().writes, 2);
    drop(canceled);
    cluster.until(|_| sender.retained().writes == 1);
    release.now();
    let receipt = cluster.complete(write).unwrap();
    assert_eq!(receipt.index, LogIndex(2));
    assert_eq!(receipt.result.index, receipt.index);
    assert_eq!(receipt.result.term, receipt.term);
    assert_eq!(
        receipt.result.result.disposition,
        ApplyDisposition::SessionOpened
    );
    assert_eq!(sender.retained().writes, 0);
    let queried = cluster.complete(read).unwrap();
    let proof = queried.proof.unwrap();
    assert!(proof.required_applied_index >= receipt.index);
    assert!(proof.local_applied_index >= receipt.index);
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
    let retry = cluster.0[1]
        .owner
        .sender()
        .write(payload, WriteOptions::default());
    let receipt = cluster.complete(retry).unwrap();
    assert_eq!(
        receipt.result.result.disposition,
        ApplyDisposition::SessionAlreadyOpen
    );
    cluster.converge(1, receipt.index);
    cluster.close();
}

#[test]
fn managed_queries_use_tls_quorum_and_preserve_read_your_writes_after_reopen() {
    let scratch = crate::scratch::ScratchDir::new("managed-query-tls");
    let mut cluster = Cluster::open(scratch.path(), true, None);
    cluster.elect(0);
    let writer = cluster.0[0].owner.sender();
    let mut floor = LogIndex::ZERO;
    let commands = [
        crate::support::open_session(0, 1),
        crate::support::execute(
            0,
            1,
            1,
            Mutation::OpenAccount {
                account_id: super::ACCOUNT,
            },
        ),
        crate::support::execute(
            0,
            1,
            2,
            Mutation::Deposit {
                account_id: super::ACCOUNT,
                amount: crate::support::amount(77),
            },
        ),
    ];
    for command in commands {
        let encoded = LedgerStateMachine::new(config())
            .encode_command(&command)
            .unwrap();
        floor = cluster
            .complete(writer.write(encoded, WriteOptions::default()))
            .unwrap()
            .index;
    }
    cluster.converge(0, floor);
    let reader = cluster.0[0].owner.read_sender().unwrap();
    let mut read = reader.read(
        vec![0],
        ReadConsistency::Linearizable,
        ReadOptions::default().with_min_applied_index(floor),
    );
    // Let the leader submit its barrier, but keep follower owners from turning.
    for _ in 0..16 {
        cluster.0[0].pump();
    }
    assert!(poll(&mut read).is_pending());
    assert_eq!(
        cluster.0[0].owner.metrics().unwrap().pending_read_barriers,
        1
    );
    cluster.until(|c| {
        c.0[0]
            .owner
            .metrics()
            .is_some_and(|m| m.completed_query_reads == 1)
    });
    assert_eq!(cluster.0[0].owner.metrics().unwrap().reserved_reads, 1);
    let queried = cluster.complete(read).unwrap();
    assert_eq!(
        cluster.0[0].owner.metrics().unwrap().completed_query_reads,
        0
    );
    let LedgerQueryResult::Summary(summary) = queried.result else {
        panic!("expected summary");
    };
    assert_eq!(summary.open_accounts, 1);
    assert_eq!(summary.total_balance, 77);
    assert!(queried.proof.unwrap().required_applied_index >= floor);
    cluster.close();

    let mut cluster = Cluster::open(scratch.path(), false, None);
    cluster.elect(1);
    let reader = cluster.0[1].owner.read_sender().unwrap();
    let queried = cluster
        .complete(reader.read(
            vec![0],
            ReadConsistency::Linearizable,
            ReadOptions::default().with_min_applied_index(floor),
        ))
        .unwrap();
    assert_eq!(queried.result, LedgerQueryResult::Summary(summary));
    let proof = queried.proof.unwrap();
    assert!(proof.required_applied_index >= floor);
    assert!(proof.local_applied_index >= floor);
    cluster.close();
}
