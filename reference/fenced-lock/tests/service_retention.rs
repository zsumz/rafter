//! Exact-package consumer of the managed driver's completed-result budget.

#[allow(dead_code)]
#[path = "support/cluster.rs"]
mod cluster;
#[allow(dead_code)]
mod support;
#[allow(dead_code)]
#[path = "support/transport.rs"]
mod transport;

use rafter::{NodeConfig, NodeId};
use rafter_app::group::RaftGroup;
use rafter_reference_fenced_lock::{LockQuery, LockStateMachine};
use rafter_runtime::DurableRaftNode;
use rafter_service::{
    ReadError, ReadOptions, TransportDriverOptions, WriteError, WriteFate, WriteOptions,
};
use rafter_storage::InMemoryRaftHardStateStore;
use std::{
    future::Future,
    pin::Pin,
    task::{Context, Poll, Waker},
};

fn ready<T>(future: &mut Pin<Box<dyn Future<Output = T> + Send>>) -> T {
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("single voter should have resolved the operation"),
    }
}

#[test]
fn uncollected_lock_results_share_a_bounded_budget_with_read_results() {
    let node = NodeId(1);
    let network = transport::DeterministicNetwork::new();
    let directory = transport::PeerDirectory::new(&[node]);
    let runtime = DurableRaftNode::new(
        NodeConfig::new(node, vec![], 1).unwrap(),
        InMemoryRaftHardStateStore::new(),
    )
    .unwrap();
    let driver = cluster::NodeDriver::new(
        RaftGroup::new(
            cluster::GROUP_ID,
            node,
            runtime,
            LockStateMachine::new(support::config(2, 2)),
        ),
        vec![],
        network.endpoint(node, directory.clone()),
        directory,
        TransportDriverOptions::new()
            .with_max_pending_waiters(1)
            .with_max_retained_waiters(2),
    )
    .unwrap();
    driver.tick().unwrap();
    let (write_id, mut write) = driver
        .begin_write(support::open_session(0, 1), WriteOptions::default())
        .unwrap();
    let query = LockQuery::GetLock {
        resource: support::resource("lock"),
    };
    let (read_id, mut read) = driver.begin_read(query, ReadOptions::default()).unwrap();
    driver.drive_pending_reads().unwrap();
    assert!(driver.pending_writes().is_empty());
    assert!(driver.pending_reads().is_empty());
    let before = driver.committed_application_index().unwrap();
    let refusal = driver
        .begin_write(support::open_session(1, 1), WriteOptions::default())
        .err()
        .unwrap();
    assert!(matches!(refusal, WriteError::Backpressure));
    assert_eq!(refusal.fate(), WriteFate::NotAppended);
    assert!(matches!(
        driver.begin_read(query, ReadOptions::default()),
        Err(ReadError::Backpressure)
    ));
    assert_eq!(driver.committed_application_index().unwrap(), before);
    assert!(ready(&mut write).is_ok());
    let (next_write, held) = driver
        .begin_write(support::open_session(1, 1), WriteOptions::default())
        .unwrap();
    assert_eq!(next_write.0, write_id.0 + 1);
    assert!(ready(&mut read).unwrap().result.status().holder.is_none());
    let (next_read, mut next) = driver.begin_read(query, ReadOptions::default()).unwrap();
    assert_eq!(next_read.0, read_id.0 + 1);
    driver.drive_pending_reads().unwrap();
    assert!(ready(&mut next).is_ok());
    drop(held);
    // Client consumption released the driver entries; their future objects
    // stayed alive through the second admission and retained no table credit.
    drop((write, read));
}
