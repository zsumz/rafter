//! Worker diagnostics cross the service boundary without driving its workers.
use super::{
    super::*,
    persistence_test::{Cluster, Owner},
};
use crate::pipelined::PersistenceWorkerOptions;
use rafter::{LogIndex, NodeConfig, NodeId};
use std::time::{Duration, Instant};

fn options(enabled: bool) -> ServiceOptions {
    let mut options = ServiceOptions::new();
    options.persistence = PersistenceWorkerOptions::new().with_storage_telemetry(enabled);
    options
}

#[test]
fn diagnostics_wait_for_consumption_and_survive_shutdown_until_fresh_recovery() {
    let mut cluster = Cluster::with_options(options(true));
    let (started, mut release) = cluster.delay(false);
    assert!(cluster.owner.persistence_telemetry().is_empty());
    cluster.propose();
    started.recv_timeout(Duration::from_secs(5)).unwrap();
    for _ in 0..16 {
        assert!(cluster.owner.persistence_telemetry().is_empty());
        assert!(cluster.owner.ready_node().is_none());
        assert_eq!(cluster.owner.durable_through(), Some(LogIndex::ZERO));
    }
    release.now();
    assert_eq!(cluster.drive(), vec![LogIndex(2)]);
    assert!(cluster
        .owner
        .persistence_telemetry()
        .iter()
        .any(|(name, _)| *name == "log_write"));
    let sample = format!("{:?}", cluster.owner.persistence_telemetry());
    for _ in 0..16 {
        assert_eq!(
            format!("{:?}", cluster.owner.persistence_telemetry()),
            sample
        );
    }
    cluster.owner.begin_shutdown();
    let store = cluster.owner.finish_shutdown().unwrap();
    assert_eq!(
        format!("{:?}", cluster.owner.persistence_telemetry()),
        sample
    );
    let storage = cluster.owner.ready_node().unwrap().clone().into_storage();
    let config = NodeConfig::new(NodeId(1), vec![NodeId(2), NodeId(3)], 1).unwrap();
    let mut recovered =
        Owner::recover(config, storage, store, options(true), super::support::adapt).unwrap();
    assert!(recovered.persistence_telemetry().is_empty());
    let deadline = Instant::now() + Duration::from_secs(5);
    while recovered.state() != ServiceState::Ready {
        recovered.poll().unwrap();
        assert!(Instant::now() < deadline);
    }
    assert!(recovered.persistence_telemetry().is_empty());
}

#[test]
fn disabled_collection_stays_empty_after_real_worker_completion() {
    let mut cluster = Cluster::with_options(options(false));
    cluster.propose();
    assert!(cluster.owner.ready_node().is_none());
    assert_eq!(cluster.drive(), vec![LogIndex(2)]);
    assert_eq!(cluster.owner.durable_through(), Some(LogIndex(2)));
    assert!(cluster.owner.persistence_telemetry().is_empty());
}
