//! File-backed maintenance through the public handle's exclusive owner.
use super::{link, Managed};
use crate::snapshot_source::FileSource;
use rafter::{
    ApplicationSnapshotKind, ApplicationSnapshotMetadata, ApplicationSnapshotVersion, LogIndex,
    RaftSnapshotMetadata, SnapshotGroupId,
};
use rafter_runtime::{
    service::{ServiceError, ServiceState},
    snapshot_io::SnapshotCancellation,
    LogRetirementWorker, LogRetirementWorkerOptions,
};
use std::time::{Duration, Instant};

impl Managed {
    pub(super) fn compact_all(&mut self, floor: LogIndex) {
        // Three replicas can each retire the 128-operation load plus setup
        // before the drop thread returns any credits.
        let mut retirement =
            LogRetirementWorker::start(LogRetirementWorkerOptions::new(512, 256 * 1024)).unwrap();
        for (driver, _) in &mut self.nodes {
            let metrics = driver.handle().metrics().unwrap();
            let deadline = Instant::now() + Duration::from_secs(5);
            let retired = loop {
                let candidate = driver.compact_snapshot_with(|application, node| {
                    let at = application.ledger.store().applied_index();
                    let metadata = RaftSnapshotMetadata::new(
                        SnapshotGroupId::new("ledger-owner-tls")?,
                        node.id(),
                        at,
                        node.term_at_index(at).unwrap(),
                        node.current_term(),
                        ApplicationSnapshotMetadata::new(
                            ApplicationSnapshotKind::new("ledger")?,
                            ApplicationSnapshotVersion::new(1)?,
                        ),
                    )?;
                    let source = FileSource::build(|file| {
                        let (_, snapshot) = application.ledger.write_snapshot_to(
                            metadata,
                            file,
                            SnapshotCancellation::new(),
                        )?;
                        Ok(snapshot)
                    })?;
                    Ok((source.snapshot().clone(), source))
                });
                match candidate {
                    Ok(retired) => break retired,
                    Err(
                        ServiceError::NotQuiescent | ServiceError::NotReady(ServiceState::Busy),
                    ) => {
                        driver.poll_next().unwrap();
                        assert!(Instant::now() < deadline, "managed compaction stalled");
                        std::thread::yield_now();
                    }
                    Err(error) => panic!("unexpected maintenance error: {error}"),
                }
            };
            assert!(!retired.is_empty());
            retirement.try_submit(retired).unwrap();
            assert!(driver.ready_node().unwrap().snapshot_index() >= floor);
            assert!(metrics.current().snapshot_index >= floor);
            assert_eq!(metrics.current().group_id, link::GROUP);
            assert_eq!(driver.durable_through(), Some(floor));
        }
        retirement.shutdown().unwrap();
    }
}
