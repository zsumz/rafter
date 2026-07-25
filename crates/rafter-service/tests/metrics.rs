#![allow(clippy::wildcard_imports)]

mod support;

use support::*;

#[test]
fn in_memory_driver_writes_reads_and_publishes_metrics_with_real_groups() {
    let driver = elected_driver();
    let handle = driver.handle();

    let write = block_on(handle.write(("alpha".to_owned(), "one".to_owned())))
        .expect("write commits and applies");
    assert_eq!(write.result, None);

    let read = block_on(handle.read("alpha".to_owned(), ReadConsistency::Linearizable))
        .expect("linearizable read succeeds");
    assert_eq!(read.result, Some("one".to_owned()));
    assert!(read.proof.is_some());

    let metrics = handle.metrics().expect("metrics").current();
    assert_eq!(metrics.role, Role::Leader);
    assert_eq!(metrics.applied_index, write.index);
}

#[test]
fn in_memory_driver_rejects_wrong_group_shutdown() {
    let driver = NumberedDriver::new_elected(NodeId(1), vec![numbered_group(7, 1, &[], 3)])
        .expect("numbered primary elects");
    let wrong_handle: RaftHandle<
        u64,
        (String, String),
        String,
        Option<String>,
        Option<String>,
        NumberedDriver,
    > = RaftHandle::new(8, driver.clone());

    assert!(matches!(
        wrong_handle.metrics(),
        Err(MetricsError::WrongGroup)
    ));
    let error =
        block_on(wrong_handle.shutdown()).expect_err("a shutdown for another group is refused");

    // Previously reported as a transport failure, which was both the wrong
    // category and, on the write path, the wrong fate.
    assert!(matches!(error, ShutdownError::WrongGroup), "got {error:?}");
    assert_eq!(
        driver
            .handle()
            .metrics()
            .expect("correct metrics")
            .current()
            .group_id,
        7
    );
    block_on(driver.handle().shutdown()).expect("correct group shutdown still succeeds");
}

#[test]
fn tick_primary_publishes_metrics_before_drain_error() {
    let driver = KvDriver::new(NodeId(1), vec![group(1, &[2], 1)])
        .expect("driver with missing remote peer builds");
    let handle = driver.handle();
    assert_eq!(
        handle.metrics().expect("metrics").current().role,
        Role::Follower
    );

    let error = driver.tick_primary().expect_err("missing peer fails drain");

    assert!(
        matches!(
            error,
            ManagedDriverError::MissingNode { node_id: NodeId(2) }
        ),
        "got {error:?}"
    );
    assert_eq!(
        handle.metrics().expect("metrics").current().role,
        Role::PreCandidate,
        "tick mutation is published before returning the drain error"
    );
}
