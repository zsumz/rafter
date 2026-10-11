//! Completed client results retain capacity until their futures are consumed.

mod support;

use rafter_app::proposal::ClientRequestId;
use rafter_service::{ReadOptions, TransportDriverOptions, TransportRaftDriver, WriteOptions};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Barrier},
};
use support::transport::{
    cluster_with_options, elect, settle, Driver, Nameable, QueueTransport, Validator, GROUP,
};
use support::{
    block_on, numbered_group, poll_once, ManagedDriverError, NodeId, ReadConsistency, ReadError,
    UnknownOutcomeReason, WriteError, WriteFate,
};

fn bounded(nodes: &[u64], retained: usize) -> BTreeMap<NodeId, (Driver, QueueTransport)> {
    let cluster = cluster_with_options(
        nodes,
        &BTreeMap::from([(
            1,
            TransportDriverOptions::new().with_max_retained_waiters(retained),
        )]),
    );
    elect(&cluster, NodeId(1));
    cluster
}

fn refuse_write(driver: &Driver) {
    let error = driver
        .begin_write(
            ("refused".to_owned(), "value".to_owned()),
            WriteOptions::default(),
        )
        .err()
        .unwrap();
    assert!(matches!(&error, WriteError::Backpressure), "{error:?}");
}

fn refuse_read(driver: &Driver) {
    let error = driver
        .begin_read("refused".to_owned(), ReadOptions::default())
        .err()
        .unwrap();
    assert!(matches!(&error, ReadError::Backpressure), "{error:?}");
}

#[test]
fn default_retention_limit_bounds_completed_unpolled_writes() {
    let nodes = cluster_with_options(
        &[1],
        &BTreeMap::from([(
            1,
            TransportDriverOptions::default().with_max_pending_waiters(1),
        )]),
    );
    elect(&nodes, NodeId(1));
    let driver = &nodes[&NodeId(1)].0;
    let mut held = Vec::new();
    for _ in 0..2048 {
        held.push(
            driver
                .begin_write(
                    ("key".to_owned(), "value".to_owned()),
                    WriteOptions::default(),
                )
                .unwrap(),
        );
        assert!(
            driver.pending_writes().is_empty(),
            "single voter resolves in place"
        );
    }
    let before = driver
        .with_group(|group| group.metrics().commit_index)
        .unwrap();
    let refused = driver
        .begin_write(
            ("not-appended".to_owned(), "value".to_owned()),
            WriteOptions::default(),
        )
        .err();
    assert!(
        matches!(refused, Some(WriteError::Backpressure)),
        "resolved results must still consume retention capacity; got {refused:?}"
    );
    assert_eq!(
        driver
            .with_group(|group| group.metrics().commit_index)
            .unwrap(),
        before
    );
    drop(held);
    assert!(driver
        .begin_write(
            ("after-drop".to_owned(), "value".to_owned()),
            WriteOptions::default()
        )
        .is_ok());
}

#[test]
fn completed_reads_and_writes_share_capacity_and_polling_releases_it() {
    let nodes = bounded(&[1], 2);
    let driver = &nodes[&NodeId(1)].0;
    let (write_id, mut write) = driver
        .begin_write(
            ("key".to_owned(), "value".to_owned()),
            WriteOptions::default(),
        )
        .unwrap();
    let (read_id, mut read) = driver
        .begin_read("key".to_owned(), ReadOptions::default())
        .unwrap();
    settle(&nodes);
    assert!(driver.pending_reads().is_empty());
    assert!(driver.pending_writes().is_empty());
    let before = driver
        .with_group(|g| (g.metrics().commit_index, g.metrics().reserved_reads))
        .unwrap();
    refuse_write(driver);
    refuse_read(driver);
    assert_eq!(
        block_on(
            driver
                .handle()
                .write(("handle".to_owned(), "value".to_owned()))
        )
        .unwrap_err()
        .fate(),
        WriteFate::NotAppended,
    );
    assert!(matches!(
        block_on(
            driver
                .handle()
                .read("key".to_owned(), ReadConsistency::Linearizable)
        ),
        Err(ReadError::Backpressure),
    ));
    assert_eq!(
        driver
            .with_group(|g| (g.metrics().commit_index, g.metrics().reserved_reads))
            .unwrap(),
        before
    );
    // No waiter is registered for local reads, so they still work when full.
    assert_eq!(
        block_on(
            driver
                .handle()
                .read("key".to_owned(), ReadConsistency::Local)
        )
        .unwrap()
        .result,
        Some("value".to_owned())
    );
    assert!(poll_once(&mut write).unwrap().is_ok());
    let (next_write_id, next_write) = driver
        .begin_write(
            ("key".to_owned(), "next".to_owned()),
            WriteOptions::default(),
        )
        .unwrap();
    assert_eq!(next_write_id.0, write_id.0 + 1, "refusal consumed no ID");
    refuse_read(driver);
    assert_eq!(
        poll_once(&mut read).unwrap().unwrap().result,
        Some("value".to_owned())
    );
    let (next_read_id, next_read) = driver
        .begin_read("key".to_owned(), ReadOptions::default())
        .unwrap();
    assert_eq!(
        next_read_id.0,
        read_id.0 + 1,
        "refusal reserved no barrier ID"
    );
    settle(&nodes);
    refuse_write(driver);
    // Keeping the already-polled future objects did not retain table capacity.
    drop((write, read, next_write, next_read));
}

#[test]
fn abandoning_preserves_unknown_identity_and_holds_credit_until_consumed_or_dropped() {
    let nodes = bounded(&[1, 2], 2);
    let driver = &nodes[&NodeId(1)].0;
    let request = ClientRequestId {
        client_id: 42,
        sequence: 7,
    };
    let (write_id, mut write) = driver
        .begin_write(
            ("key".to_owned(), "value".to_owned()),
            WriteOptions::default().with_client_request_id(request),
        )
        .unwrap();
    let (read_id, read) = driver
        .begin_read("key".to_owned(), ReadOptions::default())
        .unwrap();
    assert_eq!(driver.pending_writes().len(), 1);
    assert_eq!(driver.pending_reads().len(), 1);
    refuse_write(driver);
    refuse_read(driver);
    assert!(driver.abandon_write(write_id));
    assert!(driver.abandon_read(read_id));
    assert!(driver.pending_writes().is_empty());
    assert!(driver.pending_reads().is_empty());
    refuse_write(driver);
    refuse_read(driver);
    let error = poll_once(&mut write).unwrap().unwrap_err();
    assert!(matches!(error, WriteError::UnknownOutcome {
        local_proposal_id, client_request_id: Some(id),
        reason: UnknownOutcomeReason::DriveBoundReached,
    } if local_proposal_id == write_id && id == request));
    let (_, replacement) = driver
        .begin_write(
            ("replacement".to_owned(), "value".to_owned()),
            WriteOptions::default(),
        )
        .unwrap();
    refuse_read(driver);
    drop(read);
    let (_, replacement_read) = driver
        .begin_read("key".to_owned(), ReadOptions::default())
        .unwrap();
    drop((replacement, replacement_read));
    assert_eq!(
        driver.with_group(|g| g.metrics().reserved_reads).unwrap(),
        0
    );
}

#[test]
fn dropping_a_resolved_future_under_the_driver_lock_reclaims_before_admission() {
    let nodes = bounded(&[1], 1);
    let driver = &nodes[&NodeId(1)].0;
    let (_, write) = driver
        .begin_write(
            ("key".to_owned(), "value".to_owned()),
            WriteOptions::default(),
        )
        .unwrap();
    refuse_write(driver);
    // Reentrant drop uses deferred reclamation. The next admission drains it.
    driver.with_group(|_| drop(write)).unwrap();
    let (_, read) = driver
        .begin_read("key".to_owned(), ReadOptions::default())
        .unwrap();
    settle(&nodes);
    refuse_read(driver);
    driver.with_group(|_| drop(read)).unwrap();
    assert!(driver
        .begin_write(
            ("after".to_owned(), "value".to_owned()),
            WriteOptions::default()
        )
        .is_ok());
}

#[test]
fn simultaneous_resolving_writes_cannot_overbook_retained_slots() {
    let nodes = bounded(&[1], 8);
    let driver = &nodes[&NodeId(1)].0;
    let start = Arc::new(Barrier::new(16));
    let before = driver.with_group(|g| g.metrics().commit_index).unwrap();
    let threads: Vec<_> = (0..16)
        .map(|n| {
            let driver = driver.clone();
            let start = Arc::clone(&start);
            std::thread::spawn(move || {
                start.wait();
                driver.begin_write((n.to_string(), "value".to_owned()), WriteOptions::default())
            })
        })
        .collect();
    let mut held = Vec::new();
    let mut refused = 0;
    for thread in threads {
        match thread.join().unwrap() {
            Ok(result) => held.push(result),
            Err(error) => {
                assert!(matches!(error, WriteError::Backpressure));
                assert_eq!(error.fate(), WriteFate::NotAppended);
                refused += 1;
            }
        }
    }
    assert_eq!(held.len(), 8);
    assert_eq!(refused, 8);
    assert_eq!(
        driver.with_group(|g| g.metrics().commit_index).unwrap().0,
        before.0 + 8
    );
    drop(held);
}

#[test]
fn a_zero_retention_budget_is_refused_at_construction() {
    let transport = QueueTransport::default();
    let validator = Validator {
        transport: transport.clone(),
        authorized: BTreeSet::new(),
        nameable: Nameable::all(),
    };
    let result = TransportRaftDriver::new(
        numbered_group(GROUP, 1, &[], 3),
        Vec::new(),
        transport,
        validator,
        TransportDriverOptions::new().with_max_retained_waiters(0),
    );
    assert!(matches!(
        result,
        Err(ManagedDriverError::InvalidOptions {
            field: "max_retained_waiters",
            ..
        })
    ));
}
